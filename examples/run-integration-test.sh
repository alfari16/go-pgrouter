#!/bin/bash

# Integration Test Runner for LSN-based Orders API
# This script sets up the test environment and runs comprehensive LSN consistency tests

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if docker and docker-compose are installed
check_dependencies() {
    print_status "Checking dependencies..."

    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi

    if ! command -v go &> /dev/null; then
        print_error "Go is not installed. Please install Go first."
        exit 1
    fi

    print_success "All dependencies found"
}

# Clean up any existing containers
cleanup_containers() {
    print_status "Cleaning up existing containers..."

    # Stop and remove any running containers
    if docker-compose ps | grep -q "Up"; then
        print_warning "Stopping running containers..."
        docker-compose down -v || true
    fi

    # Remove any orphaned containers
    docker container prune -f || true

    print_success "Containers cleaned up"
}

# Start the test environment
start_environment() {
    print_status "Starting test environment..."

    # Start database containers
    docker-compose up -d

    print_status "Waiting for databases to be ready..."

    # Wait for primary database
    local max_attempts=30
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if docker exec pg_primary pg_isready -U user -d mydb &>/dev/null; then
            print_success "Primary database is ready"
            break
        fi

        attempt=$((attempt + 1))
        echo -n "."
        sleep 1
    done

    if [ $attempt -eq $max_attempts ]; then
        print_error "Primary database failed to start within $max_attempts seconds"
        cleanup_containers
        exit 1
    fi

    # Wait for replica database
    attempt=0
    while [ $attempt -lt $max_attempts ]; do
        if docker exec pg_replica pg_isready -U user -d mydb &>/dev/null; then
            print_success "Replica database is ready"
            break
        fi

        attempt=$((attempt + 1))
        echo -n "."
        sleep 1
    done

    if [ $attempt -eq $max_attempts ]; then
        print_error "Replica database failed to start within $max_attempts seconds"
        cleanup_containers
        exit 1
    fi

    echo

    # Show container status
    print_status "Container status:"
    docker-compose ps
}

# Run the application
start_application() {
    print_status "Starting the application..."

    # Start the application in background
    go run -tags=example main.go &
    APP_PID=$!

    # Give the application time to start
    print_status "Waiting for application to be ready..."
    sleep 5

    # Check if application is responding
    local max_attempts=15
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        if curl -s http://localhost:8080/health &>/dev/null; then
            print_success "Application is ready (PID: $APP_PID)"
            break
        fi

        attempt=$((attempt + 1))
        echo -n "."
        sleep 1
    done

    if [ $attempt -eq $max_attempts ]; then
        print_error "Application failed to start within $max_attempts seconds"
        kill $APP_PID 2>/dev/null || true
        cleanup_containers
        exit 1
    fi

    echo
}

# Run the integration tests
run_tests() {
    print_status "Running integration tests..."

    # Check if testify is installed
    if ! go list github.com/stretchr/testify &>/dev/null; then
        print_status "Installing testify for testing..."
        go mod tidy
        if ! grep -q "github.com/stretchr/testify" go.mod; then
            go get github.com/stretchr/testify/assert
            go get github.com/stretchr/testify/require
        fi
    fi

    # Run the tests
    echo "=========================================="
    echo "         Running LSN Consistency Tests    "
    echo "=========================================="
    echo

    # Set environment variable for tests
    export INTEGRATION_TEST=true

    # Run tests with verbose output
    if go test -v -tags=example -timeout=120s ./integration_test.go; then
        print_success "All integration tests passed!"
        return 0
    else
        print_error "Some integration tests failed!"
        return 1
    fi
}

# Manual API testing for verification
manual_test_verification() {
    print_status "Running manual API verification..."

    echo
    print_status "1. Creating an order..."

    # Create an order
    CREATE_RESPONSE=$(curl -s -X POST \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "customer_name=Manual Test Customer&amount=199.99&status=pending" \
        http://localhost:8080/orders)

    echo "Create Response: $CREATE_RESPONSE"

    # Extract order ID from response
    ORDER_ID=$(echo $CREATE_RESPONSE | grep -o '"id":[0-9]*' | cut -d':' -f2)

    if [ -n "$ORDER_ID" ]; then
        print_success "Order created with ID: $ORDER_ID"

        echo
        print_status "2. Reading the order..."

        # Read the order
        READ_RESPONSE=$(curl -s "http://localhost:8080/orders/get?id=$ORDER_ID")
        echo "Read Response: $READ_RESPONSE"

        # Check if response contains our order
        if echo "$READ_RESPONSE" | grep -q "Manual Test Customer"; then
            print_success "Order retrieved successfully!"
        else
            print_warning "Order retrieval might have issues"
        fi

        echo
        print_status "3. Listing all orders..."

        # List orders
        LIST_RESPONSE=$(curl -s "http://localhost:8080/orders/list?limit=5")
        echo "Orders List: $LIST_RESPONSE"

        if echo "$LIST_RESPONSE" | grep -q "Manual Test Customer"; then
            print_success "Order appears in the list!"
        else
            print_warning "Order not found in list (might be due to filtering or pagination)"
        fi

        echo
        print_status "4. Checking health status..."

        # Health check
        HEALTH_RESPONSE=$(curl -s "http://localhost:8080/health")
        echo "Health Status: $HEALTH_RESPONSE"

        if echo "$HEALTH_RESPONSE" | grep -q '"lsn_enabled":true'; then
            print_success "LSN is enabled!"
        else
            print_warning "LSN might not be enabled"
        fi

    else
        print_error "Failed to create order or extract order ID"
    fi

    echo
}

# Cleanup function
cleanup() {
    print_status "Cleaning up..."

    # Kill the application if it's running
    if [ ! -z "$APP_PID" ] && kill -0 $APP_PID 2>/dev/null; then
        print_status "Stopping application (PID: $APP_PID)..."
        kill $APP_PID 2>/dev/null || true
        sleep 2
        kill -9 $APP_PID 2>/dev/null || true
    fi

    # Stop containers
    cleanup_containers

    print_success "Cleanup completed"
}

# Set up signal handlers
trap cleanup EXIT INT TERM

# Main execution
main() {
    echo "=========================================="
    echo "   LSN Orders API Integration Test Suite   "
    echo "=========================================="
    echo

    print_status "Starting integration test setup..."
    echo

    # Check dependencies
    check_dependencies

    # Clean up any existing containers
    cleanup_containers

    # Start the environment
    start_environment

    # Start the application
    start_application

    # Run the tests
    echo
    if run_tests; then
        TESTS_PASSED=true
    else
        TESTS_PASSED=false
    fi

    # Run manual verification
    echo
    manual_test_verification

    # Summary
    echo
    echo "=========================================="
    echo "              Test Summary                "
    echo "=========================================="

    if [ "$TESTS_PASSED" = true ]; then
        print_success "Automated tests: PASSED"
    else
        print_error "Automated tests: FAILED"
    fi

    echo
    print_status "Container logs for debugging:"
    echo "--- Primary DB Logs ---"
    docker logs pg_primary 2>&1 | tail -10
    echo
    echo "--- Replica DB Logs ---"
    docker logs pg_replica 2>&1 | tail -10
    echo

    if [ "$TESTS_PASSED" = true ]; then
        print_success "Integration test suite completed successfully!"
        exit 0
    else
        print_error "Integration test suite completed with failures!"
        exit 1
    fi
}

# Run main function
main "$@"