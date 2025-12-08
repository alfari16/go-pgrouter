# LSN-based Orders API Example

This example demonstrates a complete web application with LSN (Log Sequence Number) based causal consistency using PostgreSQL primary-replica replication.

## Overview

The example shows how to build a simple orders management API that automatically handles read/write consistency between primary and replica databases. The system ensures that after a write operation, subsequent reads will return consistent data by intelligently routing queries to either the replica (when caught up) or the primary (when lagging).

## Key Features Demonstrated

- **LSN-based Causal Consistency**: Automatic routing based on replication lag
- **Write Operations**: Always go to primary database and update LSN tracking
- **Read Operations**: Route to replica when safe, fallback to primary when needed
- **Cookie-based LSN Tracking**: Maintains consistency across HTTP requests
- **Replication Lag Handling**: Gracefully handles 5-second artificial replication delay
- **Health Monitoring**: Real-time status of primary/replica health and LSN state
- **Connection Pooling**: Optimized database connection management
- **Comprehensive Testing**: Automated integration tests with real replication validation

## Architecture

```
┌─────────────┐    HTTP Request    ┌─────────────┐    Write Query    ┌─────────────┐
│   Client    │ ────────────────► │  Web Server │ ─────────────────► │ Primary DB  │
│ (Browser)   │                   │             │                   │  (Master)   │
│             │ ◄─────────────── │             │ ◄──────────────── │   (Port     │
│             │    HTTP Response  │             │    Response       │   5432)     │
└─────────────┘                   └─────────────┘                   └─────────────┘
                                                │
                                                │ Read Query
                                                │ (with LSN check)
                                                ▼
                                       ┌─────────────────────┐
                                       │ Replica DB (Slave)  │
                                       │    (5-second lag)   │
                                       │      (Port 5433)    │
                                       └─────────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- Go 1.25.5 or later
- PostgreSQL client tools (optional, for manual verification)

## Quick Start

### 1. Run the Integration Tests (Recommended)

The easiest way to see everything working is to run the comprehensive integration test suite:

```bash
cd examples
./run-integration-test.sh
```

This script will:
- Start PostgreSQL primary and replica containers with 5-second artificial lag
- Start the web application
- Run automated tests to validate LSN consistency
- Perform manual API verification
- Show logs and cleanup everything

### 2. Manual Setup

If you prefer to run everything manually:

```bash
# 1. Start the databases
docker-compose up -d

# 2. Wait for databases to be ready
./run-integration-test.sh --setup-only

# 3. Start the application
go run -tags=example main.go
```

## API Endpoints

### Create Order (Write)
```bash
curl -X POST \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "customer_name=John Doe&amount=99.99&status=pending" \
  http://localhost:8080/orders
```

Response:
```json
{
  "id": 1,
  "customer_name": "John Doe",
  "amount": 99.99,
  "status": "pending",
  "created_at": "2024-01-15T10:30:00Z"
}
```

### Get Order (Read with LSN Consistency)
```bash
curl http://localhost:8080/orders/get?id=1
```

This read will:
- Use the replica if it has caught up to the required LSN
- Fall back to primary if replica is lagging (within the 5-second window)
- Automatically handle LSN cookies for consistency

### List Orders (Read with LSN Consistency)
```bash
curl "http://localhost:8080/orders/list?limit=10&status=pending"
```

### Health Status
```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "healthy": true,
  "lsn_enabled": true,
  "last_known_master_lsn": "0/16B5D58",
  "replica_count": 1,
  "healthy_replicas": 1,
  "check_time": "2024-01-15T10:30:00Z"
}
```

## LSN Consistency Demonstration

### What Happens When You Create an Order:

1. **Write Operation**: Order is inserted into primary database (port 5432)
2. **LSN Tracking**: The system captures the new Log Sequence Number (LSN)
3. **Cookie Update**: LSN value is stored in `pg_min_lsn` cookie
4. **Replication**: Changes are asynchronously replicated to replica (port 5433)

### What Happens When You Read an Order:

1. **Cookie Check**: System reads `pg_min_lsn` cookie to find required LSN
2. **Replica Status**: Checks if replica has caught up to required LSN
3. **Routing Decision**:
   - **Replica Safe**: Replica has caught up → Use replica for read
   - **Replica Lagging**: Replica behind by >5 seconds → Use primary
4. **Response**: Returns consistent data regardless of database choice

### Testing Replication Lag:

The system is configured with a 5-second artificial replication delay to demonstrate:

- **Immediate Reads** (0-5 seconds after write): Go to primary (due to lag)
- **Delayed Reads** (5+ seconds after write): Go to replica (caught up)

## Integration Test Coverage

The test suite (`integration_test.go`) validates:

1. **Health Check**: Verifies LSN is enabled and databases are healthy
2. **Create/Read Consistency**: Ensures created orders can be immediately read
3. **Cookie Management**: Validates LSN cookies are set and respected
4. **Order Listing**: Confirms orders appear in list results
5. **Concurrent Operations**: Tests multiple simultaneous writes/reads
6. **Replication Lag Handling**: Verifies system handles 5-second lag gracefully

## Database Schema

The `orders` table structure:

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_orders_customer_name ON orders(customer_name);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
```

## Configuration Options

### LSN Settings
- **LSN Query Timeout**: 3 seconds
- **LSN Throttle Time**: 100ms
- **Cookie Max Age**: 5 minutes
- **Consistency Level**: ReadYourWrites

### Database Connection Pooling
- **Max Open Connections**: 20
- **Max Idle Connections**: 5
- **Connection Lifetime**: 1 hour

### Replication Configuration
- **Primary**: Port 5432 (write operations)
- **Replica**: Port 5433 (read operations)
- **Artificial Lag**: 5 seconds (for demonstration)
- **Replication User**: `repuser` with password `reppass`

## LSN Flow

1. **Write Request:**
   - Client sends POST request without LSN cookie
   - Application writes to primary master database
   - Master generates new LSN (e.g., `0/3000060`)
   - Response sets LSN cookie: `pg_min_lsn=0/3000060`

2. **Read Request:**
   - Client sends GET request with LSN cookie
   - Middleware extracts required LSN from cookie
   - Query routing checks if replicas have caught up:
     - If replica LSN >= required LSN → Use replica
     - If replica LSN < required LSN → Use primary
   - Application reads data with causal consistency guaranteed

## Monitoring and Debugging

### Application Logs
The application logs show:
- LSN cookie creation and updates
- Database routing decisions
- Query consistency levels
- Connection status

### Database Logs
View primary/replica logs:

```bash
# Primary database logs
docker logs pg_primary

# Replica database logs
docker logs pg_replica
```

### Manual Database Verification
Connect to databases directly:

```bash
# Primary
docker exec -it pg_primary psql -U user -d mydb

# Replica
docker exec -it pg_replica psql -U user -d mydb

# Check replication lag
SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn();
```

## Performance Considerations

- **LSN Query Throttling**: LSN queries to master are throttled (default: 100ms)
- **Background Monitoring**: Optional background replica health monitoring
- **Connection Pooling**: Optimized database connection management
- **Caching**: Master LSN results are cached between throttled queries
- **Graceful Degradation**: Continues working even if LSN tracking fails

## Error Handling

The middleware gracefully handles errors:

- **LSN Parse Errors**: Ignores invalid cookies and continues without LSN constraints
- **Database Connection Errors**: Falls back to master for read operations
- **LSN Query Failures**: Continues operation without LSN tracking
- **Replica Lag Exceeded**: Uses primary instead of lagging replicas

## Troubleshooting

### Common Issues

1. **Port Conflicts**: Ensure ports 5432, 5433, and 8080 are available
2. **Docker Issues**: Restart Docker daemon if containers fail to start
3. **Permission Issues**: Ensure Docker has proper permissions
4. **Build Issues**: Run `go mod tidy` to ensure dependencies are current

### Debug Mode

Enable verbose logging:

```bash
export LOG_LEVEL=debug
go run -tags=example main.go
```

### Reset Environment

Completely reset the environment:

```bash
docker-compose down -v
docker system prune -f
./run-integration-test.sh
```

## Production Considerations

This example demonstrates LSN concepts, but for production use:

1. **Remove Artificial Lag**: The 5-second delay is for demonstration only
2. **Enhanced Monitoring**: Add comprehensive metrics and alerting
3. **Connection Security**: Use SSL/TLS for all database connections
4. **Load Balancing**: Consider multiple replicas for high availability
5. **Backup Strategy**: Implement regular database backups
6. **Graceful Degradation**: Handle database failures more robustly

## Further Reading

- PostgreSQL Replication Documentation
- Log Sequence Numbers (LSN) in PostgreSQL
- Causal Consistency Patterns
- Database Connection Pooling Best Practices