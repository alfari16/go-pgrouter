package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type OrderResponse struct {
	ID           int     `json:"id"`
	CustomerName string  `json:"customer_name"`
	Amount       float64 `json:"amount"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
}

type OrdersListResponse struct {
	Orders []OrderResponse `json:"orders"`
}

type HealthResponse struct {
	Healthy            bool   `json:"healthy"`
	LSNEnabled         bool   `json:"lsn_enabled"`
	LastKnownMasterLSN string `json:"last_known_master_lsn"`
	ReplicaCount       int    `json:"replica_count"`
	HealthyReplicas    int    `json:"healthy_replicas"`
}

const (
	serverURL = "http://localhost:8080"
	timeout   = 60 * time.Second
)

func setupDockerEnvironment(t *testing.T) func() {
	// Start docker-compose
	t.Log("Starting Docker containers...")

	cmd := exec.Command("docker-compose", "up", "-d")
	cmd.Dir = "."

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to start docker-compose: %v\nOutput: %s", err, string(output))
	}
	t.Logf("Docker containers started: %s", string(output))

	// Wait for databases to be ready
	t.Log("Waiting for databases to be ready...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for primary database
outerPrimaryBreak:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for primary database")
		default:
			cmd := exec.Command("docker", "exec", "pg_primary", "pg_isready", "-U", "user", "-d", "mydb")
			if err := cmd.Run(); err == nil {
				t.Log("Primary database is ready")
				break outerPrimaryBreak
			}
			time.Sleep(1 * time.Second)
		}
	}

	// Wait for replica database
outerReplicaBreak:
	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for replica database")
		default:
			cmd := exec.Command("docker", "exec", "pg_replica", "pg_isready", "-U", "user", "-d", "mydb")
			if err := cmd.Run(); err == nil {
				t.Log("Replica database is ready")
				break outerReplicaBreak
			}
			time.Sleep(1 * time.Second)
		}
	}

	return func() {
		// Cleanup function
		t.Log("Stopping Docker containers...")
		cmd := exec.Command("docker-compose", "down", "-v")
		cmd.Dir = "."

		if output, err := cmd.CombinedOutput(); err != nil {
			t.Logf("Warning: failed to stop docker-compose: %v\nOutput: %s", err, string(output))
		} else {
			t.Log("Docker containers stopped")
		}
	}
}

func waitForServer(t *testing.T) {
	client := &http.Client{Timeout: 2 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatal("Timeout waiting for server to start")
		default:
			resp, err := client.Get(serverURL + "/health")
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					t.Log("Server is ready")
					return
				}
			}
			time.Sleep(1 * time.Second)
		}
	}
}

func startServer() (*exec.Cmd, error) {
	cmd := exec.Command("go", "run", "-tags=example", "main.go")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "GOTRACEBACK=1")

	fmt.Println("hirrr")

	// Start the server in background
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start server: %w", err)
	}

	fmt.Println("hirrr 2")

	return cmd, nil
}

func TestLSNCausalConsistency(t *testing.T) {
	// Setup Docker environment, assume is running already
	//cleanup := setupDockerEnvironment(t)
	//defer cleanup()

	// Start the server, assume is running already
	//serverCmd, err := startServer()
	//require.NoError(t, err, "Failed to start server")
	//defer func() {
	//	if serverCmd.Process != nil {
	//		serverCmd.Process.Kill()
	//	}
	//}()

	// Wait for server to be ready
	//waitForServer(t)

	// Create HTTP client with cookie jar
	jar, err := cookiejar.New(nil)
	require.NoError(t, err, "Failed to create cookie jar")

	client := &http.Client{
		Jar:     jar,
		Timeout: timeout,
	}

	t.Run("Health Check", func(t *testing.T) {
		resp, err := client.Get(serverURL + "/health")
		require.NoError(t, err, "Health check failed")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var health HealthResponse
		err = json.NewDecoder(resp.Body).Decode(&health)
		require.NoError(t, err)

		assert.True(t, health.Healthy, "Server should be healthy")
		assert.True(t, health.LSNEnabled, "LSN should be enabled")
		assert.GreaterOrEqual(t, health.ReplicaCount, 0, "Replica count should be >= 0")

		t.Logf("Health status: %+v", health)
	})

	t.Run("Create and Read Order with LSN Consistency", func(t *testing.T) {
		// Create a new order
		createData := bytes.NewBufferString("customer_name=Test Customer&amount=99.99&status=pending")
		resp, err := client.Post(serverURL+"/orders", "application/x-www-form-urlencoded", createData)
		require.NoError(t, err, "Failed to create order")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Parse created order response
		var createdOrder OrderResponse
		err = json.NewDecoder(resp.Body).Decode(&createdOrder)
		require.NoError(t, err)

		assert.Equal(t, "Test Customer", createdOrder.CustomerName)
		assert.Equal(t, 99.99, createdOrder.Amount)
		assert.Equal(t, "pending", createdOrder.Status)
		assert.NotZero(t, createdOrder.ID)

		t.Logf("Created order: %+v", createdOrder)

		// Check if LSN cookie was set
		cookies := resp.Cookies()
		var lsnCookie *http.Cookie
		for _, cookie := range cookies {
			if cookie.Name == "pg_min_lsn" {
				lsnCookie = cookie
				break
			}
		}
		assert.NotNil(t, lsnCookie, "LSN cookie should be set after write")
		if lsnCookie != nil {
			t.Logf("LSN cookie set: %s", lsnCookie.Value)
		}

		// Immediately read the same order (should use master due to replication lag)
		resp, err = client.Get(fmt.Sprintf("%s/orders/get?id=%d", serverURL, createdOrder.ID))
		require.NoError(t, err, "Failed to get order")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var retrievedOrder OrderResponse
		err = json.NewDecoder(resp.Body).Decode(&retrievedOrder)
		require.NoError(t, err)

		// Verify order data consistency
		assert.Equal(t, createdOrder.ID, retrievedOrder.ID)
		assert.Equal(t, createdOrder.CustomerName, retrievedOrder.CustomerName)
		assert.Equal(t, createdOrder.Amount, retrievedOrder.Amount)
		assert.Equal(t, createdOrder.Status, retrievedOrder.Status)

		t.Logf("Retrieved order: %+v", retrievedOrder)

		// List all orders and verify our order is in the list
		resp, err = client.Get(serverURL + "/orders/list?limit=100")
		require.NoError(t, err, "Failed to list orders")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var ordersList OrdersListResponse
		err = json.NewDecoder(resp.Body).Decode(&ordersList)
		require.NoError(t, err)

		assert.Greater(t, len(ordersList.Orders), 0, "Orders list should not be empty")

		// Find our order in the list
		found := false
		for _, order := range ordersList.Orders {
			if order.ID == createdOrder.ID {
				found = true
				assert.Equal(t, createdOrder.CustomerName, order.CustomerName)
				assert.Equal(t, createdOrder.Amount, order.Amount)
				assert.Equal(t, createdOrder.Status, order.Status)
				break
			}
		}
		assert.True(t, found, "Created order should be in the orders list")

		t.Logf("Orders list contains %d orders", len(ordersList.Orders))
	})

	t.Run("Concurrent Operations Test", func(t *testing.T) {
		const numOrders = 5
		orders := make([]OrderResponse, numOrders)

		// Create multiple orders concurrently
		for i := 0; i < numOrders; i++ {
			go func(index int) {
				customerName := fmt.Sprintf("Customer %d", index)
				amount := float64(50 + index*10)

				createData := bytes.NewBufferString(fmt.Sprintf(
					"customer_name=%s&amount=%.2f&status=pending",
					customerName, amount,
				))

				resp, err := client.Post(serverURL+"/orders", "application/x-www-form-urlencoded", createData)
				if err != nil {
					t.Logf("Failed to create order %d: %v", index, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					t.Logf("Failed to create order %d, status: %d, body: %s", index, resp.StatusCode, string(body))
					return
				}

				var order OrderResponse
				err = json.NewDecoder(resp.Body).Decode(&order)
				if err != nil {
					t.Logf("Failed to decode order %d: %v", index, err)
					return
				}

				orders[index] = order
				t.Logf("Created order %d: %+v", index, order)
			}(i)
		}

		// Wait for all orders to be created
		time.Sleep(5 * time.Second)

		// Verify all orders were created and can be read
		createdCount := 0
		for i, order := range orders {
			if order.ID != 0 {
				createdCount++

				// Try to read the order
				resp, err := client.Get(fmt.Sprintf("%s/orders/get?id=%d", serverURL, order.ID))
				if err != nil {
					t.Logf("Failed to read order %d: %v", order.ID, err)
					continue
				}
				defer resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					var retrievedOrder OrderResponse
					err = json.NewDecoder(resp.Body).Decode(&retrievedOrder)
					if err == nil {
						t.Logf("Successfully read order %d: %+v", i, retrievedOrder)
					}
				}
			}
		}

		assert.Greater(t, createdCount, 0, "At least some orders should be created")
		t.Logf("Successfully created %d out of %d orders", createdCount, numOrders)
	})
}

func TestReplicationLagHandling(t *testing.T) {
	// Setup Docker environment
	//cleanup := setupDockerEnvironment(t)
	//defer cleanup()

	// Start the server
	//serverCmd, err := startServer()
	//require.NoError(t, err, "Failed to start server")
	//defer func() {
	//	if serverCmd.Process != nil {
	//		serverCmd.Process.Kill()
	//	}
	//}()

	// Wait for server to be ready
	//waitForServer(t)

	// Create HTTP client with cookie jar
	jar, err := cookiejar.New(nil)
	require.NoError(t, err, "Failed to create cookie jar")

	client := &http.Client{
		Jar:     jar,
		Timeout: timeout,
	}

	t.Run("Immediate Read After Write", func(t *testing.T) {
		// Create an order
		createData := bytes.NewBufferString("customer_name=Lag Test Customer&amount=123.45&status=pending")
		resp, err := client.Post(serverURL+"/orders", "application/x-www-form-urlencoded", createData)
		require.NoError(t, err, "Failed to create order")
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var createdOrder OrderResponse
		err = json.NewDecoder(resp.Body).Decode(&createdOrder)
		require.NoError(t, err)

		t.Logf("Created order: %+v", createdOrder)

		// Immediately try to read the order multiple times
		// Due to 5-second replication lag, some reads should fall back to master
		successfulReads := 0
		for i := 0; i < 10; i++ {
			resp, err := client.Get(fmt.Sprintf("%s/orders/get?id=%d", serverURL, createdOrder.ID))
			if err != nil {
				t.Logf("Read attempt %d failed: %v", i+1, err)
				continue
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				var retrievedOrder OrderResponse
				err = json.NewDecoder(resp.Body).Decode(&retrievedOrder)
				if err == nil && retrievedOrder.ID == createdOrder.ID {
					successfulReads++
					t.Logf("Read attempt %d successful", i+1)
				}
			}
			time.Sleep(500 * time.Millisecond)
		}

		assert.Greater(t, successfulReads, 0, "Should be able to read order consistently despite replication lag")
		t.Logf("Successful immediate reads: %d out of 10", successfulReads)

		// Wait for replication to catch up and verify
		t.Log("Waiting for replication to catch up...")
		time.Sleep(6 * time.Second) // Wait past the 5-second lag

		resp, err = client.Get(serverURL + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()

		var health HealthResponse
		err = json.NewDecoder(resp.Body).Decode(&health)
		require.NoError(t, err)

		t.Logf("Final health status: %+v", health)
	})
}
