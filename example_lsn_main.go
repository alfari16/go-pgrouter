//go:build example
// +build example

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/alfari16/pg-consistent-replica"
	_ "github.com/lib/pq"
)

// This example demonstrates PostgreSQL LSN-based causal consistency with dbresolver
//
// Usage:
//   go run -tags example example_lsn_main.go
//
// Note: This example requires PostgreSQL with replication setup.
// Modify the connection strings to match your environment.

func main() {
	// Example database connections - modify these for your environment
	primaryDSN := "host=localhost port=5432 user=postgresrw dbname=testdb sslmode=disable password=yourpassword"
	replicaDSN := "host=localhost port=5433 user=postgresro dbname=testdb sslmode=disable password=yourpassword"

	// Open database connections
	primaryDB, err := sql.Open("postgres", primaryDSN)
	if err != nil {
		log.Fatalf("Failed to open primary database: %v", err)
	}
	defer primaryDB.Close()

	replicaDB, err := sql.Open("postgres", replicaDSN)
	if err != nil {
		log.Fatalf("Failed to open replica database: %v", err)
	}
	defer replicaDB.Close()

	// Test basic connectivity
	if err := primaryDB.Ping(); err != nil {
		log.Fatalf("Primary database connection failed: %v", err)
	}
	log.Println("✓ Primary database connected successfully")

	if err := replicaDB.Ping(); err != nil {
		log.Fatalf("Replica database connection failed: %v", err)
	}
	log.Println("✓ Replica database connected successfully")

	// Create test table if it doesn't exist
	if err := createTestTable(primaryDB); err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}
	log.Println("✓ Test table ready")

	// Example 1: Basic dbresolver without LSN features
	log.Println("\n=== Example 1: Basic dbresolver ===")
	basicDB := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB),
	)

	demonstrateBasicQueries(basicDB)

	// Example 2: dbresolver with LSN-based causal consistency
	log.Println("\n=== Example 2: LSN-based Causal Consistency ===")
	lsnDB := setupLSNResolver(primaryDB, replicaDB)
	demonstrateLSNQueries(lsnDB)

	// Example 3: Manual LSN handling
	log.Println("\n=== Example 3: Manual LSN Handling ===")
	demonstrateManualLSNHandling(lsnDB)

	// Example 4: Monitoring replica health
	log.Println("\n=== Example 4: Replica Health Monitoring ===")
	demonstrateHealthMonitoring(lsnDB)

	log.Println("\n✓ All examples completed successfully!")
}

func createTestTable(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS products (
		id SERIAL PRIMARY KEY,
		name VARCHAR(100) NOT NULL,
		price DECIMAL(10,2) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := db.Exec(query)
	return err
}

func setupLSNResolver(primaryDB, replicaDB *sql.DB) dbresolver.DB {
	// Configure LSN-based causal consistency
	ccConfig := &dbresolver.CausalConsistencyConfig{
		Enabled:          true,
		Level:            dbresolver.ReadYourWrites,
		RequireCookie:    true,
		CookieName:       "pg_min_lsn",
		CookieMaxAge:     5 * time.Minute,
		FallbackToMaster: true,
	}

	// Create database resolver with LSN features
	return dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
		dbresolver.WithCausalConsistency(ccConfig),
		dbresolver.WithLSNQueryTimeout(3*time.Second),
		dbresolver.WithLSNThrottleTime(100*time.Millisecond),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB),
	)
}

func demonstrateBasicQueries(db dbresolver.DB) {
	ctx := context.Background()

	// Insert a product (write operation - goes to primary)
	var productID int
	err := db.QueryRowContext(ctx,
		"INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id",
		"Basic Product", 29.99).Scan(&productID)

	if err != nil {
		log.Printf("❌ Failed to insert product: %v", err)
		return
	}
	log.Printf("✓ Inserted product with ID: %d (using primary)", productID)

	// Query the product (read operation - may use replica)
	var name string
	var price float64
	err = db.QueryRowContext(ctx,
		"SELECT name, price FROM products WHERE id = $1", productID).
		Scan(&name, &price)

	if err != nil {
		log.Printf("❌ Failed to query product: %v", err)
		return
	}
	log.Printf("✓ Queried product: %s ($%.2f) (may use replica)", name, price)
}

func demonstrateLSNQueries(db dbresolver.DB) {
	ctx := context.Background()

	if !db.IsCausalConsistencyEnabled() {
		log.Println("⚠ LSN-based causal consistency is not enabled")
		return
	}
	log.Println("✓ LSN-based causal consistency is enabled")

	// Insert a product with LSN tracking
	var productID int
	err := db.QueryRowContext(ctx,
		"INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id",
		"LSN Product", 39.99).Scan(&productID)

	if err != nil {
		log.Printf("❌ Failed to insert product: %v", err)
		return
	}
	log.Printf("✓ Inserted LSN product with ID: %d", productID)

	// Update LSN tracking after write
	lsn, err := db.UpdateLSNAfterWrite(ctx)
	if err != nil {
		log.Printf("⚠ Failed to update LSN after write: %v", err)
	} else {
		log.Printf("✓ Updated LSN after write: %s", lsn.String())
	}

	// Get current master LSN
	currentLSN, err := db.GetCurrentMasterLSN(ctx)
	if err != nil {
		log.Printf("⚠ Failed to get current master LSN: %v", err)
	} else {
		log.Printf("✓ Current master LSN: %s", currentLSN.String())
	}

	// Query with LSN context (read-your-writes consistency)
	lsnCtx := &dbresolver.LSNContext{
		RequiredLSN: lsn,
		Level:       dbresolver.ReadYourWrites,
	}
	ctx = dbresolver.WithLSNContext(ctx, lsnCtx)

	var name string
	var price float64
	err = db.QueryRowContext(ctx,
		"SELECT name, price FROM products WHERE id = $1", productID).
		Scan(&name, &price)

	if err != nil {
		log.Printf("❌ Failed to query product with LSN context: %v", err)
		return
	}
	log.Printf("✓ Queried product with LSN consistency: %s ($%.2f)", name, price)
}

func demonstrateManualLSNHandling(db dbresolver.DB) {
	ctx := context.Background()

	// Insert another product
	var productID int
	err := db.QueryRowContext(ctx,
		"INSERT INTO products (name, price) VALUES ($1, $2) RETURNING id",
		"Manual LSN Product", 49.99).Scan(&productID)

	if err != nil {
		log.Printf("❌ Failed to insert product: %v", err)
		return
	}

	// Get master LSN
	masterLSN, err := db.GetCurrentMasterLSN(ctx)
	if err != nil {
		log.Printf("❌ Failed to get master LSN: %v", err)
		return
	}
	log.Printf("✓ Master LSN after insert: %s", masterLSN.String())

	// Create LSN context for reading
	lsnCtx := &dbresolver.LSNContext{
		RequiredLSN: masterLSN,
		Level:       dbresolver.ReadYourWrites,
		ForceMaster: false, // Allow use of replica if caught up
	}
	ctx = dbresolver.WithLSNContext(ctx, lsnCtx)

	// This query will use replica only if it has caught up to masterLSN
	var name string
	var price float64
	err = db.QueryRowContext(ctx,
		"SELECT name, price FROM products WHERE id = $1", productID).
		Scan(&name, &price)

	if err != nil {
		log.Printf("❌ Failed to query product with manual LSN context: %v", err)
		return
	}
	log.Printf("✓ Manual LSN query successful: %s ($%.2f)", name, price)

	// Force master query for comparison
	forceMasterCtx := &dbresolver.LSNContext{
		ForceMaster: true,
	}
	ctx = dbresolver.WithLSNContext(ctx, forceMasterCtx)

	err = db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM products").Scan(new(int))

	if err != nil {
		log.Printf("❌ Failed to force master query: %v", err)
		return
	}
	log.Printf("✓ Forced master query completed")
}

func demonstrateHealthMonitoring(db dbresolver.DB) {
	if !db.IsCausalConsistencyEnabled() {
		log.Println("⚠ Health monitoring requires LSN features to be enabled")
		return
	}

	// Get replica status
	replicaStatuses := db.GetReplicaStatus()
	if replicaStatuses == nil {
		log.Println("⚠ No replica status available")
		return
	}

	log.Printf("✓ Found %d replicas", len(replicaStatuses))

	for i, status := range replicaStatuses {
		log.Printf("  Replica %d:", i+1)
		log.Printf("    Healthy: %t", status.IsHealthy)
		log.Printf("    Last Check: %s", status.LastCheck.Format(time.RFC3339))
		log.Printf("    Error Count: %d", status.ErrorCount)

		if status.LastError != nil {
			log.Printf("    Last Error: %v", status.LastError)
		}

		if status.LastLSN != nil {
			log.Printf("    Last LSN: %s", status.LastLSN.String())
		}

		if status.LagBytes > 0 {
			log.Printf("    Lag: %d bytes", status.LagBytes)
		}
	}

	// Get last known master LSN
	lastKnownLSN := db.GetLastKnownMasterLSN()
	if lastKnownLSN != nil {
		log.Printf("✓ Last known master LSN: %s", lastKnownLSN.String())
	} else {
		log.Println("⚠ No last known master LSN available")
	}

	// Wait a moment for background monitoring to collect data
	log.Println("⏳ Waiting for background monitoring...")
	time.Sleep(2 * time.Second)

	// Check updated status
	updatedStatuses := db.GetReplicaStatus()
	if updatedStatuses != nil && len(updatedStatuses) > 0 {
		status := updatedStatuses[0]
		log.Printf("✓ Updated replica health: %t", status.IsHealthy)
	}
}
