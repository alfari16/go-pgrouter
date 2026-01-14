package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	dbresolver "github.com/alfari16/go-pgrouter"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Order represents a simple order model
type Order struct {
	ID           int       `json:"id"`
	CustomerName string    `json:"customer_name"`
	Amount       float64   `json:"amount"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// setupDatabase creates a new database resolver with LSN-enabled causal consistency
func setupDatabase() (*dbresolver.DB, error) {
	// Primary (master) database connection with connection pooling
	primaryDB, err := sql.Open("pgx", "host=localhost port=5432 user=user dbname=mydb sslmode=disable password=password")
	if err != nil {
		return nil, fmt.Errorf("failed to open primary database: %w", err)
	}

	// Configure connection pool for primary
	primaryDB.SetMaxOpenConns(20)
	primaryDB.SetMaxIdleConns(5)
	primaryDB.SetConnMaxLifetime(1 * time.Hour)

	// Replica (read-only) database connection with connection pooling
	replicaDB, err := sql.Open("pgx", "host=localhost port=5433 user=user dbname=mydb sslmode=disable password=password")
	if err != nil {
		return nil, fmt.Errorf("failed to open replica database: %w", err)
	}

	// Configure connection pool for replica
	replicaDB.SetMaxOpenConns(20)
	replicaDB.SetMaxIdleConns(5)
	replicaDB.SetConnMaxLifetime(1 * time.Hour)

	// Test database connections
	if err := primaryDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to primary database: %w", err)
	}
	if err := replicaDB.Ping(); err != nil {
		log.Printf("Warning: failed to connect to replica database (continuing with primary only): %v", err)
		// Don't fail here - we can work with just the primary
	}

	// Configure LSN-based causal consistency
	ccConfig := &dbresolver.CausalConsistencyConfig{
		Enabled:          true,
		Level:            dbresolver.ReadYourWrites,
		FallbackToMaster: true,
		Timeout:          3 * time.Second,
	}

	// Create database resolver with LSN features
	db := dbresolver.New(
		dbresolver.WithPrimaryDBs(primaryDB),
		dbresolver.WithReplicaDBs(replicaDB),
		dbresolver.WithCausalConsistencyConfig(ccConfig),
		dbresolver.WithLSNQueryTimeout(3*time.Second),
		dbresolver.WithLoadBalancer(dbresolver.RoundRobinLB),
	)

	return db, nil
}

// HTTP handlers demonstrating LSN-aware database operations

// createOrderHandler demonstrates a write operation that updates LSN cookies
func createOrderHandler(db *dbresolver.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Parse order data (write operation)
		customerName := r.FormValue("customer_name")
		amountStr := r.FormValue("amount")
		status := r.FormValue("status")

		if customerName == "" || amountStr == "" {
			http.Error(w, "customer_name and amount are required", http.StatusBadRequest)
			return
		}

		// Set default status if not provided
		if status == "" {
			status = "pending"
		}

		// Parse amount
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			http.Error(w, "Invalid amount format", http.StatusBadRequest)
			return
		}

		// Insert order into database (this goes to primary)
		var orderID int
		err = db.QueryRowContext(ctx,
			"INSERT INTO orders (customer_name, amount, status) VALUES ($1, $2, $3) RETURNING id",
			customerName, amount, status).Scan(&orderID)

		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to create order: %v", err), http.StatusInternalServerError)
			return
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, `{"id": %d, "customer_name": "%s", "amount": %.2f, "status": "%s", "created_at": "%s"}`,
			orderID, customerName, amount, status, time.Now().Format(time.RFC3339))
	}
}

// getOrderHandler demonstrates a read operation that respects LSN consistency
func getOrderHandler(db *dbresolver.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		orderID := r.URL.Query().Get("id")

		if orderID == "" {
			http.Error(w, "Order ID is required", http.StatusBadRequest)
			return
		}

		// This read operation will be routed to replica only if it has caught up
		// to the required LSN (from cookie), otherwise it will use primary
		var order Order
		err := db.QueryRowContext(ctx,
			"SELECT id, customer_name, amount, status, created_at, updated_at FROM orders WHERE id = $1", orderID).
			Scan(&order.ID, &order.CustomerName, &order.Amount, &order.Status, &order.CreatedAt, &order.UpdatedAt)

		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Order not found", http.StatusNotFound)
				return
			}
			http.Error(w, fmt.Sprintf("Failed to get order: %v", err), http.StatusInternalServerError)
			return
		}

		// Log that we're using LSN-aware routing (enabled in setupDatabase)
		log.Printf("Retrieved order %s using LSN-aware routing", orderID)

		// Return order data
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id": %d, "customer_name": "%s", "amount": %.2f, "status": "%s", "created_at": "%s"}`,
			order.ID, order.CustomerName, order.Amount, order.Status, order.CreatedAt.Format(time.RFC3339))
	}
}

// listOrdersHandler demonstrates reading multiple orders
func listOrdersHandler(db *dbresolver.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Get query parameters for filtering
		status := r.URL.Query().Get("status")
		limit := r.URL.Query().Get("limit")

		// Set default limit
		limitInt := 10
		if limit != "" {
			if parsedLimit, err := strconv.Atoi(limit); err == nil && parsedLimit > 0 {
				limitInt = parsedLimit
			}
		}

		// Build query dynamically
		query := "SELECT id, customer_name, amount, status, created_at, updated_at FROM orders"
		var args []interface{}
		argIndex := 1

		if status != "" {
			query += " WHERE status = $" + strconv.Itoa(argIndex)
			args = append(args, status)
			argIndex++
		}

		query += " ORDER BY created_at DESC LIMIT $" + strconv.Itoa(argIndex)
		args = append(args, limitInt)

		// Query multiple orders - will use replica if safe, primary otherwise
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list orders: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var orders []Order
		for rows.Next() {
			var order Order
			if err := rows.Scan(&order.ID, &order.CustomerName, &order.Amount, &order.Status, &order.CreatedAt, &order.UpdatedAt); err != nil {
				http.Error(w, fmt.Sprintf("Failed to scan order: %v", err), http.StatusInternalServerError)
				return
			}
			orders = append(orders, order)
		}

		if err := rows.Err(); err != nil {
			http.Error(w, fmt.Sprintf("Row iteration error: %v", err), http.StatusInternalServerError)
			return
		}

		// Log that we're using LSN-aware routing
		log.Printf("Listed %d orders using LSN-aware routing", len(orders))

		// Return orders list
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"orders": [`))
		for i, order := range orders {
			if i > 0 {
				w.Write([]byte(","))
			}
			fmt.Fprintf(w, `{"id": %d, "customer_name": "%s", "amount": %.2f, "status": "%s", "created_at": "%s"}`,
				order.ID, order.CustomerName, order.Amount, order.Status, order.CreatedAt.Format(time.RFC3339))
		}
		w.Write([]byte("]}"))
	}
}

// healthHandler shows database status
func healthHandler(db *dbresolver.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get status
		status := struct {
			Healthy      bool      `json:"healthy"`
			LSNEnabled   bool      `json:"lsn_enabled"`
			ReplicaCount int       `json:"replica_count"`
			CheckTime    time.Time `json:"check_time"`
		}{
			Healthy:      true,
			LSNEnabled:   true, // Enabled in setupDatabase
			ReplicaCount: len(db.ReplicaDBs()),
			CheckTime:    time.Now(),
		}

		// Return JSON response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{
			"healthy": %t,
			"lsn_enabled": %t,
			"replica_count": %d,
			"check_time": "%s"
		}`,
			status.Healthy,
			status.LSNEnabled,
			status.ReplicaCount,
			status.CheckTime.Format(time.RFC3339))
	}
}

// getRouter creates a QueryRouter for the middleware
func getRouter(db *dbresolver.DB) dbresolver.QueryRouter {
	// Since LSN is enabled in setupDatabase, create a causal router
	// The DB itself implements DBProvider interface
	config := &dbresolver.CausalConsistencyConfig{
		Enabled:          true,
		Level:            dbresolver.ReadYourWrites,
		FallbackToMaster: true,
		Timeout:          3 * time.Second,
	}
	return dbresolver.NewCausalRouter(db, config)
}

func main() {
	// Set up database with LSN support
	db, err := setupDatabase()
	if err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}
	defer db.Close()

	// LSN is enabled in setupDatabase via ccConfig.Enabled = true
	log.Println("LSN-based causal consistency is enabled")

	// Create router for middleware
	router := getRouter(db)

	// Create LSN-aware middleware with secure cookies for production
	// Set useSecureCookie to false for local development
	middleware := dbresolver.NewHTTPMiddleware(router, "pg_min_lsn", 5*time.Minute, false)

	// Create HTTP router with LSN middleware
	mux := http.NewServeMux()

	// Apply LSN middleware to all routes
	handler := middleware.Middleware(mux)

	// Register routes
	mux.HandleFunc("/orders", createOrderHandler(db))
	mux.HandleFunc("/orders/list", listOrdersHandler(db))
	mux.HandleFunc("/orders/get", getOrderHandler(db))
	mux.HandleFunc("/health", healthHandler(db))

	// Start HTTP server
	server := &http.Server{
		Addr:         ":8080",
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	log.Println("Starting server on :8080 with LSN-aware routing")
	log.Println("Endpoints:")
	log.Println("  POST /orders - Create order (write)")
	log.Println("  GET  /orders/get?id=1 - Get order (read with LSN consistency)")
	log.Println("  GET  /orders/list - List orders (read)")
	log.Println("  GET  /health - Database health and LSN status")

	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
