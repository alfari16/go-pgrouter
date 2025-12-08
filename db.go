package dbresolver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"time"

	"go.uber.org/multierr"
)

// QueryRouter interface defines the contract for query routing strategies
// This follows the Open-Closed Principle, allowing different routing implementations
type QueryRouter interface {
	// RouteQuery routes a query to the appropriate database based on query type and context
	RouteQuery(ctx context.Context, queryType QueryType) (*sql.DB, error)
	// UpdateLSNAfterWrite updates LSN tracking after a write operation (optional)
	// Implementations can return zero LSN and nil error if LSN tracking is not supported
	UpdateLSNAfterWrite(ctx context.Context, db *sql.DB) (LSN, error)
}

// DBLoadBalancer is loadbalancer for physical DBs
type DBLoadBalancer LoadBalancer[*sql.DB]

// StmtLoadBalancer is loadbalancer for query prepared statements
type StmtLoadBalancer LoadBalancer[*sql.Stmt]

// DB is a logical database with multiple underlying physical databases
// forming a single ReadWrite (primary) with multiple ReadOnly(replicas) db.
// Reads and writes are automatically directed to the correct db connection
// with optional LSN-based causal consistency support.

type DB struct {
	primaries        []*sql.DB
	replicas         []*sql.DB
	loadBalancer     DBLoadBalancer
	stmtLoadBalancer StmtLoadBalancer
	queryTypeChecker QueryTypeChecker
	queryRouter      QueryRouter
}

// PrimaryDBs return all the active primary DB
func (db *DB) PrimaryDBs() []*sql.DB {
	return db.primaries
}

// ReplicaDBs return all the active replica DB
func (db *DB) ReplicaDBs() []*sql.DB {
	return db.replicas
}

// LoadBalancer returns the database load balancer
func (db *DB) LoadBalancer() LoadBalancer[*sql.DB] {
	return db.loadBalancer
}

// IsCausalConsistencyEnabled returns true if causal consistency (LSN tracking) is enabled
func (db *DB) IsCausalConsistencyEnabled() bool {
	_, ok := db.queryRouter.(*CausalRouter)
	return ok
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	var errors []error

	errPrimaries := doParallely(len(db.primaries), func(i int) error {
		return db.primaries[i].Close()
	})
	errReplicas := doParallely(len(db.replicas), func(i int) error {
		return db.replicas[i].Close()
	})

	// Combine all errors
	if errPrimaries != nil {
		errors = append(errors, errPrimaries)
	}
	if errReplicas != nil {
		errors = append(errors, errReplicas)
	}

	if len(errors) > 0 {
		return multierr.Combine(errors...)
	}
	return nil
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.ReadWrite().Driver()
}

// Begin starts a transaction on the RW-db. The isolation level is dependent on the driver.
func (db *DB) Begin() (Tx, error) {
	return db.BeginTx(context.Background(), nil)
}

// BeginTx starts a transaction with the provided context on the RW-db.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Tx, error) {
	sourceDB := db.ReadWrite()

	stx, err := sourceDB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &tx{
		sourceDB:         sourceDB,
		tx:               stx,
		queryTypeChecker: db.queryTypeChecker,
	}, nil
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.ExecContext(context.Background(), query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
// Exec uses the RW-database as the underlying db connection
// Optimized version: Uses single responsibility function for LSN tracking
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	usedDB := db.ReadWrite()
	result, err := usedDB.ExecContext(ctx, query, args...)

	return result, err
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping() error {
	return db.PingContext(context.Background())
}

// PingContext verifies if a connection to each physical database is still
// alive, establishing a connection if necessary.
func (db *DB) PingContext(ctx context.Context) error {
	errPrimaries := doParallely(len(db.primaries), func(i int) error {
		return db.primaries[i].PingContext(ctx)
	})
	errReplicas := doParallely(len(db.replicas), func(i int) error {
		return db.replicas[i].PingContext(ctx)
	})
	return multierr.Combine(errPrimaries, errReplicas)
}

// Prepare creates a prepared statement for later queries or executions
// on each physical database, concurrently.
func (db *DB) Prepare(query string) (_stmt Stmt, err error) {
	return db.PrepareContext(context.Background(), query)
}

// PrepareContext creates a prepared statement for later queries or executions
// on each physical database, concurrently.
//
// The provided context is used for the preparation of the statement, not for
// the execution of the statement.
func (db *DB) PrepareContext(ctx context.Context, query string) (_stmt Stmt, err error) {
	dbStmt := map[*sql.DB]*sql.Stmt{}
	var dbStmtLock sync.Mutex
	roStmts := make([]*sql.Stmt, len(db.replicas))
	primaryStmts := make([]*sql.Stmt, len(db.primaries))
	errPrimaries := doParallely(len(db.primaries), func(i int) (err error) {
		primaryStmts[i], err = db.primaries[i].PrepareContext(ctx, query)
		dbStmtLock.Lock()
		dbStmt[db.primaries[i]] = primaryStmts[i]
		dbStmtLock.Unlock()
		return
	})

	errReplicas := doParallely(len(db.replicas), func(i int) (err error) {
		roStmts[i], err = db.replicas[i].PrepareContext(ctx, query)
		dbStmtLock.Lock()
		dbStmt[db.replicas[i]] = roStmts[i]
		dbStmtLock.Unlock()

		// if connection error happens on RO connection,
		// ignore and fallback to RW connection
		if isDBConnectionError(err) {
			roStmts[i] = primaryStmts[0]
			return nil
		}
		return err
	})

	err = multierr.Combine(errPrimaries, errReplicas)
	if err != nil {
		return //nolint: nakedret
	}

	writeFlag := db.queryTypeChecker.Check(query)

	_stmt = &stmt{
		loadBalancer: db.stmtLoadBalancer,
		primaryStmts: primaryStmts,
		replicaStmts: roStmts,
		dbStmt:       dbStmt,
		writeFlag:    writeFlag == QueryTypeWrite,
	}
	return _stmt, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.QueryContext(context.Background(), query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	var curDB *sql.DB
	writeFlag := db.queryTypeChecker.Check(query) == QueryTypeWrite

	if writeFlag {
		curDB = db.ReadWrite()
	} else {
		// Use query router for read operations if available
		if db.queryRouter != nil {
			curDB = db.ReadWithLSN(ctx)
		} else {
			curDB = db.ReadOnly()
		}
	}

	rows, err = curDB.QueryContext(ctx, query, args...)

	// Handle connection error fallback
	if isDBConnectionError(err) && !writeFlag {
		rows, err = db.ReadWrite().QueryContext(ctx, query, args...)
	}

	return
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.QueryRowContext(context.Background(), query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row.
// QueryRowContext always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var curDB *sql.DB
	writeFlag := db.queryTypeChecker.Check(query) == QueryTypeWrite

	if writeFlag {
		curDB = db.ReadWrite()
	} else {
		// Use query router for read operations if available
		if db.queryRouter != nil {
			curDB = db.ReadWithLSN(ctx)
		} else {
			curDB = db.ReadOnly()
		}
	}

	row := curDB.QueryRowContext(ctx, query, args...)

	// Handle connection error fallback
	if isDBConnectionError(row.Err()) && !writeFlag {
		row = db.ReadWrite().QueryRowContext(ctx, query, args...)
	}

	return row
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying db connection
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.primaries {
		db.primaries[i].SetMaxIdleConns(n)
	}

	for i := range db.replicas {
		db.replicas[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical db.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	for i := range db.primaries {
		db.primaries[i].SetMaxOpenConns(n)
	}
	for i := range db.replicas {
		db.replicas[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.primaries {
		db.primaries[i].SetConnMaxLifetime(d)
	}
	for i := range db.replicas {
		db.replicas[i].SetConnMaxLifetime(d)
	}
}

// SetConnMaxIdleTime sets the maximum amount of time a connection may be idle.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are not closed due to a connection's idle time.
func (db *DB) SetConnMaxIdleTime(d time.Duration) {
	for i := range db.primaries {
		db.primaries[i].SetConnMaxIdleTime(d)
	}

	for i := range db.replicas {
		db.replicas[i].SetConnMaxIdleTime(d)
	}
}

// ReadOnly returns the readonly database
func (db *DB) ReadOnly() *sql.DB {
	if len(db.replicas) == 0 {
		return db.loadBalancer.Resolve(db.primaries)
	}
	return db.loadBalancer.Resolve(db.replicas)
}

// ReadWithLSN returns a readonly database considering query router requirements
func (db *DB) ReadWithLSN(ctx context.Context) *sql.DB {
	// If no query router is available, fall back to standard routing
	if db.queryRouter == nil {
		return db.ReadOnly()
	}

	// Use query router for routing
	selectedDB, err := db.queryRouter.RouteQuery(ctx, QueryTypeRead)
	if err != nil {
		// Fallback to standard routing if routing fails
		return db.ReadOnly()
	}

	return selectedDB
}

// ReadWrite returns the primary database
func (db *DB) ReadWrite() *sql.DB {
	return db.loadBalancer.Resolve(db.primaries)
}

// Conn returns a single connection by either opening a new connection or returning an existing connection from the
// connection pool of the first primary db.
func (db *DB) Conn(ctx context.Context) (Conn, error) {
	c, err := db.primaries[0].Conn(ctx)
	if err != nil {
		return nil, err
	}

	return &conn{
		sourceDB:         db.primaries[0],
		conn:             c,
		queryTypeChecker: db.queryTypeChecker,
	}, nil
}

// Stats returns database statistics for the first primary db
func (db *DB) Stats() sql.DBStats {
	return db.primaries[0].Stats()
}
