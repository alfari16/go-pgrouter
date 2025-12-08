package dbresolver

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// PGLSNCheckerRegistry manages singleton instances per DB connection
type PGLSNCheckerRegistry struct {
	mu       sync.RWMutex
	checkers map[*sql.DB]*PGLSNChecker
}

var (
	globalRegistry *PGLSNCheckerRegistry
	registryOnce   sync.Once
)

// getRegistry returns the singleton registry instance
func getRegistry() *PGLSNCheckerRegistry {
	registryOnce.Do(func() {
		globalRegistry = &PGLSNCheckerRegistry{
			checkers: make(map[*sql.DB]*PGLSNChecker),
		}
	})
	return globalRegistry
}

// getOrCreateChecker returns existing instance or creates new one
func getOrCreateChecker(db *sql.DB, queryTimeout time.Duration) *PGLSNChecker {
	if db == nil {
		return nil
	}

	registry := getRegistry()

	// Try to get existing instance with read lock
	registry.mu.RLock()
	if checker, exists := registry.checkers[db]; exists {
		registry.mu.RUnlock()
		return checker
	}
	registry.mu.RUnlock()

	// Create new instance with write lock
	registry.mu.Lock()
	defer registry.mu.Unlock()

	// Double-check after acquiring write lock
	if checker, exists := registry.checkers[db]; exists {
		return checker
	}

	// Create new checker
	checker := &PGLSNChecker{
		db:           db,
		queryTimeout: queryTimeout,
	}
	registry.checkers[db] = checker
	return checker
}

// PGLSNChecker handles PostgreSQL-specific LSN queries and operations
type PGLSNChecker struct {
	db           *sql.DB
	queryTimeout time.Duration
}

// PGLSNCheckerOption configures the PGLSNChecker
type PGLSNCheckerOption func(*PGLSNChecker)

// WithQueryTimeout sets the timeout for LSN queries
func WithQueryTimeout(timeout time.Duration) PGLSNCheckerOption {
	return func(c *PGLSNChecker) {
		c.queryTimeout = timeout
	}
}

// GetCurrentWALLSN queries the current WAL LSN from the master database
func (c *PGLSNChecker) GetCurrentWALLSN(ctx context.Context) (LSN, error) {
	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	var lsnStr string
	err := c.db.QueryRowContext(queryCtx, "SELECT "+PGCurrentWALLSN).Scan(&lsnStr)
	if err != nil {
		return LSN{}, fmt.Errorf("failed to get current WAL LSN: %w", err)
	}

	lsn, err := ParseLSN(lsnStr)
	if err != nil {
		return LSN{}, fmt.Errorf("failed to parse master LSN: %w", err)
	}

	return lsn, nil
}

// GetLastReplayLSN queries the last replay LSN from a replica database
func (c *PGLSNChecker) GetLastReplayLSN(ctx context.Context) (LSN, error) {
	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	var lsnStr string
	err := c.db.QueryRowContext(queryCtx, "SELECT "+PGLastWalReplayLSN).Scan(&lsnStr)
	if err != nil {
		return LSN{}, fmt.Errorf("failed to get last replay LSN: %w", err)
	}

	lsn, err := ParseLSN(lsnStr)
	if err != nil {
		return LSN{}, fmt.Errorf("failed to parse replica LSN: %w", err)
	}

	return lsn, nil
}

// GetReplicationLag calculates the replication lag in bytes between master and replica
func (c *PGLSNChecker) GetReplicationLag(ctx context.Context, masterLSN LSN) (uint64, error) {
	replicaLSN, err := c.GetLastReplayLSN(ctx)
	if err != nil {
		return 0, err
	}

	// Calculate the lag
	lag := masterLSN.Subtract(replicaLSN)
	return lag, nil
}

// IsReplicaHealthy checks if a replica is healthy and within acceptable lag
func (c *PGLSNChecker) IsReplicaHealthy(ctx context.Context, masterLSN LSN, maxLagBytes uint64) (bool, error) {
	replicaLSN, err := c.GetLastReplayLSN(ctx)
	if err != nil {
		// If we can't query the replica, consider it unhealthy
		return false, err
	}

	// Check if replica has caught up to master (or within acceptable lag)
	if !masterLSN.IsZero() {
		lag := masterLSN.Subtract(replicaLSN)
		return lag <= maxLagBytes, nil
	}

	// If no master LSN provided, just check if replica is responding
	return true, nil
}

// GetWALLagBytes queries the WAL lag in bytes between two LSNs using pg_wal_lsn_diff
func (c *PGLSNChecker) GetWALLagBytes(ctx context.Context, fromLSN, toLSN LSN) (uint64, error) {
	if fromLSN.IsZero() || toLSN.IsZero() {
		return 0, fmt.Errorf("both LSNs must be non-zero")
	}

	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	var lagBytes uint64
	query := fmt.Sprintf("SELECT pg_wal_lsn_diff('%s'::pg_lsn, '%s'::pg_lsn)", toLSN.String(), fromLSN.String())

	err := c.db.QueryRowContext(queryCtx, query).Scan(&lagBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to get WAL lag: %w", err)
	}

	return lagBytes, nil
}

// TestConnection performs a basic connection test
func (c *PGLSNChecker) TestConnection(ctx context.Context) error {
	queryCtx, cancel := context.WithTimeout(ctx, c.queryTimeout)
	defer cancel()

	return c.db.PingContext(queryCtx)
}
