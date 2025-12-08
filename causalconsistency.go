package dbresolver

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// DBProvider interface provides access to primary and replica databases
type DBProvider interface {
	PrimaryDBs() []*sql.DB
	ReplicaDBs() []*sql.DB
	LoadBalancer() LoadBalancer[*sql.DB]
}

// SimpleRouter implements QueryRouter with basic read/write routing without LSN tracking
type SimpleRouter struct {
	dbProvider DBProvider
}

// NewSimpleRouter creates a new simple router without LSN tracking
func NewSimpleRouter(dbProvider DBProvider) *SimpleRouter {
	return &SimpleRouter{
		dbProvider: dbProvider,
	}
}

// RouteQuery implements basic read/write routing
func (r *SimpleRouter) RouteQuery(ctx context.Context, queryType QueryType) (*sql.DB, error) {
	if r.dbProvider == nil {
		return nil, fmt.Errorf("no database provider available")
	}

	primaries := r.dbProvider.PrimaryDBs()
	replicas := r.dbProvider.ReplicaDBs()

	if len(primaries) == 0 {
		return nil, fmt.Errorf("no primary databases available")
	}

	switch queryType {
	case QueryTypeWrite:
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	case QueryTypeRead:
		if len(replicas) > 0 {
			return r.dbProvider.LoadBalancer().Resolve(replicas), nil
		}
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	default:
		// Default to primary for unknown query types
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	}
}

// UpdateLSNAfterWrite is a no-op for SimpleRouter since it doesn't track LSN
func (r *SimpleRouter) UpdateLSNAfterWrite(ctx context.Context, db *sql.DB) (LSN, error) {
	// Simple router doesn't track LSN, return zero LSN
	return LSN{}, nil
}

// CausalConsistencyLevel defines the level of causal consistency required
type CausalConsistencyLevel int

const (
	// NoneCausalConsistency - No causal consistency requirements (default behavior)
	NoneCausalConsistency CausalConsistencyLevel = iota
	// ReadYourWrites - Ensure reads see your own writes
	ReadYourWrites
	// StrongConsistency - Ensure all reads see the latest committed writes
	StrongConsistency
)

// CausalConsistencyConfig defines configuration for LSN-based causal consistency
type CausalConsistencyConfig struct {
	Enabled          bool                   // Enable LSN-based routing
	Level            CausalConsistencyLevel // Consistency level required
	RequireCookie    bool                   // Require LSN cookie for read-your-writes
	CookieName       string                 // HTTP cookie name for LSN tracking
	CookieMaxAge     time.Duration          // Maximum age for LSN cookie
	FallbackToMaster bool                   // Fallback to master when LSN requirements can't be met
	Timeout          time.Duration          // Timeout for LSN queries
}

// DefaultCausalConsistencyConfig returns default configuration for causal consistency
func DefaultCausalConsistencyConfig() *CausalConsistencyConfig {
	return &CausalConsistencyConfig{
		Enabled:          false,
		Level:            ReadYourWrites,
		RequireCookie:    true,
		CookieName:       "pg_min_lsn",
		CookieMaxAge:     5 * time.Minute,
		FallbackToMaster: true,
		Timeout:          5 * time.Second,
	}
}

// LSNContext holds LSN-related context information
type LSNContext struct {
	RequiredLSN       LSN
	Level             CausalConsistencyLevel
	ForceMaster       bool
	HasWriteOperation bool // Track if this request performed a write operation
}

// ReplicaStatus represents the health and replication status of a replica
type ReplicaStatus struct {
	IsHealthy  bool
	LastCheck  time.Time
	ErrorCount int
	LastError  error
	LastLSN    *LSN
	LagBytes   int64
}

// Context keys for storing LSN information in context
type contextKey string

const (
	lsnContextKey contextKey = "lsn_context"
	dbContextKey  contextKey = "db_connection"
)

// WithLSNContext adds LSN requirements to the context
func WithLSNContext(ctx context.Context, lsnCtx *LSNContext) context.Context {
	return context.WithValue(ctx, lsnContextKey, lsnCtx)
}

// GetLSNContext retrieves LSN context from the request context
func GetLSNContext(ctx context.Context) *LSNContext {
	if lsnCtx, ok := ctx.Value(lsnContextKey).(*LSNContext); ok {
		return lsnCtx
	}
	return nil
}

// WithDBConnection stores the selected DB connection in context
func WithDBConnection(ctx context.Context, db *sql.DB) context.Context {
	return context.WithValue(ctx, dbContextKey, db)
}

// GetDBConnection retrieves the DB connection from context
func GetDBConnection(ctx context.Context) *sql.DB {
	if db, ok := ctx.Value(dbContextKey).(*sql.DB); ok {
		return db
	}
	return nil
}

// CausalRouter provides LSN-aware database routing
type CausalRouter struct {
	config     *CausalConsistencyConfig
	dbProvider DBProvider // Dependency injected to access databases

	// Simple LSN tracking state
	mu            sync.RWMutex
	lastMasterLSN LSN

	// Configuration for on-demand checkers
	queryTimeout time.Duration
}

// NewCausalRouter creates a new LSN-aware router
func NewCausalRouter(dbProvider DBProvider, config *CausalConsistencyConfig) *CausalRouter {
	if config == nil {
		config = DefaultCausalConsistencyConfig()
	}

	return &CausalRouter{
		config:       config,
		dbProvider:   dbProvider,
		queryTimeout: 3 * time.Second, // Default timeout
	}
}

// RouteQuery routes a query to the appropriate database based on LSN requirements
// Optimized version: Cookie-first approach with simplified logic
func (r *CausalRouter) RouteQuery(ctx context.Context, queryType QueryType) (*sql.DB, error) {
	slog.Debug("RouteQuery", "queryType", queryType, "enabled", r.config.Enabled)

	if !r.config.Enabled || r.dbProvider == nil {
		slog.Debug("RouteQuery: causal consistency not enabled or no db provider")
		return nil, fmt.Errorf("causal consistency not enabled")
	}

	lsnCtx := GetLSNContext(ctx)
	primaries := r.dbProvider.PrimaryDBs()
	replicas := r.dbProvider.ReplicaDBs()

	slog.Debug("RouteQuery", "primaries", len(primaries), "replicas", len(replicas), "hasLSNContext", lsnCtx != nil)

	if len(primaries) == 0 {
		slog.Debug("RouteQuery: no primary databases available")
		return nil, fmt.Errorf("no primary databases available")
	}

	// If master is explicitly forced, use master
	if lsnCtx != nil && lsnCtx.ForceMaster {
		slog.Debug("RouteQuery: master forced, using primary")
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	}

	// For write operations, always use master
	if queryType == QueryTypeWrite {
		slog.Debug("RouteQuery: write operation, using primary")
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	}

	// For read operations: check cookie first
	switch r.config.Level {
	case ReadYourWrites:
		slog.Debug("RouteQuery: ReadYourWrites consistency level")
		// Check if we have LSN cookie requirements
		if lsnCtx != nil && !lsnCtx.RequiredLSN.IsZero() {
			slog.Debug("RouteQuery: checking replica status", "requiredLSN", lsnCtx.RequiredLSN)
			// Has LSN requirement - check if replica has caught up
			useReplica, db, err := r.shouldUseReplica(ctx, lsnCtx.RequiredLSN)
			if err != nil {
				slog.Debug("RouteQuery: failed to check replica status", "error", err)
				return nil, fmt.Errorf("failed to check replica status: %w", err)
			}
			if useReplica {
				slog.Debug("RouteQuery: using replica", "requiredLSN", lsnCtx.RequiredLSN)
				return db, nil
			}
			// Replica hasn't caught up yet, fall back to master
			if r.config.FallbackToMaster {
				slog.Debug("RouteQuery: replica not ready, falling back to master")
				return r.dbProvider.LoadBalancer().Resolve(primaries), nil
			}
			slog.Debug("RouteQuery: no replica has caught up to required LSN")
			return nil, fmt.Errorf("no replica has caught up to required LSN")
		}
		// No LSN cookie - use simple read/write routing (ignore LSN checking)
		slog.Debug("RouteQuery: no LSN cookie, falling through to simple routing")
		fallthrough

	case NoneCausalConsistency:
		slog.Debug("RouteQuery: NoneCausalConsistency level")
		// No LSN requirements, use any replica
		if len(replicas) > 0 {
			slog.Debug("RouteQuery: using replica", "replicaCount", len(replicas))
			return r.dbProvider.LoadBalancer().Resolve(replicas), nil
		}
		slog.Debug("RouteQuery: no replicas available, using primary")
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil

	case StrongConsistency:
		slog.Debug("RouteQuery: StrongConsistency level, using primary")
		// Always use master for strong consistency or when no LSN cookie
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	}

	// Default fallback to master
	if r.config.FallbackToMaster {
		slog.Debug("RouteQuery: default fallback to master")
		return r.dbProvider.LoadBalancer().Resolve(primaries), nil
	}
	slog.Debug("RouteQuery: unable to route query")
	return nil, fmt.Errorf("unable to route query: no suitable database found")
}

// shouldUseReplica determines if a replica should be used based on LSN requirements
func (r *CausalRouter) shouldUseReplica(ctx context.Context, requiredLSN LSN) (bool, *sql.DB, error) {
	replicas := r.dbProvider.ReplicaDBs()
	if len(replicas) == 0 {
		return false, nil, nil
	}

	// If LSN is zero, use load balancer to select any replica
	if requiredLSN.IsZero() {
		selected := r.dbProvider.LoadBalancer().Resolve(replicas)
		return true, selected, nil
	}

	// Try the load balancer selected replica first
	selected := r.dbProvider.LoadBalancer().Resolve(replicas)

	// Check if this replica has caught up to the required LSN
	checker := getOrCreateChecker(selected, r.queryTimeout)

	replicaLSN, err := checker.GetLastReplayLSN(ctx)
	if err == nil && !replicaLSN.LessThan(requiredLSN) {
		// Selected replica is ready to use
		return true, selected, nil
	}

	// Selected replica is lagged or error occurred, fall back to master
	return false, nil, nil
}

// GetLSNFromCookie extracts LSN from HTTP request cookies
func GetLSNFromCookie(r *http.Request, cookieName string) (LSN, bool) {
	if cookie, err := r.Cookie(cookieName); err == nil && cookie.Value != "" {
		if lsn, err := ParseLSN(cookie.Value); err == nil {
			return lsn, true
		}
	}
	return LSN{}, false
}

// UpdateLSNAfterWrite updates the LSN context after a write operation using the specific DB
// Optimized version: Event-driven, queries the specific DB that performed the write
func (r *CausalRouter) UpdateLSNAfterWrite(ctx context.Context, db *sql.DB) (LSN, error) {
	slog.Debug("UpdateLSNAfterWrite", "enabled", r.config.Enabled, "hasDB", db != nil)

	if !r.config.Enabled || db == nil {
		slog.Debug("UpdateLSNAfterWrite: LSN tracking not enabled or no DB provided, returning zero LSN")
		return LSN{}, nil
	}

	// Create checker on-demand for the specific DB using router's configuration
	checker := getOrCreateChecker(db, r.queryTimeout)
	slog.Debug("UpdateLSNAfterWrite: created/updated checker", "queryTimeout", r.queryTimeout)

	masterLSN, err := checker.GetCurrentWALLSN(ctx)
	if err != nil {
		slog.Debug("UpdateLSNAfterWrite: failed to get master LSN", "error", err)
		return LSN{}, fmt.Errorf("failed to get master LSN after write: %w", err)
	}

	slog.Debug("UpdateLSNAfterWrite: got master LSN", "masterLSN", masterLSN)

	// Update internal master LSN tracking
	r.mu.Lock()
	r.lastMasterLSN = masterLSN
	r.mu.Unlock()

	// Update context with new LSN requirement
	lsnCtx := GetLSNContext(ctx)
	if lsnCtx == nil {
		lsnCtx = &LSNContext{
			Level: r.config.Level,
		}
		slog.Debug("UpdateLSNAfterWrite: created new LSN context", "level", r.config.Level)
	}
	lsnCtx.RequiredLSN = masterLSN
	slog.Debug("UpdateLSNAfterWrite: updated LSN context with new required LSN", "requiredLSN", masterLSN)

	// Store updated context
	ctx = WithLSNContext(ctx, lsnCtx)

	return masterLSN, nil
}

// GetCurrentMasterLSN gets the current WAL LSN from the master database
func (r *CausalRouter) GetCurrentMasterLSN(ctx context.Context) (LSN, error) {
	if !r.config.Enabled {
		return LSN{}, fmt.Errorf("LSN tracking not enabled")
	}

	primaries := r.dbProvider.PrimaryDBs()
	if len(primaries) == 0 {
		return LSN{}, fmt.Errorf("no primary databases available")
	}

	// Use the first primary database
	primary := primaries[0]
	checker := getOrCreateChecker(primary, r.queryTimeout)

	lsn, err := checker.GetCurrentWALLSN(ctx)
	if err != nil {
		return LSN{}, fmt.Errorf("failed to get master LSN: %w", err)
	}

	// Update cached LSN
	r.mu.Lock()
	r.lastMasterLSN = lsn
	r.mu.Unlock()

	return lsn, nil
}

// GetLastKnownMasterLSN returns the last cached master LSN without querying the database
func (r *CausalRouter) GetLastKnownMasterLSN() LSN {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastMasterLSN
}
