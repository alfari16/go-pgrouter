# PostgreSQL LSN-Based Causal Consistency Implementation

This document describes the comprehensive implementation of PostgreSQL LSN-based causal consistency enhancement for the dbresolver library.

## Overview

The implementation provides **Read-Your-Writes (RYW)** consistency using PostgreSQL Log Sequence Numbers (LSNs). It ensures that when a user performs a write operation and immediately reads, they will always see their own writes, even under replication lag.

## Architecture

### Core Components

#### 1. LSN Parsing and Utilities (`lsn.go`)
- **LSN struct**: Represents PostgreSQL LSN in format X/Y
- **ParseLSN()**: Parses LSN strings to numeric representation
- **Comparison operations**: LessThan, GreaterThan, Equals, etc.
- **Arithmetic operations**: Subtract, Add for lag calculations
- **Conversions**: To/from uint64 for efficient storage

#### 2. PostgreSQL LSN Checker (`pg_lsn_checker.go`)
- **PGLSNChecker**: Handles PostgreSQL-specific LSN queries
- **Throttled queries**: Prevents excessive database load
- **Caching**: Optional result caching for master LSN queries
- **Error handling**: Graceful fallback on connection failures

#### 3. LSN Tracker (`lsn_tracker.go`)
- **LSNTracker**: Coordinates LSN tracking across all replicas
- **Replica health monitoring**: Background health checks
- **Replica status tracking**: LSN positions, lag calculations
- **Healthy replica selection**: Filters replicas based on lag tolerance

#### 4. Causal Consistency Router (`causal_consistency.go`)
- **CausalRouter**: Intelligent query routing based on LSN requirements
- **Context management**: LSN requirements in HTTP context
- **HTTP middleware**: Automatic cookie-based LSN tracking
- **Multiple consistency levels**: None, Read-Your-Writes, Strong

#### 5. Enhanced Database Interface (`db.go`)
- **Extended DB interface**: New LSN-aware methods
- **LSN-aware query routing**: Modified QueryContext/QueryRowContext
- **Write operation tracking**: Automatic LSN updates after writes
- **Backward compatibility**: All existing features preserved

## Key Features

### 1. Client-Carried State Pattern
- **HTTP Cookies**: Store LSN of last write (`pg_min_lsn`)
- **Automatic Management**: Middleware handles cookie setting/reading
- **Configurable**: Cookie name, max age, security settings

### 2. Intelligent Query Routing
- **Write Operations**: Always use primary master
- **Read Operations**:
  - Check LSN requirements from context/cookie
  - If replica LSN >= required LSN → Use replica
  - If replica LSN < required LSN → Use primary (fallback)
  - If no LSN requirement → Use any healthy replica

### 3. Performance Optimization
- **Throttled LSN Queries**: Prevents database overload
- **Result Caching**: Master LSN cached between queries
- **Background Monitoring**: Optional async health checks
- **Connection Reuse**: Efficient database connection management

### 4. Graceful Degradation
- **Connection Failures**: Fallback to master when replicas fail
- **LSN Query Errors**: Continue operation without LSN tracking
- **Invalid LSNs**: Ignore malformed cookies
- **Replica Lag**: Automatic master fallback when lagging

## Configuration Options

### LSN Tracker Configuration
```go
lsnConfig := &LSNTrackerConfig{
    CheckInterval:    5 * time.Second,
    QueryTimeout:     3 * time.Second,
    ThrottleTime:     100 * time.Millisecond,
    EnableMonitoring: true, // Background monitoring
}
```

### Causal Consistency Configuration
```go
ccConfig := &CausalConsistencyConfig{
    Enabled:          true,
    Level:            ReadYourWrites,
    FallbackToMaster: true,
    Timeout:          5 * time.Second,
}
```

### Functional Options
```go
db := dbresolver.New(
    dbresolver.WithPrimaryDBs(primaryDB),
    dbresolver.WithReplicaDBs(replicaDB),
    dbresolver.WithCausalConsistency(ccConfig),
    dbresolver.WithMaxReplicationLag(512*1024),
    dbresolver.WithLSNQueryTimeout(3*time.Second),
    dbresolver.WithLSNThrottleTime(100*time.Millisecond),
    EnableLSNMonitoring(),
)
```

## API Methods

### New DB Interface Methods
```go
// LSN tracking methods
GetCurrentMasterLSN(ctx context.Context) (*LSN, error)
GetLastKnownMasterLSN() *LSN
GetReplicaStatus() []*ReplicaStatus
UpdateLSNAfterWrite(ctx context.Context) (*LSN, error)
IsCausalConsistencyEnabled() bool
```

### Context Management
```go
// LSN context for read-your-writes
WithLSNContext(ctx context.Context, lsnCtx *LSNContext) context.Context
GetLSNContext(ctx context.Context) *LSNContext

// DB connection context
WithDBConnection(ctx context.Context, db *sql.DB) context.Context
GetDBConnection(ctx context.Context) *sql.DB
```

## HTTP Middleware Usage

```go
// Create middleware
middleware := NewHTTPMiddleware(causalRouter, "pg_min_lsn", 5*time.Minute)

// Apply to handlers
handler := middleware(http.HandlerFunc(yourHandler))

// Automatic LSN cookie management:
// - Write operations set cookies with current master LSN
// - Read operations use cookies to enforce consistency
```

## Usage Examples

### Basic Setup
```go
// Create resolver with LSN features
db := dbresolver.New(
    dbresolver.WithPrimaryDBs(primaryDB),
    dbresolver.WithReplicaDBs(replicaDB),
    dbresolver.WithCausalConsistency(&dbresolver.CausalConsistencyConfig{
        Enabled: true,
        Level: dbresolver.ReadYourWrites,
    }),
)

// Use normally - LSN routing is automatic
var user User
err := db.QueryRowContext(ctx, "SELECT * FROM users WHERE id = $1", userID).
    Scan(&user.ID, &user.Name, &user.Email)
```

### Manual LSN Handling
```go
// After write operation
result, err := db.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", name)
if err == nil {
    // Update LSN tracking
    lsn, err := db.UpdateLSNAfterWrite(ctx)
    // Store LSN for future reads
}

// Read with LSN requirements
lsnCtx := &dbresolver.LSNContext{
    RequiredLSN: lastWriteLSN,
    Level: dbresolver.ReadYourWrites,
}
ctx = dbresolver.WithLSNContext(ctx, lsnCtx)

err := db.QueryRowContext(ctx, "SELECT * FROM users WHERE id = $1", userID).Scan(&user)
```

## Monitoring and Observability

### Replica Status
```go
statuses := db.GetReplicaStatus()
for _, status := range statuses {
    fmt.Printf("Replica healthy: %t, lag: %d bytes\n",
        status.IsHealthy, status.LagBytes)
}
```

### LSN Information
```go
// Current master LSN
masterLSN, err := db.GetCurrentMasterLSN(ctx)

// Last cached master LSN
lastKnown := db.GetLastKnownMasterLSN()

// Check if causal consistency is enabled
if db.IsCausalConsistencyEnabled() {
    fmt.Println("LSN-based routing is active")
}
```

## Performance Characteristics

### Query Overhead
- **LSN Queries**: Throttled to prevent database overload
- **Context Creation**: Minimal overhead for LSN context
- **Replica Selection**: Fast O(n) check for healthy replicas
- **Caching**: Reduces actual database queries

### Memory Usage
- **LSN Struct**: 8 bytes per LSN instance
- **Replica Status**: Minimal per-replica metadata
- **Context Storage**: Small context values
- **Background Monitoring**: Optional, configurable resource usage

## Testing

### Unit Tests
- LSN parsing and comparison operations
- Context management functionality
- Configuration validation
- Error handling scenarios

### Integration Tests
- End-to-end query routing
- LSN cookie lifecycle
- Replica lag handling
- Failure scenarios

### Benchmarks
- LSN parsing performance
- Comparison operations
- Context overhead
- Query routing efficiency

## Backward Compatibility

The implementation maintains **100% backward compatibility**:

- **Existing API**: All original dbresolver methods unchanged
- **Optional Features**: LSN features are opt-in only
- **Default Behavior**: Without LSN config, works exactly as before
- **Migration Path**: Easy upgrade from existing implementations

## Production Considerations

### Configuration Tuning
- **ThrottleTime**: Balance between consistency and performance
- **QueryTimeout**: Set based on your database response times
- **Cookie Security**: Enable Secure/HttpOnly in production

### Monitoring
- **Replica Health**: Monitor replica lag metrics
- **LSN Query Performance**: Watch for slow LSN queries
- **Error Rates**: Track fallback to master usage
- **Connection Usage**: Monitor database connection pools

### High Availability
- **Multiple Replicas**: Configure multiple read replicas
- **Failover Handling**: Automatic fallback on replica failures
- **Connection Resilience**: Retry logic for LSN queries
- **Graceful Degradation**: Continue operation during partial failures

## Conclusion

This implementation provides a robust, production-ready solution for PostgreSQL read-your-writes consistency using LSN tracking. It addresses the common problem of replication lag in read/write splitting architectures while maintaining high performance and operational simplicity.

The modular design allows for easy customization and extension, while maintaining backward compatibility ensures smooth adoption in existing applications.