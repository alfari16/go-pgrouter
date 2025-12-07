package dbresolver

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"
)

// RandomRouter implements QueryRouter with random database selection
// This demonstrates how the QueryRouter interface enables the Open-Closed Principle:
// We can add new routing strategies without modifying existing code.
type RandomRouter struct {
	dbProvider DBProvider
	rand       *rand.Rand
}

// NewRandomRouter creates a new router that randomly selects databases
func NewRandomRouter(dbProvider DBProvider) *RandomRouter {
	return &RandomRouter{
		dbProvider: dbProvider,
		rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// RouteQuery routes queries to randomly selected databases
func (r *RandomRouter) RouteQuery(ctx context.Context, queryType QueryType) (*sql.DB, error) {
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
		// For writes, randomly select from primaries
		selected := primaries[r.rand.Intn(len(primaries))]
		return selected, nil

	case QueryTypeRead:
		// For reads, randomly select from all available databases
		allDBs := append(primaries, replicas...)
		selected := allDBs[r.rand.Intn(len(allDBs))]
		return selected, nil

	default:
		// Default to primary for unknown query types
		selected := primaries[r.rand.Intn(len(primaries))]
		return selected, nil
	}
}

// UpdateLSNAfterWrite is a no-op for RandomRouter since it doesn't track LSN
func (r *RandomRouter) UpdateLSNAfterWrite(ctx context.Context, db *sql.DB) (LSN, error) {
	// Random router doesn't track LSN, return zero LSN
	return LSN{}, nil
}

// RoundRobinRouter implements QueryRouter with round-robin database selection
type RoundRobinRouter struct {
	dbProvider     DBProvider
	primariesIndex int
	replicasIndex  int
}

// NewRoundRobinRouter creates a new router that uses round-robin selection
func NewRoundRobinRouter(dbProvider DBProvider) *RoundRobinRouter {
	return &RoundRobinRouter{
		dbProvider:     dbProvider,
		primariesIndex: 0,
		replicasIndex:  0,
	}
}

// RouteQuery routes queries using round-robin selection
func (r *RoundRobinRouter) RouteQuery(ctx context.Context, queryType QueryType) (*sql.DB, error) {
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
		// For writes, use round-robin on primaries
		selected := primaries[r.primariesIndex%len(primaries)]
		r.primariesIndex++
		return selected, nil

	case QueryTypeRead:
		// For reads, use round-robin on replicas if available, otherwise primaries
		if len(replicas) > 0 {
			selected := replicas[r.replicasIndex%len(replicas)]
			r.replicasIndex++
			return selected, nil
		}
		// Fallback to primaries if no replicas
		selected := primaries[r.primariesIndex%len(primaries)]
		r.primariesIndex++
		return selected, nil

	default:
		// Default to primary for unknown query types
		selected := primaries[r.primariesIndex%len(primaries)]
		r.primariesIndex++
		return selected, nil
	}
}

// UpdateLSNAfterWrite is a no-op for RoundRobinRouter since it doesn't track LSN
func (r *RoundRobinRouter) UpdateLSNAfterWrite(ctx context.Context, db *sql.DB) (LSN, error) {
	// Round-robin router doesn't track LSN, return zero LSN
	return LSN{}, nil
}

// This example demonstrates how the QueryRouter interface enables the Open-Closed Principle:
//
// 1. The system is open for extension: We can easily add new routing strategies
//    by implementing the QueryRouter interface (RandomRouter, RoundRobinRouter, etc.)
//
// 2. The system is closed for modification: We don't need to modify existing code
//    like DB, tx, or CausalRouter to add new routing behavior
//
// Usage example:
//
//     // Using the default LSN-aware router
//     db := dbresolver.New(
//         dbresolver.WithPrimaryDBs(primaryDB),
//         dbresolver.WithReplicaDBs(replicaDB1, replicaDB2),
//         dbresolver.WithCausalConsistency(&dbresolver.CausalConsistencyConfig{
//             Enabled: true,
//             Level: dbresolver.ReadYourWrites,
//         }),
//     )
//
//     // Using a simple router without LSN tracking
//     simpleDB := dbresolver.New(
//         dbresolver.WithPrimaryDBs(primaryDB),
//         dbresolver.WithReplicaDBs(replicaDB1, replicaDB2),
//         // You could extend the New function to accept custom routers
//         // dbresolver.WithQueryRouter(dbresolver.NewSimpleRouter(db)),
//     )
//
//     // Using a random router (would need extension to Options)
//     randomDB := dbresolver.New(
//         dbresolver.WithPrimaryDBs(primaryDB1, primaryDB2),
//         dbresolver.WithReplicaDBs(replicaDB1, replicaDB2),
//         // dbresolver.WithQueryRouter(dbresolver.NewRandomRouter(db)),
//     )
