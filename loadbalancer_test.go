package dbresolver

import (
	"database/sql"
	"testing"
	"testing/quick"
)

func TestReplicaRoundRobin(t *testing.T) {
	db := &RoundRobinLoadBalancer[*sql.DB]{}

	err := quick.Check(func(n int) bool {
		if n <= 0 {
			return true // Skip invalid cases
		}

		index := db.predict(n)
		if n <= 1 {
			return index == 0
		}

		// For round robin with n > 1, index should be valid and alternate
		return index >= 0 && index < n
	}, nil)

	if err != nil {
		t.Error(err)
	}
}
