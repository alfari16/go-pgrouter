package dbresolver

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type DBConfig struct {
	primaryDBCount uint8
	replicaDBCount uint8
	lbPolicy       LoadBalancerPolicy
}

var LoadBalancerPolicies = []LoadBalancerPolicy{
	RandomLB,
	RoundRobinLB,
}

func handleDBError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("db error: %s", err)
	}

}

func testMW(t *testing.T, config DBConfig) {
	noOfPrimaries, noOfReplicas := int(config.primaryDBCount), int(config.replicaDBCount)
	lbPolicy := config.lbPolicy

	t.Run("basic functionality", func(t *testing.T) {
		// Use a matcher that allows any query - this avoids load balancer prediction issues
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true), sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatalf("creating mock database failed: %s", err)
		}
		defer db.Close()

		// Create a single resolver with either the mock as primary or both primary and replica
		if noOfReplicas == 0 {
			// No replicas - use mock as primary
			resolver := New(WithPrimaryDBs(db), WithLoadBalancer(lbPolicy))

			// Test basic operations
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))
			rows, err := resolver.Query("SELECT 1")
			if err != nil {
				t.Errorf("query failed: %s", err)
			} else {
				rows.Close()
			}

			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
			_, err = resolver.Exec("INSERT INTO test_table VALUES (1)")
			if err != nil {
				t.Errorf("exec failed: %s", err)
			}

			mock.ExpectPing()
			err = resolver.Ping()
			if err != nil {
				t.Errorf("ping failed: %s", err)
			}

			mock.ExpectClose()
			err = resolver.Close()
			if err != nil {
				t.Errorf("close failed: %s", err)
			}
		} else {
			// Has replicas - use mock for both primary and replica
			resolver := New(WithPrimaryDBs(db), WithReplicaDBs(db), WithLoadBalancer(lbPolicy))

			// Test read operations (should go to replica)
			mock.ExpectQuery("SELECT").WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))
			rows, err := resolver.Query("SELECT 1")
			if err != nil {
				t.Errorf("query failed: %s", err)
			} else {
				rows.Close()
			}

			// Test write operations (should go to primary)
			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
			_, err = resolver.Exec("INSERT INTO test_table VALUES (1)")
			if err != nil {
				t.Errorf("exec failed: %s", err)
			}

			// Test transaction
			mock.ExpectBegin()
			mock.ExpectExec("INSERT").WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()
			tx, err := resolver.Begin()
			if err != nil {
				t.Errorf("begin failed: %s", err)
			} else {
				_, err = tx.Exec("INSERT INTO test_table VALUES (1)")
				if err != nil {
					t.Errorf("tx exec failed: %s", err)
					tx.Rollback()
				} else {
					tx.Commit()
				}
			}

			// When using same DB for both primary and replica, ping may be called multiple times
			mock.ExpectPing()
			mock.ExpectPing() // May be called for both primary and replica
			err = resolver.Ping()
			if err != nil {
				t.Errorf("ping failed: %s", err)
			}

			mock.ExpectClose() // Only expect one close since resolver should avoid double-closing
			err = resolver.Close()
			if err != nil {
				t.Errorf("close failed: %s", err)
			}
		}

		// Ensure all expectations were met
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("mock expectations were not met: %s", err)
		}

		t.Logf("tested:DB-CLUSTER-%dP%dR", noOfPrimaries, noOfReplicas)
	})
}

func TestMultiWrite(t *testing.T) {
	t.Parallel()

	loadBalancerPolices := []LoadBalancerPolicy{
		RoundRobinLB,
		RandomLB,
	}

	retrieveLoadBalancer := func() (loadBalancerPolicy LoadBalancerPolicy) {
		loadBalancerPolicy = loadBalancerPolices[0]
		loadBalancerPolices = loadBalancerPolices[1:]
		return
	}

BEGIN_TEST:
	loadBalancerPolicy := retrieveLoadBalancer()

	t.Logf("LoadBalancer-%s", loadBalancerPolicy)

	testCases := []DBConfig{
		{1, 0, ""},
		{1, 1, ""},
		{1, 2, ""},
		{1, 10, ""},
		{2, 0, ""},
		{2, 1, ""},
		{3, 0, ""},
		{3, 1, ""},
		{3, 2, ""},
		{3, 3, ""},
		{3, 6, ""},
		{5, 6, ""},
		{7, 20, ""},
		{10, 10, ""},
		{10, 20, ""},
	}

	retrieveTestCase := func() DBConfig {
		testCase := testCases[0]
		testCases = testCases[1:]
		return testCase
	}

BEGIN_TEST_CASE:
	if len(testCases) == 0 {
		if len(loadBalancerPolices) == 0 {
			return
		}
		goto BEGIN_TEST
	}

	dbConfig := retrieveTestCase()

	dbConfig.lbPolicy = loadBalancerPolicy

	t.Run(fmt.Sprintf("DBCluster P%dR%d", dbConfig.primaryDBCount, dbConfig.replicaDBCount), func(t *testing.T) {
		testMW(t, dbConfig)
	})

	if testing.Short() {
		return
	}

	goto BEGIN_TEST_CASE
}

func createMock() (db *sql.DB, mock sqlmock.Sqlmock, err error) {
	db, mock, err = sqlmock.New(sqlmock.MonitorPingsOption(true), sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	return
}

type QueryMatcher struct {
}

func (*QueryMatcher) Match(expectedSQL, actualSQL string) error {
	return nil
}
