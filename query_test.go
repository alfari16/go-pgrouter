package dbresolver

import (
	"strings"
	"testing"
)

func TestDefaultQueryTypeChecker(t *testing.T) {
	checker := NewDefaultQueryTypeChecker()

	tests := []struct {
		name     string
		query    string
		expected QueryType
	}{
		// Write queries - should return QueryTypeWrite
		{
			name:     "INSERT INTO",
			query:    "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
			expected: QueryTypeWrite,
		},
		{
			name:     "INSERT lowercase",
			query:    "insert into users (name) values ('Jane')",
			expected: QueryTypeWrite,
		},
		{
			name:     "INSERT with leading whitespace",
			query:    "  \t  INSERT INTO table VALUES (1)",
			expected: QueryTypeWrite,
		},
		{
			name:     "UPDATE statement",
			query:    "UPDATE users SET name = 'John' WHERE id = 1",
			expected: QueryTypeWrite,
		},
		{
			name:     "DELETE statement",
			query:    "DELETE FROM users WHERE id = 1",
			expected: QueryTypeWrite,
		},
		{
			name:     "MERGE statement",
			query:    "MERGE INTO target USING source ON target.id = source.id",
			expected: QueryTypeWrite,
		},
		{
			name:     "TRUNCATE statement",
			query:    "TRUNCATE TABLE users",
			expected: QueryTypeWrite,
		},
		{
			name:     "REPLACE statement (MySQL)",
			query:    "REPLACE INTO users VALUES (1, 'John')",
			expected: QueryTypeWrite,
		},
		{
			name:     "SELECT with RETURNING clause",
			query:    "SELECT * FROM users WHERE id IN (SELECT user_id FROM logs RETURNING user_id)",
			expected: QueryTypeWrite,
		},
		{
			name:     "INSERT OR REPLACE (SQLite)",
			query:    "INSERT OR REPLACE INTO users VALUES (1, 'John')",
			expected: QueryTypeWrite,
		},
		{
			name:     "INSERT IGNORE (MySQL)",
			query:    "INSERT IGNORE INTO users VALUES (1, 'John')",
			expected: QueryTypeWrite,
		},
		{
			name:     "UPDATE lowercase with whitespace",
			query:    "  \n update  \t table set col = 'value'",
			expected: QueryTypeWrite,
		},

		// Read queries - should return QueryTypeUnknown (not write operations)
		{
			name:     "Simple SELECT",
			query:    "SELECT * FROM users",
			expected: QueryTypeUnknown,
		},
		{
			name:     "SELECT with JOIN",
			query:    "SELECT u.*, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			expected: QueryTypeUnknown,
		},
		{
			name:     "SELECT with subquery",
			query:    "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
			expected: QueryTypeUnknown,
		},
		{
			name:     "WITH clause (CTE)",
			query:    "WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users",
			expected: QueryTypeUnknown,
		},
		{
			name:     "SHOW statement",
			query:    "SHOW TABLES",
			expected: QueryTypeUnknown,
		},
		{
			name:     "DESCRIBE statement",
			query:    "DESCRIBE users",
			expected: QueryTypeUnknown,
		},
		{
			name:     "EXPLAIN statement",
			query:    "EXPLAIN SELECT * FROM users",
			expected: QueryTypeUnknown,
		},
		// Edge cases
		{
			name:     "Empty query",
			query:    "",
			expected: QueryTypeUnknown,
		},
		{
			name:     "Only whitespace",
			query:    "   \t\n  ",
			expected: QueryTypeUnknown,
		},
		{
			name:     "Comment only",
			query:    "-- This is a comment",
			expected: QueryTypeUnknown,
		},
		{
			name:     "String containing INSERT keyword but not as command",
			query:    "SELECT 'INSERT INTO users' as sql_query FROM queries",
			expected: QueryTypeUnknown,
		},
		{
			name:     "Complex query with INSERT in string literal",
			query:    "SELECT * FROM queries WHERE sql LIKE '%INSERT%UPDATE%'",
			expected: QueryTypeUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checker.Check(tt.query)
			if result != tt.expected {
				t.Errorf("DefaultQueryTypeChecker.Check() = %v, want %v for query: %s", result, tt.expected, tt.query)
			}
		})
	}
}

// Benchmark the regex-based implementation
func BenchmarkDefaultQueryTypeChecker(b *testing.B) {
	checker := NewDefaultQueryTypeChecker()
	queries := []string{
		"SELECT * FROM users WHERE id = ?",
		"INSERT INTO users (name) VALUES (?)",
		"UPDATE users SET name = ? WHERE id = ?",
		"DELETE FROM users WHERE id = ?",
		"MERGE INTO target USING source ON target.id = source.id",
		"TRUNCATE TABLE users",
		"WITH cte AS (SELECT * FROM table) SELECT * FROM cte",
		"SELECT * FROM orders WHERE user_id IN (SELECT id FROM users RETURNING id)",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, query := range queries {
			checker.Check(query)
		}
	}
}

// Compare with old string-based implementation
func TestOldVsNewImplementation(t *testing.T) {
	// Old implementation (for comparison)
	oldCheck := func(query string) QueryType {
		if strings.Contains(strings.ToUpper(query), "RETURNING") {
			return QueryTypeWrite
		}
		return QueryTypeUnknown
	}

	newChecker := NewDefaultQueryTypeChecker()

	tests := []struct {
		query               string
		expectedOld         QueryType
		expectedNew         QueryType
		shouldDetectAsWrite bool
	}{
		{
			query:               "INSERT INTO users VALUES (1)",
			expectedOld:         QueryTypeUnknown,
			expectedNew:         QueryTypeWrite,
			shouldDetectAsWrite: true,
		},
		{
			query:               "UPDATE users SET name = 'John'",
			expectedOld:         QueryTypeUnknown,
			expectedNew:         QueryTypeWrite,
			shouldDetectAsWrite: true,
		},
		{
			query:               "SELECT * FROM users RETURNING id",
			expectedOld:         QueryTypeWrite,
			expectedNew:         QueryTypeWrite,
			shouldDetectAsWrite: true,
		},
		{
			query:               "SELECT * FROM users",
			expectedOld:         QueryTypeUnknown,
			expectedNew:         QueryTypeUnknown,
			shouldDetectAsWrite: false,
		},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			oldResult := oldCheck(tt.query)
			newResult := newChecker.Check(tt.query)

			if oldResult != tt.expectedOld {
				t.Errorf("Old implementation result = %v, want %v", oldResult, tt.expectedOld)
			}
			if newResult != tt.expectedNew {
				t.Errorf("New implementation result = %v, want %v", newResult, tt.expectedNew)
			}

			// Verify new implementation correctly identifies write operations
			if tt.shouldDetectAsWrite && newResult != QueryTypeWrite {
				t.Errorf("New implementation should detect as write query but didn't: %s", tt.query)
			}
		})
	}
}