package dbresolver

import (
	"regexp"
)

type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeRead
	QueryTypeWrite
)

// QueryTypeChecker is used to try to detect the query type, like for detecting RETURNING clauses in
// INSERT/UPDATE clauses.
type QueryTypeChecker interface {
	Check(query string) QueryType
}

// DefaultQueryTypeChecker uses regex patterns to detect write queries by identifying SQL DML statements.
type DefaultQueryTypeChecker struct {
	// writeRegex matches common SQL write operations at the beginning of the query
	// or when they contain a RETURNING clause anywhere in the query
	writeRegex *regexp.Regexp
}

// NewDefaultQueryTypeChecker creates a new DefaultQueryTypeChecker with compiled regex
func NewDefaultQueryTypeChecker() *DefaultQueryTypeChecker {
	// This regex matches:
	// 1. INSERT statements (including INSERT INTO, INSERT OR REPLACE, etc.)
	// 2. UPDATE statements
	// 3. DELETE statements
	// 4. MERGE statements
	// 5. TRUNCATE statements
	// 6. REPLACE statements (MySQL)
	// 7. Any query containing RETURNING clause
	// Uses case-insensitive matching and allows for optional whitespace
	writePattern := `(?i)^\s*(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|REPLACE)\b|\bRETURNING\b`

	return &DefaultQueryTypeChecker{
		writeRegex: regexp.MustCompile(writePattern),
	}
}

func (c *DefaultQueryTypeChecker) Check(query string) QueryType {
	// Use the compiled regex to detect write operations
	if c.writeRegex.MatchString(query) {
		return QueryTypeWrite
	}
	return QueryTypeUnknown
}
