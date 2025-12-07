package dbresolver

import (
	"context"
	"database/sql"
	"strings"
)

// Tx is a *sql.Tx wrapper.
// Its main purpose is to be able to return the internal Stmt interface.
type Tx interface {
	Commit() error
	Rollback() error
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (Stmt, error)
	PrepareContext(ctx context.Context, query string) (Stmt, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	Stmt(stmt Stmt) Stmt
	StmtContext(ctx context.Context, stmt Stmt) Stmt
}

type tx struct {
	sourceDB         *sql.DB
	tx               *sql.Tx
	queryRouter      QueryRouter
	queryTypeChecker QueryTypeChecker
	writesOccurred   bool
}

// trackLSNAfterWrite handles LSN tracking after successful write operations
// Single responsibility function with built-in throttling (100ms default)
// This function should be called after every successful write operation
func (t *tx) trackLSNAfterWrite(ctx context.Context, err error) {
	// Only proceed if operation was successful and query router is available
	if err == nil && t.queryRouter != nil && t.sourceDB != nil {
		// The underlying implementation handles throttling if it supports LSN tracking
		if _, lsnErr := t.queryRouter.UpdateLSNAfterWrite(ctx, t.sourceDB); lsnErr != nil {
			// Log error but don't fail the operation (best-effort tracking)
			// In production, you might want to log this error
		}
	}
}

// markWriteOperation marks that a write operation has occurred during the transaction
func (t *tx) markWriteOperation(ctx context.Context, err error) {
	if err == nil {
		t.writesOccurred = true
	}
}

func (t *tx) Commit() error {
	err := t.tx.Commit()

	// Track LSN after successful commit if writes occurred during transaction
	if err == nil && t.writesOccurred {
		t.trackLSNAfterWrite(context.Background(), err)
	}

	return err
}

func (t *tx) Rollback() error {
	return t.tx.Rollback()
}

func (t *tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return t.ExecContext(context.Background(), query, args...)
}

func (t *tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)

	// Mark write operation if it was successful
	t.markWriteOperation(ctx, err)

	return result, err
}

func (t *tx) Prepare(query string) (Stmt, error) {
	return t.PrepareContext(context.Background(), query)
}

func (t *tx) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	txstmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return newSingleDBStmt(t.sourceDB, txstmt, true), nil
}

func (t *tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.QueryContext(context.Background(), query, args...)
}

func (t *tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var writeFlag bool
	if t.queryTypeChecker != nil {
		writeFlag = t.queryTypeChecker.Check(query) == QueryTypeWrite
	} else {
		// Fallback: check for RETURNING clause if no query type checker available
		_query := strings.ToUpper(query)
		writeFlag = strings.Contains(_query, "RETURNING")
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)

	// Mark write operation if successful and it was a write query (e.g., with RETURNING)
	if writeFlag {
		t.markWriteOperation(ctx, err)
	}

	return rows, err
}

func (t *tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return t.QueryRowContext(context.Background(), query, args...)
}

func (t *tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var writeFlag bool
	if t.queryTypeChecker != nil {
		writeFlag = t.queryTypeChecker.Check(query) == QueryTypeWrite
	} else {
		// Fallback: check for RETURNING clause if no query type checker available
		_query := strings.ToUpper(query)
		writeFlag = strings.Contains(_query, "RETURNING")
	}

	row := t.tx.QueryRowContext(ctx, query, args...)

	// Mark write operation if successful and it was a write query (e.g., with RETURNING)
	if writeFlag {
		t.markWriteOperation(ctx, row.Err())
	}

	return row
}

func (t *tx) Stmt(s Stmt) Stmt {
	return t.StmtContext(context.Background(), s)
}

func (t *tx) StmtContext(ctx context.Context, s Stmt) Stmt {
	if rstmt, ok := s.(*stmt); ok {
		return newSingleDBStmt(t.sourceDB, t.tx.StmtContext(ctx, rstmt.stmtForDB(t.sourceDB)), true)
	}
	return s
}
