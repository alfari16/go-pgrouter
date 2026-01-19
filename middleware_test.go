package dbresolver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPMiddleware(t *testing.T) {
	// Create a mock DB
	primary := MockDB()
	replica := MockDB()

	// Create DB with causal consistency
	config := &CausalConsistencyConfig{
		Enabled:          true,
		Level:            ReadYourWrites,
		FallbackToMaster: true,
	}

	db := New(
		WithPrimaryDBs(primary),
		WithReplicaDBs(replica),
		WithCausalConsistencyConfig(config),
	)

	// Create a simple router for testing
	router := NewSimpleRouter(db)

	// Create middleware
	middleware := NewHTTPMiddleware(router, "test_lsn", 0, false)

	// Create a test handler that simulates a write operation
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Wrap the handler with middleware
	wrappedHandler := middleware.Middleware(testHandler)

	// Create a test request
	req := httptest.NewRequest("GET", "/", http.NoBody)
	rec := httptest.NewRecorder()

	// Serve the request
	wrappedHandler.ServeHTTP(rec, req)

	// Check response
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	// Note: Since we're not actually doing a write operation in this test,
	// we don't expect a cookie to be set. The test verifies that the middleware
	// properly wraps the response writer.
}

func TestHTTPMiddlewareWithExistingLSNCookie(t *testing.T) {
	// Create a mock DB
	primary := MockDB()
	replica := MockDB()

	// Create DB with causal consistency
	config := &CausalConsistencyConfig{
		Enabled:          true,
		Level:            ReadYourWrites,
		FallbackToMaster: true,
	}

	db := New(
		WithPrimaryDBs(primary),
		WithReplicaDBs(replica),
		WithCausalConsistencyConfig(config),
	)

	// Create a simple router for testing
	router := NewSimpleRouter(db)

	// Create middleware
	middleware := NewHTTPMiddleware(router, "test_lsn", 0, false)

	// Create a test handler
	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if LSN context is present and has the correct LSN
		ctx := r.Context()
		lsnCtx := GetLSNContext(ctx)
		if lsnCtx == nil {
			t.Error("LSN context should be present")
		} else if lsnCtx.RequiredLSN.IsZero() {
			t.Error("Expected LSN in context to be set from cookie")
		} else {
			lsnStr := lsnCtx.RequiredLSN.String()
			if lsnStr != "1/ABCDEF" {
				t.Errorf("Expected LSN '1/ABCDEF' in context, got '%s'", lsnStr)
			}
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Wrap the handler with middleware
	wrappedHandler := middleware.Middleware(testHandler)

	// Create a test request with existing LSN cookie
	req := httptest.NewRequest("GET", "/", http.NoBody)
	req.AddCookie(&http.Cookie{
		Name:  "test_lsn",
		Value: "1/ABCDEF",
	})
	rec := httptest.NewRecorder()

	// Serve the request
	wrappedHandler.ServeHTTP(rec, req)

	// Check response
	resp := rec.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}
