package dbresolver

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// lsnResponseWriter wraps http.ResponseWriter to intercept WriteHeader calls
// and automatically set LSN cookies for successful write operations
type lsnResponseWriter struct {
	http.ResponseWriter
	middleware  *HTTPMiddleware
	ctx         context.Context
	wroteHeader bool
	statusCode  int
}

// WriteHeader intercepts the WriteHeader call to set LSN cookies when appropriate
func (lrw *lsnResponseWriter) WriteHeader(statusCode int) {
	if !lrw.wroteHeader {
		lrw.statusCode = statusCode
		lrw.wroteHeader = true

		// Check for 2xx status code and write operation
		if statusCode >= 200 && statusCode < 300 {
			if lsnCtx := GetLSNContext(lrw.ctx); lsnCtx != nil && lsnCtx.HasWriteOperation {
				// Get LSN from router and set cookie
				if lsn, err := lrw.middleware.router.UpdateLSNAfterWrite(lrw.ctx); err == nil && !lsn.IsZero() {
					SetLSNCookie(lrw.ResponseWriter, lsn, lrw.middleware.cookieName, lrw.middleware.cookieMaxAge, lrw.middleware.cookieSecure)
				}
			}
		}

		lrw.ResponseWriter.WriteHeader(statusCode)
	}
}

func (lrw *lsnResponseWriter) reset(ctx context.Context, w http.ResponseWriter) {
	lrw.ResponseWriter = w
	lrw.ctx = ctx
	lrw.wroteHeader = false
	lrw.statusCode = 0
}

// HTTPMiddleware provides HTTP middleware for LSN-aware database routing
// Optimized version with automatic cookie setting via response wrapper
type HTTPMiddleware struct {
	router       QueryRouter
	cookieName   string
	cookieMaxAge time.Duration
	cookieSecure bool
	wrapperPool  *sync.Pool
}

// NewHTTPMiddleware creates new HTTP middleware for LSN tracking
// maxAge determine your threshold of avg time sync between master and replica
func NewHTTPMiddleware(router QueryRouter, cookieName string, maxAge time.Duration, useSecureCookie bool) *HTTPMiddleware {
	if cookieName == "" {
		cookieName = "pg_min_lsn"
	}
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}

	m := &HTTPMiddleware{
		router:       router,
		cookieName:   cookieName,
		cookieMaxAge: maxAge,
		cookieSecure: useSecureCookie,
	}

	// Initialize wrapper pool for reuse
	m.wrapperPool = &sync.Pool{
		New: func() interface{} {
			return &lsnResponseWriter{
				middleware: m,
			}
		},
	}

	return m
}

// Middleware returns an HTTP middleware function
// Enhanced version with automatic cookie setting via response wrapper
func (m *HTTPMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract LSN from cookie if present
		requiredLSN, hasLSN := GetLSNFromCookie(r, m.cookieName)

		// Create LSN context only if cookie exists
		lsnCtx := &LSNContext{}
		if hasLSN {
			lsnCtx.RequiredLSN = requiredLSN
		}
		ctx = WithLSNContext(ctx, lsnCtx)

		// Get response writer from pool and set up for reuse
		rw := m.wrapperPool.Get().(*lsnResponseWriter)
		defer m.wrapperPool.Put(rw)

		rw.reset(ctx, w)

		// Call next handler with wrapped response writer
		next.ServeHTTP(rw, r.WithContext(ctx))
	})
}

// SetLSNCookie is a helper function to set LSN cookie after write operations
// Call this explicitly after write operations instead of relying on response wrapping
func SetLSNCookie(w http.ResponseWriter, lsn LSN, cookieName string, maxAge time.Duration, secure bool) {
	if lsn.IsZero() {
		return
	}
	if cookieName == "" {
		cookieName = "pg_min_lsn"
	}
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}

	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    lsn.String(),
		MaxAge:   int(maxAge.Seconds()), // threshold on avg time your database sync took.
		HttpOnly: true,
		Secure:   secure, // Set to true in production with HTTPS
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
	})
}
