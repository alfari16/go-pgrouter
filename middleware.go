package dbresolver

import (
	"net/http"
	"time"
)

// HTTPMiddleware provides HTTP middleware for LSN-aware database routing
// Optimized version: Simplified middleware without response wrapping
type HTTPMiddleware struct {
	router       *CausalRouter
	cookieName   string
	cookieMaxAge time.Duration
}

// NewHTTPMiddleware creates new HTTP middleware for LSN tracking
func NewHTTPMiddleware(router *CausalRouter, cookieName string, maxAge time.Duration) *HTTPMiddleware {
	if cookieName == "" {
		cookieName = "pg_min_lsn"
	}
	if maxAge <= 0 {
		maxAge = 5 * time.Minute
	}

	return &HTTPMiddleware{
		router:       router,
		cookieName:   cookieName,
		cookieMaxAge: maxAge,
	}
}

// Middleware returns an HTTP middleware function
// Optimized version: Simple cookie management without response wrapping
func (m *HTTPMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract LSN from cookie if present
		requiredLSN, hasLSN := GetLSNFromCookie(r, m.cookieName)

		// Create LSN context only if cookie exists
		if hasLSN {
			lsnCtx := &LSNContext{
				RequiredLSN: requiredLSN,
				Level:       m.router.config.Level,
			}
			ctx = WithLSNContext(ctx, lsnCtx)
		}

		// Call next handler with updated context
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// SetLSNCookie is a helper function to set LSN cookie after write operations
// Call this explicitly after write operations instead of relying on response wrapping
func SetLSNCookie(w http.ResponseWriter, lsn LSN, cookieName string, maxAge time.Duration) {
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
		MaxAge:   int(maxAge.Seconds()), // threshold on avg time your database sync took. the lesser the better since it dont need to query the LSN
		HttpOnly: true,
		Secure:   false, // Set to true in production with HTTPS
		Path:     "/",
		SameSite: http.SameSiteLaxMode,
	})
}
