package web

// Mutations authenticate with an ambient cookie, so they need a header a
// cross-site page cannot set, plus a same-origin check. The page itself is
// served from this origin, which is what makes the CSP below free.

import (
	"net/http"
	"net/url"
	"strings"
)

// csrfHeader is required on every state-changing /api request; the SPA sends it
// from api() and from the login form.
const csrfHeader = "X-PocketKafka-Request"

// securityHeaders applies the dashboard's response headers. The policy allows
// inline script and style because the SPA ships as a single self-contained
// document with inline handlers; everything else is locked to this origin.
const csp = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; " +
	"img-src 'self' data:; font-src 'self' data:; connect-src 'self'; object-src 'none'; " +
	"frame-ancestors 'none'; base-uri 'self'; form-action 'self'"

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Frame-Options", "DENY")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		if r.TLS != nil {
			h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		}
		next.ServeHTTP(w, r)
	})
}

// csrfMiddleware rejects state-changing API calls that cannot have come from the
// dashboard. A Bearer-authenticated request is exempt: that credential is carried
// deliberately by the caller, not attached by a browser.
func csrfMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			next.ServeHTTP(w, r)
			return
		}
		if r.Header.Get(csrfHeader) != "1" {
			writeErr(w, http.StatusForbidden, "missing "+csrfHeader+" header: cross-site request blocked")
			return
		}
		if !sameOriginRequest(r) {
			writeErr(w, http.StatusForbidden, "cross-site request blocked")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sameOriginRequest accepts a missing Origin (browsers omit it on same-origin
// requests); the required custom header is what keeps a cross-site page out.
func sameOriginRequest(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host)
}
