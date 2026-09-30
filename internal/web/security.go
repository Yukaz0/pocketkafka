package web

// Transport-level protections for the dashboard.
//
// Two things are load-bearing here:
//
//   - The mutation endpoints (ACLs, cluster registry, topic and group
//     operations) authenticate with an ambient cookie. Without a cross-site
//     guard, any page the operator visits could POST to this broker while that
//     cookie is attached. A required custom header plus a same-origin check
//     closes that class: a cross-site form cannot set a header, and a
//     cross-origin fetch with one is rejected by the browser's preflight.
//   - The embedded page is served from the same origin as the API, so a CSP
//     that forbids external scripts and framing costs nothing but still stops
//     an injected script from loading anything off-origin.

import (
	"net/http"
	"net/url"
	"strings"
)

// csrfHeader is required on every state-changing /api request. The SPA sends it
// from its api() helper and from the login form.
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
// dashboard itself.
//
// A request authenticated by an Authorization header is exempt: that is a
// broker-to-broker credential carried deliberately by the caller, not a browser
// cookie attached automatically, so there is nothing for a third-party page to
// ride on.
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

// sameOriginRequest compares the Origin header with the Host the request was
// sent to. Browsers omit Origin on same-origin GETs and on some same-origin
// requests, and a non-browser client may not send it at all; a missing Origin is
// therefore accepted, because the required custom header is what actually stops
// a cross-site page from reaching these endpoints.
func sameOriginRequest(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && strings.EqualFold(u.Host, r.Host)
}
