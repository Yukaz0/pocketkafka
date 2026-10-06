package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/audit"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/credential"
)

// authCookieName is the signed login cookie. Behind TLS it is written with the
// __Host- prefix, which ties it to this origin (no Domain, Path=/, Secure).
const (
	authCookieName = "auth_token"
	hostCookieName = "__Host-" + authCookieName
)

// signToken produces an HMAC-SHA256 signed token "payload.signature".
func signToken(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payload + "." + sig
}

// verifyToken checks a token's signature and returns the payload.
func verifyToken(token, secret string) (string, bool) {
	idx := strings.LastIndex(token, ".")
	if idx < 0 {
		return "", false
	}
	payload, sig := token[:idx], token[idx+1:]
	expected := signToken(payload, secret)
	got := payload + "." + sig
	return payload, subtle.ConstantTimeCompare([]byte(expected), []byte(got)) == 1
}

// userExists reports whether a username is a configured credential.
func userExists(users []config.SecurityUser, username string) bool {
	for _, u := range users {
		if u.Username == username {
			return true
		}
	}
	return false
}

// cookieName returns the cookie to read or write. The __Host- form is only used
// on a connection the browser made over HTTPS; writing it on plain HTTP would
// make the browser reject the cookie outright.
func cookieName(secure bool) string {
	if secure {
		return hostCookieName
	}
	return authCookieName
}

// authMiddleware requires an authenticated identity for every API route and for
// /metrics. Login, logout and the session status probe answer without one, so
// the SPA can render the login form.
func (s *Server) authMiddleware(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authEnabled {
			next.ServeHTTP(w, r)
			return
		}
		if !isAPIPath(r.URL.Path) || apiPublicPath(r.URL.Path) {
			// Static assets and the liveness probes are not gated; /healthz and
			// /livez reveal only whether the process is up.
			next.ServeHTTP(w, r)
			return
		}
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			presented := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
			// The cluster health token is accepted only on the routes that
			// report this broker's health: it used to unlock every GET under
			// /api, including message contents, the audit trail and the log.
			if s.isClusterToken(presented) {
				if !s.clusterTokenAllows(mux, r) {
					s.auditDeny(w, r, audit.Anonymous, "auth.bearer", "cluster token used outside the health routes")
					writeErr(w, http.StatusForbidden, "the cluster token is limited to health reports")
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if _, ok := s.delegatedPrincipal(presented); ok {
				next.ServeHTTP(w, r)
				return
			}
			audit.Log(audit.Event{
				Actor: audit.Anonymous, Action: "auth.bearer", Resource: r.URL.Path,
				Result: audit.ResultDeny, Detail: "invalid bearer token", RemoteAddr: clientAddr(r),
			})
			writeErr(w, http.StatusUnauthorized, "invalid bearer token")
			return
		}
		if _, _, ok := s.sessionFromRequest(r); !ok {
			writeErr(w, http.StatusUnauthorized, "authentication required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isClusterToken reports whether a presented bearer value is this broker's
// cluster health token.
func (s *Server) isClusterToken(presented string) bool {
	return s.clusterToken != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(s.clusterToken)) == 1
}

// clusterTokenPresented reports whether the request carries the cluster token.
func (s *Server) clusterTokenPresented(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	return s.isClusterToken(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))
}

// clusterTokenAllows reports whether the read-only cluster token may call this
// route. ServeMux reports the matched pattern already qualified with its method
// ("GET /api/v1/health/overview"), which is the form the table is keyed by.
func (s *Server) clusterTokenAllows(mux *http.ServeMux, r *http.Request) bool {
	_, pattern := mux.Handler(r)
	return clusterTokenRoutes[pattern]
}

// delegatedPrincipal resolves a delegated dashboard token to its user.
func (s *Server) delegatedPrincipal(token string) (string, bool) {
	if s.delegatedTokens == nil || !s.authEnabled || !s.authorize {
		return "", false
	}
	principal, ok := s.delegatedTokens.Principal(token)
	if !ok || !userExists(s.users, principal) {
		return "", false
	}
	return principal, true
}

// sessionFromRequest validates the login cookie against the server-side session
// store and returns the user and session id.
//
// The cookie class must match the connection: over HTTPS only the __Host- form
// is accepted. The plain form carries no Secure attribute, so a network
// attacker who can inject a cookie must not get it honoured on a connection the
// browser made over TLS. The same secureRequest(r) that chose the name at login
// chooses it here.
func (s *Server) sessionFromRequest(r *http.Request) (user, sid string, ok bool) {
	name := authCookieName
	if s.secureRequest(r) {
		name = hostCookieName
	}
	cookie, err := r.Cookie(name)
	if err != nil {
		return "", "", false
	}
	payload, ok := verifyToken(cookie.Value, s.authSecret)
	if !ok {
		return "", "", false
	}
	user, sid, exp, ok := parseSessionPayload(payload)
	if !ok || time.Now().After(exp) {
		return "", "", false
	}
	if !userExists(s.users, user) {
		return "", "", false
	}
	if s.sessions == nil || !s.sessions.Validate(sid, user) {
		return "", "", false
	}
	return user, sid, true
}

// requestPrincipal returns the verified identity behind a request: the session
// user, or the user a delegated bearer token maps to. The cluster health token
// never yields a principal, so it can never satisfy an ACL rule.
func (s *Server) requestPrincipal(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		if s.clusterTokenPresented(r) {
			return ""
		}
		principal, ok := s.delegatedPrincipal(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))
		if !ok {
			return ""
		}
		return principal
	}
	user, _, ok := s.sessionFromRequest(r)
	if !ok {
		return ""
	}
	return user
}

// handleLogin validates credentials against security.users and starts a session.
// Every rejection costs the same class of work, so the answer does not reveal
// whether the username exists (see the decoy below).
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	addr := clientAddr(r)
	username := strings.TrimSpace(req.Username)

	// Rate limit per source and per account. The source key is bounded by the
	// number of addresses that can reach the broker; the account key is added
	// only for a configured user, because the username in the body is
	// attacker-controlled and would otherwise let an anonymous flood grow the
	// limiter's map without bound. Guessing a *nonexistent* account still costs
	// the source its budget.
	keys := []string{"ip:" + addr}
	if userExists(s.users, username) {
		keys = append(keys, "user:"+strings.ToLower(username))
	}
	for _, key := range keys {
		if ok, retry := s.loginLimiter.Allow(key); !ok {
			w.Header().Set("Retry-After", retrySeconds(retry))
			audit.Log(audit.Event{
				Actor: username, Action: "auth.login", Resource: "dashboard",
				Result: audit.ResultDeny, Detail: "rate limited", RemoteAddr: addr,
			})
			writeErr(w, http.StatusTooManyRequests, "too many attempts: retry shortly")
			return
		}
	}

	// paidKDF records whether this attempt already spent PBKDF2 work: a stored
	// verifier pays it inside Matches, so a wrong password against such an
	// account is already as expensive as the decoy below.
	paidKDF := false
	for i := range s.users {
		u := &s.users[i]
		if u.Username != username || u.Username == "" {
			continue
		}
		stored := u.Password
		if stored == "" {
			stored = u.PasswordHash
		}
		if stored == "" {
			break
		}
		paidKDF = credential.LooksLikeHash(stored)
		if !credential.Matches(stored, req.Password) {
			break
		}
		for _, key := range keys {
			s.loginLimiter.Succeed(key)
		}
		id, exp, err := s.sessions.Create(u.Username)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "could not start a session")
			return
		}
		secure := s.secureRequest(r)
		//#nosec G124 -- Secure is derived from secureRequest(r): true whenever the
		// browser's connection (or a trusted proxy) is HTTPS. Forcing it on a
		// plaintext development connection would make the browser drop the cookie.
		http.SetCookie(w, &http.Cookie{
			Name:     cookieName(secure),
			Value:    signToken(sessionPayload(u.Username, exp, id), s.authSecret),
			Path:     "/",
			HttpOnly: true,
			Secure:   secure,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   int(time.Until(exp).Seconds()),
		})
		audit.Log(audit.Event{
			Actor: u.Username, Action: "auth.login", Resource: "dashboard",
			Result: audit.ResultOK, RemoteAddr: addr,
		})
		writeJSON(w, 200, map[string]string{"status": "ok", "username": u.Username})
		return
	}

	// An unknown username, a plaintext-password account, and an account without
	// a usable credential all reach here without having paid for a KDF. Verify a
	// decoy verifier (result discarded) so their answer takes as long as a
	// PBKDF2-backed account's, which is what keeps the form from enumerating
	// users.
	if !paidKDF {
		credential.VerifyPassword(loginDecoyHash(), req.Password)
	}
	for _, key := range keys {
		s.loginLimiter.Fail(key)
	}
	audit.Log(audit.Event{
		Actor: username, Action: "auth.login", Resource: "dashboard",
		Result: audit.ResultDeny, Detail: "invalid credentials", RemoteAddr: addr,
	})
	writeErr(w, 401, "invalid credentials")
}

// loginDecoyHash is a PBKDF2 verifier that no password matches. It exists so a
// rejection for a username that does not exist costs the same PBKDF2 work as a
// rejection for one that does; the same iteration count is what makes the two
// indistinguishable over the network. Computed once, on the first rejected
// login, so startup and the CLI subcommands never pay for it.
var loginDecoyHash = sync.OnceValue(func() string {
	hash, err := credential.HashPassword("pocketkafka-login-decoy")
	if err != nil {
		return ""
	}
	return hash
})

// handleLogout revokes the session and clears the cookie.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	user, sid, ok := s.sessionFromRequest(r)
	if ok && s.sessions != nil {
		s.sessions.Revoke(sid)
		audit.Log(audit.Event{
			Actor: user, Action: "auth.logout", Resource: "dashboard",
			Result: audit.ResultOK, RemoteAddr: clientAddr(r),
		})
	}
	for _, name := range []string{hostCookieName, authCookieName} {
		//#nosec G124 -- HttpOnly and SameSite are set below; Secure follows the
		// connection, exactly as on the login cookie.
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			Secure:   s.secureRequest(r),
			SameSite: http.SameSiteStrictMode,
			MaxAge:   -1,
		})
	}
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// handleAuthStatus reports whether the current session is authenticated.
func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled {
		writeJSON(w, 200, map[string]any{"enabled": false, "authenticated": true})
		return
	}
	_, _, authed := s.sessionFromRequest(r)
	writeJSON(w, 200, map[string]any{"enabled": true, "authenticated": authed})
}

// secureRequest reports whether the browser's connection to the dashboard is
// HTTPS, which decides the Secure cookie attribute. The X-Forwarded-Proto
// header is believed only from a configured trusted proxy.
func (s *Server) secureRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if !s.trustedProxy(r.RemoteAddr) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https")
}

// trustedProxy reports whether a peer address is one of web.trusted_proxies.
func (s *Server) trustedProxy(remoteAddr string) bool {
	if len(s.trustedProxies) == 0 {
		return false
	}
	host := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	for _, entry := range s.trustedProxies {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if entry == host {
			return true
		}
		if ip == nil {
			continue
		}
		if _, cidr, err := net.ParseCIDR(entry); err == nil && cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// clientAddr renders a request's peer address for the audit trail.
func clientAddr(r *http.Request) string {
	if r.RemoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// auditDeny records a refused request and answers 403.
func (s *Server) auditDeny(w http.ResponseWriter, r *http.Request, actor, action, detail string) {
	audit.Log(audit.Event{
		Actor: actor, Action: action, Resource: r.URL.Path,
		Result: audit.ResultDeny, Detail: detail, RemoteAddr: clientAddr(r),
	})
	writeErr(w, http.StatusForbidden, detail)
}

func retrySeconds(d time.Duration) string {
	secs := int(d.Seconds())
	if secs < 1 {
		secs = 1
	}
	return strconv.Itoa(secs)
}
