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

const (
	authCookieName = "auth_token"
	hostCookieName = "__Host-" + authCookieName
)

func signToken(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payload + "." + sig
}

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

func userExists(users []config.SecurityUser, username string) bool {
	for _, u := range users {
		if u.Username == username {
			return true
		}
	}
	return false
}

func cookieName(secure bool) string {
	if secure {
		return hostCookieName
	}
	return authCookieName
}

func (s *Server) authMiddleware(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		if strings.HasPrefix(r.URL.Path, "/api/v1/fleet/agent/") || r.URL.Path == "/api/v1/fleet/enroll" {
			next.ServeHTTP(w, r)
			return
		}
		if !s.authEnabled {
			next.ServeHTTP(w, r)
			return
		}
		if !isAPIPath(r.URL.Path) || apiPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			presented := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
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

func (s *Server) isClusterToken(presented string) bool {
	return s.clusterToken != "" && subtle.ConstantTimeCompare([]byte(presented), []byte(s.clusterToken)) == 1
}

func (s *Server) clusterTokenPresented(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	return s.isClusterToken(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")))
}

func (s *Server) clusterTokenAllows(mux *http.ServeMux, r *http.Request) bool {
	_, pattern := mux.Handler(r)
	return clusterTokenRoutes[pattern]
}

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

		http.SetCookie(w, &http.Cookie{ //#nosec G124 -- Secure is runtime-dependent (TLS or trusted-proxy X-Forwarded-Proto); HttpOnly and SameSite=Strict always set
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

var loginDecoyHash = sync.OnceValue(func() string {
	hash, err := credential.HashPassword("pocketkafka-login-decoy")
	if err != nil {
		return ""
	}
	return hash
})

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

		http.SetCookie(w, &http.Cookie{ //#nosec G124 -- Secure is runtime-dependent (TLS or trusted-proxy X-Forwarded-Proto); HttpOnly and SameSite=Strict always set
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

func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled {
		writeJSON(w, 200, map[string]any{"enabled": false, "authenticated": true})
		return
	}
	_, _, authed := s.sessionFromRequest(r)
	writeJSON(w, 200, map[string]any{"enabled": true, "authenticated": authed})
}

func (s *Server) secureRequest(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	if !s.trustedProxy(r.RemoteAddr) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https")
}

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

func clientAddr(r *http.Request) string {
	if r.RemoteAddr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

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
