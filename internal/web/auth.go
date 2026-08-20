package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
)

// authCookieName is the name of the signed login cookie.
const authCookieName = "auth_token"

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

// authPayloadToUser extracts the username from a signed token payload of the
// form "username|timestamp".
func authPayloadToUser(payload string) string {
	if idx := strings.Index(payload, "|"); idx >= 0 {
		return payload[:idx]
	}
	return payload
}

// newAuthMiddleware protects /api/* and /metrics (but not the login endpoint or
// static assets) when web auth is enabled. Unauthenticated API calls receive
// 401 so the SPA can show the login screen.
func newAuthMiddleware(users []config.SecurityUser, secret string, enabled bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !enabled {
				next.ServeHTTP(w, r)
				return
			}
			// Login endpoint and static assets are always reachable.
			if r.URL.Path == "/api/v1/auth/login" || r.URL.Path == "/" {
				next.ServeHTTP(w, r)
				return
			}
			if strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/metrics" {
				cookie, err := r.Cookie(authCookieName)
				if err != nil {
					writeErr(w, http.StatusUnauthorized, "authentication required")
					return
				}
				username, ok := verifyToken(cookie.Value, secret)
				if !ok || !userExists(users, authPayloadToUser(username)) {
					writeErr(w, http.StatusUnauthorized, "invalid session")
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

func userExists(users []config.SecurityUser, username string) bool {
	for _, u := range users {
		if u.Username == username {
			return true
		}
	}
	return false
}

// handleLogin validates credentials against security.users and issues a signed
// HttpOnly cookie.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	for _, u := range s.users {
		if u.Username == req.Username &&
			subtle.ConstantTimeCompare([]byte(u.Password), []byte(req.Password)) == 1 {
			payload := req.Username + "|" + time.Now().Format(time.RFC3339)
			token := signToken(payload, s.authSecret)
			http.SetCookie(w, &http.Cookie{
				Name:     authCookieName,
				Value:    token,
				Path:     "/",
				HttpOnly: true,
				SameSite: http.SameSiteStrictMode,
				MaxAge:   86400,
			})
			writeJSON(w, 200, map[string]string{"status": "ok", "username": req.Username})
			return
		}
	}
	writeErr(w, 401, "invalid credentials")
}

// handleLogout clears the auth cookie.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     authCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// handleAuthStatus reports whether the current session is authenticated.
func (s *Server) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled {
		writeJSON(w, 200, map[string]any{"enabled": false, "authenticated": true})
		return
	}
	cookie, err := r.Cookie(authCookieName)
	authed := false
	if err == nil {
		username, ok := verifyToken(cookie.Value, s.authSecret)
		authed = ok && userExists(s.users, authPayloadToUser(username))
	}
	writeJSON(w, 200, map[string]any{"enabled": true, "authenticated": authed})
}
