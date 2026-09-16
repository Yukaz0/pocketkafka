// Package httpauth provides HTTP authentication and authorization for the
// broker's management surfaces (web API, REST proxy, Schema Registry) so they
// cannot be used to bypass the ACLs enforced on the Kafka protocol path.
package httpauth

import (
	"net/http"
	"strings"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
)

// Policy describes which paths are public and how a request maps to an
// authorization decision.
type Policy struct {
	// Public lists path prefixes that are served without authentication
	// (health checks, login, metrics scraping).
	Public []string
	// Classify maps a request to the operation/resource to authorize. When it
	// returns ok=false the request is authenticated but not authorized against
	// a specific resource.
	Classify func(r *http.Request) (authz.Operation, authz.Resource, bool)
}

// Middleware authenticates a request with HTTP Basic credentials and then
// authorizes it. When security is disabled it is a pass-through, preserving
// allow-all behaviour (decision D2).
type Middleware struct {
	enabled bool
	users   []config.SecurityUser
	authz   authz.Authorizer
	policy  Policy
}

// New builds the middleware. A nil authorizer is replaced with a default-deny
// in-memory store, so enabling security can never silently allow everything.
func New(enabled bool, users []config.SecurityUser, a authz.Authorizer, p Policy) *Middleware {
	if a == nil {
		a = authz.NewInMemory()
	}
	return &Middleware{enabled: enabled, users: users, authz: a, policy: p}
}

// Wrap returns next guarded by the middleware.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	if !m.enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m.isPublic(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		username, password, ok := r.BasicAuth()
		if !ok || !m.credentialsValid(username, password) {
			w.Header().Set("WWW-Authenticate", `Basic realm="pocketkafka"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		op, res, checked := m.classify(r)
		if checked {
			if err := m.authz.Authorize(username, op, res); err != nil {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (m *Middleware) isPublic(path string) bool {
	for _, p := range m.policy.Public {
		if p != "" && strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func (m *Middleware) credentialsValid(username, password string) bool {
	for _, u := range m.users {
		if u.Username == username && u.Password == password && u.Username != "" {
			return true
		}
	}
	return false
}

func (m *Middleware) classify(r *http.Request) (authz.Operation, authz.Resource, bool) {
	if m.policy.Classify != nil {
		return m.policy.Classify(r)
	}
	return DefaultClassify(r)
}

// DefaultClassify maps a request to a coarse operation/resource: reads map to
// Read, writes to Write, and the resource name is taken from the first
// /topics/<name> or /groups/<name> path segment, falling back to the cluster.
// It parses the path directly rather than using PathValue because the
// middleware runs before the router populates path variables.
func DefaultClassify(r *http.Request) (authz.Operation, authz.Resource, bool) {
	var op authz.Operation
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		op = authz.OpRead
	default:
		op = authz.OpWrite
	}
	segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	for i := 0; i+1 < len(segments); i++ {
		switch segments[i] {
		case "topics":
			return op, authz.Resource{Type: authz.ResourceTopic, Name: segments[i+1]}, true
		case "groups":
			return op, authz.Resource{Type: authz.ResourceGroup, Name: segments[i+1]}, true
		}
	}
	return op, authz.Resource{Type: authz.ResourceCluster, Name: "cluster"}, true
}
