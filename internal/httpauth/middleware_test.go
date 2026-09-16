package httpauth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func users() []config.SecurityUser {
	return []config.SecurityUser{{Username: "alice", Password: "s3cret"}}
}

func do(h http.Handler, method, path, user, pass string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if user != "" || pass != "" {
		req.SetBasicAuth(user, pass)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestDisabledPassesThrough(t *testing.T) {
	m := New(false, nil, nil, Policy{})
	rec := do(m.Wrap(okHandler()), http.MethodPost, "/topics/x", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 when security disabled", rec.Code)
	}
}

func TestEnabledRequiresCredentials(t *testing.T) {
	m := New(true, users(), authz.NewInMemory(), Policy{})
	rec := do(m.Wrap(okHandler()), http.MethodGet, "/subjects", "", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") == "" {
		t.Error("missing WWW-Authenticate header")
	}
}

func TestEnabledRejectsWrongCredentials(t *testing.T) {
	m := New(true, users(), authz.NewInMemory(), Policy{})
	rec := do(m.Wrap(okHandler()), http.MethodGet, "/subjects", "alice", "wrong")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestEnabledForbidsWithoutACL(t *testing.T) {
	// Valid credentials but the (default-deny) store grants nothing.
	m := New(true, users(), authz.NewInMemory(), Policy{})
	rec := do(m.Wrap(okHandler()), http.MethodGet, "/subjects", "alice", "s3cret")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestEnabledAllowsWithACL(t *testing.T) {
	store := authz.NewInMemory()
	if err := store.Upsert(authz.Rule{
		Principal: "alice", ResourceType: authz.ResourceCluster,
		ResourceName: "*", Operations: []string{"Read", "Write", "Admin"},
	}); err != nil {
		t.Fatal(err)
	}
	m := New(true, users(), store, Policy{})
	rec := do(m.Wrap(okHandler()), http.MethodGet, "/subjects", "alice", "s3cret")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestPublicPathBypassesAuth(t *testing.T) {
	m := New(true, users(), authz.NewInMemory(), Policy{Public: []string{"/healthz", "/livez"}})
	rec := do(m.Wrap(okHandler()), http.MethodGet, "/healthz", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for public path", rec.Code)
	}
}

func TestClassifyUsesTopicPathValue(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /topics/{topic}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	store := authz.NewInMemory()
	if err := store.Upsert(authz.Rule{
		Principal: "alice", ResourceType: authz.ResourceTopic,
		ResourceName: "orders", Operations: []string{"Read"},
	}); err != nil {
		t.Fatal(err)
	}
	m := New(true, users(), store, Policy{})
	h := m.Wrap(mux)

	if rec := do(h, http.MethodGet, "/topics/orders", "alice", "s3cret"); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for authorized topic", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/topics/secret", "alice", "s3cret"); rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for unauthorized topic", rec.Code)
	}
}
