package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

// newSecureWeb builds a web handler with security enabled and a shared ACL store.
func newSecureWeb(t *testing.T, acl *authz.Store) http.Handler {
	t.Helper()
	cfg := config.Default()
	cfg.Web.Auth = false // worse case: web.auth off, security on
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "changeme"}}

	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	os, _ := coordinator.NewOffsetStore("inmemory", t.TempDir())
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)
	return New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(acl).Handler()
}

// login returns the auth cookie for the default app user.
func login(t *testing.T, h http.Handler) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"username": "app", "password": "changeme"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body)))
	if rr.Code != http.StatusOK {
		t.Fatalf("login status = %d", rr.Code)
	}
	cookies := rr.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("no auth cookie issued")
	}
	return cookies[0]
}

func doJSON(t *testing.T, h http.Handler, method, path string, cookie *http.Cookie, body string) int {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code
}

// TestSecurityImpliesWebLogin: turning security on must protect the web API even
// when web.auth is false, otherwise the dashboard is an open bypass.
func TestSecurityImpliesWebLogin(t *testing.T) {
	h := newSecureWeb(t, authz.NewInMemory())
	if code := doJSON(t, h, "GET", "/api/v1/topics", nil, ""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET status = %d, want 401", code)
	}
}

// TestWebWriteRequiresACL: a valid session without an Admin grant cannot mutate
// state; with the grant it can.
func TestWebWriteRequiresACL(t *testing.T) {
	acl := authz.NewInMemory()
	h := newSecureWeb(t, acl)
	cookie := login(t, h)

	// Read-only is authentication-only and should succeed.
	if code := doJSON(t, h, "GET", "/api/v1/topics", cookie, ""); code != http.StatusOK {
		t.Fatalf("authenticated GET status = %d, want 200", code)
	}

	// Write with no ACL is forbidden.
	if code := doJSON(t, h, "POST", "/api/v1/topics", cookie, `{"name":"x","partitions":1}`); code != http.StatusForbidden {
		t.Fatalf("write without ACL status = %d, want 403", code)
	}

	// Grant cluster Admin and retry.
	if err := acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Admin", "Write", "Read"}}); err != nil {
		t.Fatal(err)
	}
	if code := doJSON(t, h, "POST", "/api/v1/topics", cookie, `{"name":"x","partitions":1}`); code != http.StatusCreated {
		t.Fatalf("write with Admin ACL status = %d, want 201", code)
	}
}

// TestACLMutationRequiresAdmin: ACL edits need Admin specifically, not just Write.
func TestACLMutationRequiresAdmin(t *testing.T) {
	acl := authz.NewInMemory()
	if err := acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Write"}}); err != nil {
		t.Fatal(err)
	}
	h := newSecureWeb(t, acl)
	cookie := login(t, h)

	body := `{"principal":"bob","resourceType":"topic","resourceName":"t","operations":["Read"]}`
	if code := doJSON(t, h, "PUT", "/api/v1/acls", cookie, body); code != http.StatusForbidden {
		t.Fatalf("ACL edit with only Write status = %d, want 403", code)
	}

	if err := acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Admin"}}); err != nil {
		t.Fatal(err)
	}
	if code := doJSON(t, h, "PUT", "/api/v1/acls", cookie, body); code != http.StatusOK {
		t.Fatalf("ACL edit with Admin status = %d, want 200", code)
	}
}

// TestRemovedUserSessionRejected: a cookie for a user that no longer exists must
// not be accepted (authentication middleware re-checks membership).
func TestRemovedUserSessionRejected(t *testing.T) {
	acl := authz.NewInMemory()
	_ = acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Read"}})

	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "changeme"}}
	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	os, _ := coordinator.NewOffsetStore("inmemory", t.TempDir())
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)
	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(acl).Handler()

	cookie := login(t, h)
	if code := doJSON(t, h, "GET", "/api/v1/topics", cookie, ""); code != http.StatusOK {
		t.Fatalf("session with existing user status = %d, want 200", code)
	}

	// Rebuild a handler whose user list no longer contains "app"; the old cookie
	// must be rejected.
	cfg.Security.Users = []config.SecurityUser{{Username: "other", Password: "x"}}
	h2 := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(acl).Handler()
	if code := doJSON(t, h2, "GET", "/api/v1/topics", cookie, ""); code != http.StatusUnauthorized {
		t.Fatalf("removed user's cookie status = %d, want 401", code)
	}
}
