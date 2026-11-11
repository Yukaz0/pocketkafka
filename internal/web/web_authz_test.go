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

func newSecureWeb(t *testing.T, acl *authz.Store) http.Handler {
	t.Helper()
	cfg := config.Default()
	cfg.Web.Auth = false
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

func login(t *testing.T, h http.Handler) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"username": "app", "password": "changeme"})
	req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
	req.Header.Set(csrfHeader, "1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
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
	if method != http.MethodGet {
		req.Header.Set(csrfHeader, "1")
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code
}

func TestSecurityImpliesWebLogin(t *testing.T) {
	h := newSecureWeb(t, authz.NewInMemory())
	if code := doJSON(t, h, "GET", "/api/v1/topics", nil, ""); code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET status = %d, want 401", code)
	}
}

func TestWebWriteRequiresACL(t *testing.T) {
	acl := authz.NewInMemory()
	if err := acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Read"}}); err != nil {
		t.Fatal(err)
	}
	h := newSecureWeb(t, acl)
	cookie := login(t, h)

	if code := doJSON(t, h, "GET", "/api/v1/topics", cookie, ""); code != http.StatusOK {
		t.Fatalf("authenticated GET status = %d, want 200", code)
	}

	if code := doJSON(t, h, "POST", "/api/v1/topics", cookie, `{"name":"x","partitions":1}`); code != http.StatusForbidden {
		t.Fatalf("write without ACL status = %d, want 403", code)
	}

	if err := acl.Upsert(authz.Rule{Principal: "app", ResourceType: authz.ResourceCluster, ResourceName: "*", Operations: []string{"Admin", "Write", "Read"}}); err != nil {
		t.Fatal(err)
	}
	if code := doJSON(t, h, "POST", "/api/v1/topics", cookie, `{"name":"x","partitions":1}`); code != http.StatusCreated {
		t.Fatalf("write with Admin ACL status = %d, want 201", code)
	}
}

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

	cfg.Security.Users = []config.SecurityUser{{Username: "other", Password: "x"}}
	h2 := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(acl).Handler()
	if code := doJSON(t, h2, "GET", "/api/v1/topics", cookie, ""); code != http.StatusUnauthorized {
		t.Fatalf("removed user's cookie status = %d, want 401", code)
	}
}
