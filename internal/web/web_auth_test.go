package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

// TestAuthIntegration verifies the web UI login flow (Fitur 12): unauthenticated
// API calls are rejected, login issues a signed cookie, and the cookie grants
// access.
func TestAuthIntegration(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "test-secret"
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "changeme"}}

	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	os, err := coordinator.NewOffsetStore("inmemory", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)

	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).Handler()

	// Unauthenticated request must be rejected.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/topics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", rr.Code)
	}

	// Wrong password must fail.
	bad, _ := json.Marshal(map[string]string{"username": "app", "password": "wrong"})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(bad)))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad credentials, got %d", rr.Code)
	}

	// Correct login issues a cookie.
	good, _ := json.Marshal(map[string]string{"username": "app", "password": "changeme"})
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(good)))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for login, got %d", rr.Code)
	}
	cookies := rr.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected auth cookie")
	}

	// Authenticated request with the cookie succeeds.
	req := httptest.NewRequest("GET", "/api/v1/topics", nil)
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for authenticated request, got %d body=%s", rr.Code, rr.Body.String())
	}
}

// TestHealthEndpoints verifies /healthz and /livez (Fitur 10).
func TestHealthEndpoints(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	os, _ := coordinator.NewOffsetStore("inmemory", t.TempDir())
	gm := coordinator.NewGroupManager(os, 0, "localhost", 9092, 45000)
	h := New(store, gm, nil, 0, "test", "dev").Handler()

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz expected 200, got %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/livez", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "ok" {
		t.Fatalf("livez expected 200 ok, got %d %q", rr.Code, rr.Body.String())
	}
}
