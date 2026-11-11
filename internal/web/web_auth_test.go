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

	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(testACL(t, "app")).Handler()

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/topics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for unauthenticated request, got %d", rr.Code)
	}

	bad, _ := json.Marshal(map[string]string{"username": "app", "password": "wrong"})
	badReq := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(bad))
	badReq.Header.Set(csrfHeader, "1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, badReq)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad credentials, got %d", rr.Code)
	}

	good, _ := json.Marshal(map[string]string{"username": "app", "password": "changeme"})
	goodReq := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(good))
	goodReq.Header.Set(csrfHeader, "1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, goodReq)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for login, got %d", rr.Code)
	}
	cookies := rr.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected auth cookie")
	}

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

func TestAuthStatusIsReachableWithoutASession(t *testing.T) {
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
	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(testACL(t, "app")).Handler()

	readStatus := func(cookies []*http.Cookie) (int, map[string]any) {
		t.Helper()
		req := httptest.NewRequest("GET", "/api/v1/auth/status", nil)
		for _, c := range cookies {
			req.AddCookie(c)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		var body map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("status body is not JSON: %v (%s)", err, rr.Body.String())
		}
		return rr.Code, body
	}

	code, body := readStatus(nil)
	if code != http.StatusOK {
		t.Fatalf("auth status without a session = %d, want 200", code)
	}
	if body["enabled"] != true || body["authenticated"] != false {
		t.Fatalf("auth status = %v, want enabled=true authenticated=false", body)
	}

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/topics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("topics without a session = %d, want 401", rr.Code)
	}

	good, _ := json.Marshal(map[string]string{"username": "app", "password": "changeme"})
	loginReq := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(good))
	loginReq.Header.Set(csrfHeader, "1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, loginReq)
	if rr.Code != http.StatusOK {
		t.Fatalf("login = %d body=%s", rr.Code, rr.Body.String())
	}
	code, body = readStatus(rr.Result().Cookies())
	if code != http.StatusOK {
		t.Fatalf("auth status with a session = %d, want 200", code)
	}
	if body["enabled"] != true || body["authenticated"] != true {
		t.Fatalf("auth status = %v, want enabled=true authenticated=true", body)
	}
}

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
