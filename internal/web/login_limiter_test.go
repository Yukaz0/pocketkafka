package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func TestLoginLimiterKeysAreBounded(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "limiter-test-signing-secret"
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "a-good-password"}}

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
	s := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg)
	h := s.Handler()

	login := func(user, addr string) int {
		t.Helper()
		body := fmt.Sprintf(`{"username":%q,"password":"wrong"}`, user)
		req := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(body))
		req.Header.Set(csrfHeader, "1")
		req.RemoteAddr = addr
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}

	for i := 0; i < 30; i++ {
		code := login(fmt.Sprintf("ghost-%d", i), fmt.Sprintf("198.51.100.%d:5000", i%250+1))
		if code != http.StatusUnauthorized {
			t.Fatalf("unknown username attempt %d = %d, want 401", i, code)
		}
	}
	if got := s.loginLimiter.Failures("user:ghost-0"); got != 0 {
		t.Fatalf("a nonexistent account was tracked (%d failures), so the limiter state is attacker-growable", got)
	}

	limited := false
	for i := 0; i < 12; i++ {
		if login("app", fmt.Sprintf("203.0.113.%d:5000", i+1)) == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("a configured account was never rate limited")
	}
}
