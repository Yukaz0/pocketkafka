package web

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func TestCookieClassMatchesTheConnection(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Auth = true
	cfg.Web.AuthSecret = "cookie-class-signing-secret"
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
	h := New(store, gm, nil, 0, "test", "dev").WithAuth(cfg).WithACLStore(testACL(t, "app")).Handler()

	login := func(overTLS bool) []*http.Cookie {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"username": "app", "password": "a-good-password"})
		req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(body))
		req.Header.Set(csrfHeader, "1")
		if overTLS {
			req.TLS = &tls.ConnectionState{}
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("login over TLS=%v = %d", overTLS, rr.Code)
		}
		return rr.Result().Cookies()
	}
	status := func(overTLS bool, cookies ...*http.Cookie) int {
		t.Helper()
		req := httptest.NewRequest("GET", "/api/v1/topics", nil)
		if overTLS {
			req.TLS = &tls.ConnectionState{}
		}
		for _, c := range cookies {
			req.AddCookie(c)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr.Code
	}

	secureCookies := login(true)
	if len(secureCookies) != 1 || secureCookies[0].Name != hostCookieName {
		t.Fatalf("login over TLS issued %v, want a single %s cookie", secureCookies, hostCookieName)
	}
	if !secureCookies[0].Secure {
		t.Fatal("the __Host- cookie must carry Secure")
	}
	if code := status(true, secureCookies[0]); code != http.StatusOK {
		t.Fatalf("__Host- cookie over TLS = %d, want 200", code)
	}
	plainCopy := *secureCookies[0]
	plainCopy.Name = authCookieName
	if code := status(true, &plainCopy); code != http.StatusUnauthorized {
		t.Fatalf("plain cookie over TLS = %d, want 401", code)
	}
	if code := status(false, secureCookies[0]); code != http.StatusUnauthorized {
		t.Fatalf("__Host- cookie over plaintext = %d, want 401", code)
	}

	plainCookies := login(false)
	if len(plainCookies) != 1 || plainCookies[0].Name != authCookieName {
		t.Fatalf("login over plaintext issued %v, want a single %s cookie", plainCookies, authCookieName)
	}
	if plainCookies[0].Secure {
		t.Fatal("a Secure cookie over plaintext would never be sent back by a browser")
	}
	if code := status(false, plainCookies[0]); code != http.StatusOK {
		t.Fatalf("plain cookie over plaintext = %d, want 200", code)
	}
}
