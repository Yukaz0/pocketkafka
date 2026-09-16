package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
)

func TestBuildWebServerDisabled(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Enabled = false
	ws, srv := buildWebServer(cfg, nil, nil, nil, nil, "test")
	if ws != nil || srv != nil {
		t.Fatalf("buildWebServer(disabled) = (%v, %v), want (nil, nil)", ws, srv)
	}
	// Shutdown must stay safe when the web server was never created.
	shutdownHTTP(srv)
}

func TestBuildWebServerEnabled(t *testing.T) {
	cfg := config.Default()
	cfg.Web.Enabled = true
	cfg.Web.Auth = false
	ws, srv := buildWebServer(cfg, nil, nil, nil, nil, "test")
	if ws == nil || srv == nil {
		t.Fatal("buildWebServer(enabled) returned a nil server")
	}
	if srv.ReadHeaderTimeout == 0 {
		t.Error("ReadHeaderTimeout is zero; slow-header clients would not be cut off")
	}
	if srv.WriteTimeout != 0 {
		t.Error("WriteTimeout must be unset for the web server so WebSocket tail is not cut off")
	}
}

func TestNewHTTPServerTimeouts(t *testing.T) {
	cfg := config.Default()
	cfg.Network.ReadTimeoutMs = 1000
	cfg.Network.WriteTimeoutMs = 2000
	cfg.Network.IdleTimeoutMs = 3000

	plain := newHTTPServer(cfg, ":0", http.NotFoundHandler(), false)
	if plain.ReadHeaderTimeout != time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 1s", plain.ReadHeaderTimeout)
	}
	if plain.WriteTimeout != 2*time.Second {
		t.Errorf("WriteTimeout = %v, want 2s", plain.WriteTimeout)
	}
	if plain.IdleTimeout != 3*time.Second {
		t.Errorf("IdleTimeout = %v, want 3s", plain.IdleTimeout)
	}

	hijackable := newHTTPServer(cfg, ":0", http.NotFoundHandler(), true)
	if hijackable.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 for WebSocket-capable server", hijackable.WriteTimeout)
	}
}

func TestLimitBodyRejectsOversized(t *testing.T) {
	var readErr error
	h := limitBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
	}), 8)

	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("this body is way too long"))
	h.ServeHTTP(httptest.NewRecorder(), req)
	if readErr == nil {
		t.Fatal("oversized body was read without error; limit not applied")
	}
}

func TestLimitBodyAllowsNormal(t *testing.T) {
	h := limitBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, _ = w.Write(b)
	}), 1024)

	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("ok"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
}
