package web

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/fleet"
)

func TestFleetProxyDisabledServes404(t *testing.T) {
	s := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(false, "", "test-proxy-token", false, "", "")
	handler := s.Handler()

	for _, p := range []string{"/api/v1/fleet/servers", "/api/v1/fleet/jobs/x", "/api/v1/fleet/audit-events"} {
		req := httptest.NewRequest("GET", p, nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound && rr.Code != http.StatusForbidden {
			t.Fatalf("%s = %d, want 404/403", p, rr.Code)
		}
	}
}

func TestFleetInfoFlag(t *testing.T) {

	sDisabled := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(false, "", "test-proxy-token", false, "", "")
	req := httptest.NewRequest("GET", "/api/v1/info", nil)
	rr := httptest.NewRecorder()
	sDisabled.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("info status: %d", rr.Code)
	}
	var resDisabled map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &resDisabled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if enabled, ok := resDisabled["fleetEnabled"].(bool); !ok || enabled {
		t.Fatalf("expected fleetEnabled=false, got %v", resDisabled["fleetEnabled"])
	}

	sEnabled := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(true, "127.0.0.1:18090", "test-proxy-token", false, "", "")
	req2 := httptest.NewRequest("GET", "/api/v1/info", nil)
	rr2 := httptest.NewRecorder()
	sEnabled.Handler().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("info status: %d", rr2.Code)
	}
	var resEnabled map[string]interface{}
	if err := json.Unmarshal(rr2.Body.Bytes(), &resEnabled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if enabled, ok := resEnabled["fleetEnabled"].(bool); !ok || !enabled {
		t.Fatalf("expected fleetEnabled=true, got %v", resEnabled["fleetEnabled"])
	}
}

func TestFleetProxyForwardsWhenEnabled(t *testing.T) {
	reg, err := fleet.NewRegistry(t.TempDir(), "1234567890123456")
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	jobs := fleet.NewJobs(t.TempDir())
	audit := fleet.NewAudit(t.TempDir())
	mgr := fleet.NewAgentManager(reg, jobs)

	fleetSrv := fleet.NewServer(fleet.ServerConfig{
		Registry:   reg,
		Jobs:       jobs,
		Audit:      audit,
		Manager:    mgr,
		Authorizer: authz.AllowAllAuthorizer{},
		ProxyToken: "test-proxy-token",
	})

	ts := httptest.NewServer(fleetSrv.Handler())
	defer ts.Close()

	target := ts.Listener.Addr().String()

	ws := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(true, target, "test-proxy-token", false, "", "")
	handler := ws.Handler()

	req := httptest.NewRequest("GET", "/api/v1/fleet/status", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("forwarded status: %d, body: %s", rr.Code, rr.Body.String())
	}

	var status map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &status); err != nil {
		t.Fatalf("unmarshal forwarded response: %v", err)
	}
	if status["status"] != "ok" || status["enabled"] != true {
		t.Fatalf("unexpected forwarded response: %v", status)
	}
}

func TestFleetProxyRejectsForgedPrincipal(t *testing.T) {
	reg, _ := fleet.NewRegistry(t.TempDir(), "1234567890123456")
	jobs := fleet.NewJobs(t.TempDir())
	audit := fleet.NewAudit(t.TempDir())
	mgr := fleet.NewAgentManager(reg, jobs)
	fleetSrv := fleet.NewServer(fleet.ServerConfig{
		Registry: reg, Jobs: jobs, Audit: audit, Manager: mgr,
		ProxyToken: "test-proxy-token",
	})
	ts := httptest.NewServer(fleetSrv.Handler())
	defer ts.Close()
	target := ts.Listener.Addr().String()
	ws := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(true, target, "test-proxy-token", false, "", "")
	handler := ws.Handler()

	req := httptest.NewRequest("GET", "/api/v1/fleet/servers", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized && rr.Code != http.StatusForbidden {
		t.Fatalf("anon = %d, want 401/403", rr.Code)
	}

	req2 := httptest.NewRequest("GET", "/api/v1/fleet/servers", nil)
	req2.Header.Set("X-Pocketkafka-Principal", "admin")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusUnauthorized && rr2.Code != http.StatusForbidden {
		t.Fatalf("forged = %d, want 401/403", rr2.Code)
	}

	direct := httptest.NewRequest("GET", "/api/v1/fleet/servers", nil)
	direct.Header.Set("X-Pocketkafka-Principal", "admin")
	drr := httptest.NewRecorder()
	fleetSrv.Handler().ServeHTTP(drr, direct)
	if drr.Code != http.StatusForbidden {
		t.Fatalf("direct = %d, want 403", drr.Code)
	}
}

func TestFleetProxyForwardsOverTLS(t *testing.T) {
	reg, _ := fleet.NewRegistry(t.TempDir(), "1234567890123456")
	jobs := fleet.NewJobs(t.TempDir())
	audit := fleet.NewAudit(t.TempDir())
	mgr := fleet.NewAgentManager(reg, jobs)
	fleetSrv := fleet.NewServer(fleet.ServerConfig{
		Registry: reg, Jobs: jobs, Audit: audit, Manager: mgr,
		Authorizer: authz.AllowAllAuthorizer{},
		ProxyToken: "test-proxy-token",
	})
	ts := httptest.NewTLSServer(fleetSrv.Handler())
	defer ts.Close()
	u, _ := url.Parse(ts.URL)
	target := u.Host

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	pem := pemEncodeCert(t, ts.Certificate())
	if err := os.WriteFile(caPath, pem, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	ws := New(nil, nil, nil, 1, "test-cluster", "test-ver").WithFleet(true, target, "test-proxy-token", true, caPath, "")
	handler := ws.Handler()
	req := httptest.NewRequest("GET", "/api/v1/fleet/status", nil)
	req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("tls proxy status = %d, body %s", rr.Code, rr.Body.String())
	}

	if got := fleetTargetURL("0.0.0.0:18090", true); got.Scheme != "https" || got.Hostname() != "127.0.0.1" {
		t.Fatalf("wildcard TLS target = %v, want https://127.0.0.1:18090", got)
	}
}

func pemEncodeCert(t *testing.T, cert *x509.Certificate) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}
