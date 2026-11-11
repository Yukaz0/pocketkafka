package fleet

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
)

func TestFleetDisabledServes404(t *testing.T) {
	h := NewDisabledHandler()
	for _, p := range []string{"/api/v1/fleet/servers", "/api/v1/fleet/jobs/x", "/api/v1/fleet/audit-events"} {
		req := httptest.NewRequest("GET", p, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != 404 && rr.Code != 403 {
			t.Fatalf("%s = %d, want 404/403", p, rr.Code)
		}
	}
}

type mockAuthorizer struct {
	rules map[string]map[string][]string
}

func (m *mockAuthorizer) Authorize(principal string, op authz.Operation, res authz.Resource) error {
	typeRules, ok := m.rules[principal]
	if !ok {
		return &authz.DeniedError{Principal: principal, Operation: op, Resource: res}
	}
	ops, ok := typeRules[res.Type]
	if !ok {
		ops, ok = typeRules["*"]
	}
	if !ok {
		return &authz.DeniedError{Principal: principal, Operation: op, Resource: res}
	}
	for _, granted := range ops {
		if granted == "*" || strings.EqualFold(granted, string(op)) {
			return nil
		}
		if strings.EqualFold(granted, "Admin") {
			return nil
		}
		if strings.EqualFold(granted, "Control") && (strings.EqualFold(string(op), "Control") || strings.EqualFold(string(op), "Describe")) {
			return nil
		}
		if strings.EqualFold(granted, "Write") && (strings.EqualFold(string(op), "Write") || strings.EqualFold(string(op), "Describe")) {
			return nil
		}
		if strings.EqualFold(granted, "Read") && (strings.EqualFold(string(op), "Read") || strings.EqualFold(string(op), "Describe")) {
			return nil
		}
	}
	return &authz.DeniedError{Principal: principal, Operation: op, Resource: res}
}

func setupFleetTestServer(t *testing.T, authorizer authz.Authorizer) (*FleetServer, *Registry, *Jobs, *Audit) {
	t.Helper()
	dir := t.TempDir()
	reg, err := NewRegistry(dir, "1234567890123456")
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	jobs := NewJobs(dir)
	audit := NewAudit(dir)
	mgr := NewAgentManager(reg, jobs)

	srv := NewServer(ServerConfig{
		Registry:     reg,
		Jobs:         jobs,
		Audit:        audit,
		Manager:      mgr,
		Authorizer:   authorizer,
		MaxBodyBytes: 1024 * 1024,
		ProxyToken:   "test-proxy-token",
	})
	return srv, reg, jobs, audit
}

func TestFleetAuthzRoles(t *testing.T) {
	auth := &mockAuthorizer{
		rules: map[string]map[string][]string{
			"viewer": {
				"fleet_server":    {"Describe"},
				"fleet_container": {"Describe"},
				"fleet_job":       {"Describe"},
				"fleet_audit":     {"Describe"},
			},
			"operator": {
				"fleet_server":    {"Describe"},
				"fleet_container": {"Control", "Describe"},
				"fleet_job":       {"Describe"},
				"fleet_audit":     {"Describe"},
			},
			"admin": {
				"fleet_server":    {"Admin"},
				"fleet_container": {"Admin"},
				"fleet_job":       {"Admin"},
				"fleet_audit":     {"Admin"},
			},
		},
	}

	srv, reg, _, _ := setupFleetTestServer(t, auth)

	tok, err := reg.Enroll("srv-1")
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	_, err = reg.ConsumeToken(tok)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		role       string
		wantStatus int
	}{

		{"viewer gets servers", "GET", "/api/v1/fleet/servers", "", "viewer", http.StatusOK},
		{"viewer gets jobs", "GET", "/api/v1/fleet/jobs", "", "viewer", http.StatusOK},
		{"viewer gets audit-events", "GET", "/api/v1/fleet/audit-events", "", "viewer", http.StatusOK},
		{"viewer cannot restart container", "POST", "/api/v1/fleet/containers/c1/restart", `{"server_id":"srv-1"}`, "viewer", http.StatusForbidden},
		{"viewer cannot enroll server", "POST", "/api/v1/fleet/enrollments", `{"server_id":"srv-2"}`, "viewer", http.StatusForbidden},
		{"viewer cannot revoke server", "POST", "/api/v1/fleet/servers/srv-1/revoke", "", "viewer", http.StatusForbidden},

		{"operator gets servers", "GET", "/api/v1/fleet/servers", "", "operator", http.StatusOK},
		{"operator gets jobs", "GET", "/api/v1/fleet/jobs", "", "operator", http.StatusOK},
		{"operator gets audit-events", "GET", "/api/v1/fleet/audit-events", "", "operator", http.StatusOK},
		{"operator can restart container", "POST", "/api/v1/fleet/containers/c1/restart", `{"server_id":"srv-1"}`, "operator", http.StatusAccepted},
		{"operator cannot enroll server", "POST", "/api/v1/fleet/enrollments", `{"server_id":"srv-2"}`, "operator", http.StatusForbidden},
		{"operator cannot revoke server", "POST", "/api/v1/fleet/servers/srv-1/revoke", "", "operator", http.StatusForbidden},

		{"admin gets servers", "GET", "/api/v1/fleet/servers", "", "admin", http.StatusOK},
		{"admin gets jobs", "GET", "/api/v1/fleet/jobs", "", "admin", http.StatusOK},
		{"admin gets audit-events", "GET", "/api/v1/fleet/audit-events", "", "admin", http.StatusOK},
		{"admin can restart container", "POST", "/api/v1/fleet/containers/c1/restart", `{"server_id":"srv-1"}`, "admin", http.StatusAccepted},
		{"admin can enroll server", "POST", "/api/v1/fleet/enrollments", `{"server_id":"srv-3"}`, "admin", http.StatusOK},
		{"admin can revoke server", "POST", "/api/v1/fleet/servers/srv-1/revoke", "", "admin", http.StatusOK},
	}

	handler := srv.Handler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var bodyReader *bytes.Reader
			if tc.body != "" {
				bodyReader = bytes.NewReader([]byte(tc.body))
			} else {
				bodyReader = bytes.NewReader(nil)
			}
			req := httptest.NewRequest(tc.method, tc.path, bodyReader)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Pocketkafka-Principal", tc.role)
			req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != tc.wantStatus {
				t.Fatalf("%s %s as %s: got status %d, want %d (body: %s)", tc.method, tc.path, tc.role, rr.Code, tc.wantStatus, rr.Body.String())
			}
		})
	}
}

func TestFleetMutationWritesAudit(t *testing.T) {
	auth := &mockAuthorizer{
		rules: map[string]map[string][]string{
			"admin": {
				"*": {"*"},
			},
		},
	}

	srv, reg, _, audit := setupFleetTestServer(t, auth)
	handler := srv.Handler()

	enrollReq := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", strings.NewReader(`{"server_id":"host-alpha"}`))
	enrollReq.Header.Set("Content-Type", "application/json")
	enrollReq.Header.Set("X-Pocketkafka-Principal", "admin")
	enrollReq.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, enrollReq)
	if rr.Code != http.StatusOK && rr.Code != http.StatusCreated {
		t.Fatalf("enrollment create failed: %d (%s)", rr.Code, rr.Body.String())
	}

	tokAlpha, err := reg.Enroll("host-alpha")
	if err != nil {
		t.Fatalf("enroll host-alpha: %v", err)
	}
	if _, err := reg.ConsumeToken(tokAlpha); err != nil {
		t.Fatalf("consume host-alpha: %v", err)
	}
	restartReq := httptest.NewRequest("POST", "/api/v1/fleet/containers/app-web/restart", strings.NewReader(`{"server_id":"host-alpha"}`))
	restartReq.Header.Set("Content-Type", "application/json")
	restartReq.Header.Set("X-Pocketkafka-Principal", "admin")
	restartReq.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, restartReq)
	if rr2.Code != http.StatusAccepted {
		t.Fatalf("restart container failed: %d (%s)", rr2.Code, rr2.Body.String())
	}

	tok, err := reg.Enroll("host-to-revoke")
	if err != nil {
		t.Fatalf("enroll host: %v", err)
	}
	_, err = reg.ConsumeToken(tok)
	if err != nil {
		t.Fatalf("consume host: %v", err)
	}

	revokeReq := httptest.NewRequest("POST", "/api/v1/fleet/servers/host-to-revoke/revoke", nil)
	revokeReq.Header.Set("X-Pocketkafka-Principal", "admin")
	revokeReq.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, revokeReq)
	if rr3.Code != http.StatusOK {
		t.Fatalf("revoke server failed: %d (%s)", rr3.Code, rr3.Body.String())
	}

	events := audit.List()
	if len(events) < 3 {
		t.Fatalf("expected at least 3 audit events, got %d", len(events))
	}

	foundEnroll := false
	foundRestart := false
	foundRevoke := false

	for _, e := range events {
		if e.Actor != "admin" {
			t.Errorf("expected actor 'admin', got %q", e.Actor)
		}
		if strings.Contains(e.Action, "enroll") && e.Target == "host-alpha" {
			foundEnroll = true
		}
		if strings.Contains(e.Action, "restart") && e.Target == "app-web" {
			foundRestart = true
		}
		if strings.Contains(e.Action, "revoke") && e.Target == "host-to-revoke" {
			foundRevoke = true
		}
	}

	if !foundEnroll {
		t.Errorf("missing enroll audit event")
	}
	if !foundRestart {
		t.Errorf("missing restart audit event")
	}
	if !foundRevoke {
		t.Errorf("missing revoke audit event")
	}
}

func TestFleetServerPanicRecoveryAndBodyLimit(t *testing.T) {
	srv, _, _, _ := setupFleetTestServer(t, authz.AllowAllAuthorizer{})
	srv.mux.HandleFunc("GET /api/v1/fleet/panic-test", srv.wrap(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	handler := srv.Handler()

	req := httptest.NewRequest("GET", "/api/v1/fleet/panic-test", nil)
	req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("panic did not recover with 500, got %d", rr.Code)
	}

	largeBody := bytes.Repeat([]byte("A"), 2*1024*1024)
	postReq := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", bytes.NewReader(largeBody))
	postReq.Header.Set("Content-Type", "application/json")
	postReq.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, postReq)
	if rr2.Code != http.StatusRequestEntityTooLarge && rr2.Code != http.StatusBadRequest && rr2.Code != http.StatusInternalServerError {
		t.Fatalf("large body did not get rejected, got %d", rr2.Code)
	}
}

func TestFleetEnrollBypassesProxyToken(t *testing.T) {
	srv, reg, _, _ := setupFleetTestServer(t, authz.AllowAllAuthorizer{})
	tok, err := reg.Enroll("srv-1")
	if err != nil {
		t.Fatalf("enroll: %v", err)
	}
	body, _ := json.Marshal(map[string]string{"token": tok})
	req := httptest.NewRequest("POST", "/api/v1/fleet/enroll", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("enroll direct = %d, want 200 (body %s)", rr.Code, rr.Body.String())
	}
}
