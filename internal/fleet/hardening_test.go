package fleet

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
)

func TestFleetConsistentErrorEnvelope(t *testing.T) {
	srv, _, _, _ := setupFleetTestServer(t, authz.AllowAllAuthorizer{})
	handler := srv.Handler()

	req1 := httptest.NewRequest("GET", "/api/v1/fleet/servers/non-existent-host", nil)
	req1.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	req1.Header.Set("X-Request-ID", "req-custom-123")
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr1.Code)
	}
	if reqID := rr1.Header().Get("X-Request-ID"); reqID != "req-custom-123" {
		t.Fatalf("expected X-Request-ID header %q, got %q", "req-custom-123", reqID)
	}

	var errResp1 ErrorResponse
	if err := json.Unmarshal(rr1.Body.Bytes(), &errResp1); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp1.Code != ErrCodeNotFound {
		t.Fatalf("expected code %s, got %s", ErrCodeNotFound, errResp1.Code)
	}
	if errResp1.RequestID != "req-custom-123" {
		t.Fatalf("expected request_id %q, got %q", "req-custom-123", errResp1.RequestID)
	}
	if errResp1.Message == "" || errResp1.ErrorText == "" {
		t.Fatalf("expected non-empty message and errorText, got msg=%q errText=%q", errResp1.Message, errResp1.ErrorText)
	}

	req2 := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", nil)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr2.Code)
	}
	var errResp2 ErrorResponse
	if err := json.Unmarshal(rr2.Body.Bytes(), &errResp2); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp2.Code != ErrCodeBadRequest {
		t.Fatalf("expected code %s, got %s", ErrCodeBadRequest, errResp2.Code)
	}
	if errResp2.RequestID == "" {
		t.Fatalf("expected auto-generated request_id, got empty")
	}

	req3 := httptest.NewRequest("GET", "/api/v1/fleet/servers", nil)
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, req3)

	if rr3.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rr3.Code)
	}
	var errResp3 ErrorResponse
	if err := json.Unmarshal(rr3.Body.Bytes(), &errResp3); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp3.Code != ErrCodeForbidden {
		t.Fatalf("expected code %s, got %s", ErrCodeForbidden, errResp3.Code)
	}

	dis := NewDisabledHandler()
	req4 := httptest.NewRequest("GET", "/api/v1/fleet/servers", nil)
	rr4 := httptest.NewRecorder()
	dis.ServeHTTP(rr4, req4)
	if rr4.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr4.Code)
	}
	var errResp4 ErrorResponse
	if err := json.Unmarshal(rr4.Body.Bytes(), &errResp4); err != nil {
		t.Fatalf("unmarshal disabled error response: %v", err)
	}
	if errResp4.Code != ErrCodeNotFound || errResp4.Message != "fleet disabled" {
		t.Fatalf("unexpected disabled envelope: %+v", errResp4)
	}
}

func TestFleetRateLimits(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewRegistry(dir, "1234567890123456")
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	jobs := NewJobs(dir)
	audit := NewAudit(dir)
	mgr := NewAgentManager(reg, jobs)

	srv := NewServer(ServerConfig{
		Registry:        reg,
		Jobs:            jobs,
		Audit:           audit,
		Manager:         mgr,
		Authorizer:      authz.AllowAllAuthorizer{},
		ProxyToken:      "test-proxy-token",
		EnrollRateLimit: 2,
		OpRateLimit:     2,
	})
	handler := srv.Handler()

	for i := range 2 {
		req := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", strings.NewReader(`{"server_id":"srv-limit"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
		req.RemoteAddr = "192.0.2.1:12345"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d (body: %s)", i+1, rr.Code, rr.Body.String())
		}
	}

	req3 := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", strings.NewReader(`{"server_id":"srv-limit"}`))
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	req3.RemoteAddr = "192.0.2.1:12345"
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, req3)

	if rr3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rr3.Code)
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(rr3.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal rate limit error response: %v", err)
	}
	if errResp.Code != ErrCodeRateLimited {
		t.Fatalf("expected code %s, got %s", ErrCodeRateLimited, errResp.Code)
	}

	reqDiffIP := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", strings.NewReader(`{"server_id":"srv-limit"}`))
	reqDiffIP.Header.Set("Content-Type", "application/json")
	reqDiffIP.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	reqDiffIP.RemoteAddr = "192.0.2.2:12345"
	rrDiff := httptest.NewRecorder()
	handler.ServeHTTP(rrDiff, reqDiffIP)
	if rrDiff.Code != http.StatusOK {
		t.Fatalf("request from different IP should succeed: got %d", rrDiff.Code)
	}

	for i := range 2 {
		req := httptest.NewRequest("GET", "/api/v1/fleet/containers/c1/logs", nil)
		req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
		req.Header.Set("X-Pocketkafka-Principal", "operator-1")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("op request %d: expected 200, got %d", i+1, rr.Code)
		}
	}

	reqOp3 := httptest.NewRequest("GET", "/api/v1/fleet/containers/c1/logs", nil)
	reqOp3.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	reqOp3.Header.Set("X-Pocketkafka-Principal", "operator-1")
	rrOp3 := httptest.NewRecorder()
	handler.ServeHTTP(rrOp3, reqOp3)

	if rrOp3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for op rate limit, got %d", rrOp3.Code)
	}
}

func TestFleetResourceLimits(t *testing.T) {
	dir := t.TempDir()
	reg, err := NewRegistry(dir, "1234567890123456")
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	jobs := NewJobs(dir)
	audit := NewAudit(dir)
	mgr := NewAgentManager(reg, jobs)

	srv := NewServer(ServerConfig{
		Registry:        reg,
		Jobs:            jobs,
		Audit:           audit,
		Manager:         mgr,
		Authorizer:      authz.AllowAllAuthorizer{},
		ProxyToken:      "test-proxy-token",
		MaxActiveJobs:   2,
		MaxAgents:       2,
		EnrollRateLimit: 100,
		OpRateLimit:     100,
	})
	handler := srv.Handler()

	tok1, _ := reg.Enroll("srv-1")
	ag1, err := reg.ConsumeToken(tok1)
	if err != nil {
		t.Fatalf("consume 1: %v", err)
	}

	tok2, _ := reg.Enroll("srv-2")
	_, err = reg.ConsumeToken(tok2)
	if err != nil {
		t.Fatalf("consume 2: %v", err)
	}

	_, controlSide1 := NewChannelTransportPair(16)
	if _, err := mgr.RegisterSession(ag1.ID, ag1.RawSecret, controlSide1); err != nil {
		t.Fatalf("register session 1: %v", err)
	}

	reqAgent3 := httptest.NewRequest("POST", "/api/v1/fleet/enrollments", strings.NewReader(`{"server_id":"srv-3"}`))
	reqAgent3.Header.Set("Content-Type", "application/json")
	reqAgent3.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rrAgent3 := httptest.NewRecorder()
	handler.ServeHTTP(rrAgent3, reqAgent3)

	if rrAgent3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when max agents reached, got %d (body: %s)", rrAgent3.Code, rrAgent3.Body.String())
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(rrAgent3.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if errResp.Code != ErrCodeLimitExceeded {
		t.Fatalf("expected %s, got %s", ErrCodeLimitExceeded, errResp.Code)
	}

	reqJob1 := httptest.NewRequest("POST", "/api/v1/fleet/containers/c1/restart", strings.NewReader(`{"server_id":"srv-1"}`))
	reqJob1.Header.Set("Content-Type", "application/json")
	reqJob1.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rrJob1 := httptest.NewRecorder()
	handler.ServeHTTP(rrJob1, reqJob1)
	if rrJob1.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rrJob1.Code)
	}

	reqJob2 := httptest.NewRequest("POST", "/api/v1/fleet/containers/c2/restart", strings.NewReader(`{"server_id":"srv-1"}`))
	reqJob2.Header.Set("Content-Type", "application/json")
	reqJob2.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rrJob2 := httptest.NewRecorder()
	handler.ServeHTTP(rrJob2, reqJob2)
	if rrJob2.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rrJob2.Code)
	}

	reqJob3 := httptest.NewRequest("POST", "/api/v1/fleet/containers/c3/restart", strings.NewReader(`{"server_id":"srv-1"}`))
	reqJob3.Header.Set("Content-Type", "application/json")
	reqJob3.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rrJob3 := httptest.NewRecorder()
	handler.ServeHTTP(rrJob3, reqJob3)

	if rrJob3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 when max active jobs reached, got %d (body: %s)", rrJob3.Code, rrJob3.Body.String())
	}
	var errRespJob ErrorResponse
	if err := json.Unmarshal(rrJob3.Body.Bytes(), &errRespJob); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if errRespJob.Code != ErrCodeLimitExceeded {
		t.Fatalf("expected %s, got %s", ErrCodeLimitExceeded, errRespJob.Code)
	}
}

func TestFleetConnectionLimit(t *testing.T) {
	dir := t.TempDir()
	reg, _ := NewRegistry(dir, "1234567890123456")
	jobs := NewJobs(dir)
	audit := NewAudit(dir)
	mgr := NewAgentManager(reg, jobs)

	srv := NewServer(ServerConfig{
		Registry:       reg,
		Jobs:           jobs,
		Audit:          audit,
		Manager:        mgr,
		Authorizer:     authz.AllowAllAuthorizer{},
		ProxyToken:     "test-proxy-token",
		MaxConnections: 1,
	})

	blockCh := make(chan struct{})
	doneCh := make(chan struct{})

	srv.mux.HandleFunc("GET /api/v1/fleet/block", srv.wrap(func(w http.ResponseWriter, r *http.Request) {
		close(doneCh)
		<-blockCh
		w.WriteHeader(http.StatusOK)
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest("GET", "/api/v1/fleet/block", nil)
		req.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first request to block")
	}

	req2 := httptest.NewRequest("GET", "/api/v1/fleet/status", nil)
	req2.Header.Set("X-Pocketkafka-Fleet-Proxy", "test-proxy-token")
	rr2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for connection limit, got %d", rr2.Code)
	}
	var errResp ErrorResponse
	if err := json.Unmarshal(rr2.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if errResp.Code != ErrCodeConnectionLimitExceeded {
		t.Fatalf("expected code %s, got %s", ErrCodeConnectionLimitExceeded, errResp.Code)
	}

	close(blockCh)
	wg.Wait()
}
