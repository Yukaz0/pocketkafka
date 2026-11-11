package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/fleet"
)

func TestHTTPStreamTransportPoll(t *testing.T) {
	secret := "agent-secret-123"
	agentID := "agent-xyz"

	var pollHeaders http.Header
	var pollCount int32
	var pollMu sync.Mutex

	dispatchPayload, _ := json.Marshal(fleet.JobDispatchMessage{
		JobID:  "job-test-1",
		Action: "start",
		Target: "redis-test",
	})
	jobEnv, err := fleet.Sign(secret, fleet.DirectionControlToAgent, fleet.OpJobDispatch, dispatchPayload, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/fleet/agent/inbound" {
			pollMu.Lock()
			pollHeaders = r.Header.Clone()
			pollMu.Unlock()

			count := atomic.AddInt32(&pollCount, 1)
			if count == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(jobEnv)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := ts.Client()
	transport := NewHTTPStreamTransportWithConfig(HTTPStreamTransportConfig{
		Client:       client,
		ServerURL:    ts.URL,
		AgentID:      agentID,
		Secret:       secret,
		PollInterval: 20 * time.Millisecond,
	})
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	received, err := transport.Receive(ctx)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}
	if received.Op != fleet.OpJobDispatch {
		t.Fatalf("expected op %s, got %s", fleet.OpJobDispatch, received.Op)
	}

	pollMu.Lock()
	headerAgentID := pollHeaders.Get("X-Fleet-Agent-ID")
	headerHMAC := pollHeaders.Get("X-Fleet-HMAC")
	pollMu.Unlock()

	if headerAgentID != agentID {
		t.Fatalf("expected header agent_id %s, got %s", agentID, headerAgentID)
	}
	if headerHMAC == "" {
		t.Fatal("expected non-empty X-Fleet-HMAC header")
	}
}

func TestHTTPStreamTransportBackoffAndRecovery(t *testing.T) {
	secret := "secret-recover"
	agentID := "agent-recover"

	var attempts int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		att := atomic.AddInt32(&attempts, 1)
		if att < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("internal error"))
			return
		}

		env, _ := fleet.Sign(secret, fleet.DirectionControlToAgent, "test.ping", []byte(`{}`), time.Minute)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(env)
	}))
	defer ts.Close()

	client := ts.Client()
	transport := NewHTTPStreamTransportWithConfig(HTTPStreamTransportConfig{
		Client:       client,
		ServerURL:    ts.URL,
		AgentID:      agentID,
		Secret:       secret,
		PollInterval: 10 * time.Millisecond,
		MinBackoff:   15 * time.Millisecond,
	})
	defer transport.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	received, err := transport.Receive(ctx)
	if err != nil {
		t.Fatalf("receive after backoff recovery failed: %v", err)
	}
	if received.Op != "test.ping" {
		t.Fatalf("expected op test.ping, got %s", received.Op)
	}
}

func TestAgentRunnerWithHTTPStreamTransport(t *testing.T) {
	secret := "shared-secret-999"
	agentID := "ag-runner-test"
	serverID := "srv-runner-test"

	fakeDocker := fleet.NewFakeDocker()
	fakeDocker.AddContainer(&fleet.ContainerInspect{
		ID:    "target-cnt",
		Name:  "/target-cnt",
		Image: "redis:alpine",
		State: fleet.ContainerState{
			Status:  "exited",
			Running: false,
		},
	})

	var receivedOps []string
	var opsMu sync.Mutex
	doneCh := make(chan struct{})

	dispatchPayload, _ := json.Marshal(fleet.JobDispatchMessage{
		JobID:  "job-http-1",
		Action: "start",
		Target: "target-cnt",
	})
	jobEnv, err := fleet.Sign(secret, fleet.DirectionControlToAgent, fleet.OpJobDispatch, dispatchPayload, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	var pollCount int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/fleet/agent/envelope":
			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var env fleet.Envelope
			if err := json.Unmarshal(b, &env); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			opsMu.Lock()
			receivedOps = append(receivedOps, env.Op)
			if env.Op == fleet.OpJobResult {
				select {
				case <-doneCh:
				default:
					close(doneCh)
				}
			}
			opsMu.Unlock()
			w.WriteHeader(http.StatusOK)

		case "/api/v1/fleet/agent/inbound":
			pCount := atomic.AddInt32(&pollCount, 1)
			if pCount == 1 {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(jobEnv)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	client := ts.Client()
	transport := NewHTTPStreamTransportWithConfig(HTTPStreamTransportConfig{
		Client:       client,
		ServerURL:    ts.URL,
		AgentID:      agentID,
		Secret:       secret,
		PollInterval: 20 * time.Millisecond,
	})
	defer transport.Close()

	runner := fleet.NewAgentRunner(fleet.AgentRunnerConfig{
		AgentID:           agentID,
		ServerID:          serverID,
		Secret:            secret,
		HeartbeatInterval: 1 * time.Second,
		Docker:            fakeDocker,
		Transport:         transport,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	select {
	case <-doneCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for job completion via HTTPStreamTransport")
	}

	opsMu.Lock()
	defer opsMu.Unlock()

	hasHello := false
	hasAccepted := false
	hasRunning := false
	hasResult := false

	for _, op := range receivedOps {
		switch op {
		case fleet.OpHello:
			hasHello = true
		case fleet.OpJobAccepted:
			hasAccepted = true
		case fleet.OpJobRunning:
			hasRunning = true
		case fleet.OpJobResult:
			hasResult = true
		}
	}

	if !hasHello || !hasAccepted || !hasRunning || !hasResult {
		t.Fatalf("expected all ops (hello=%v, accepted=%v, running=%v, result=%v), got: %v",
			hasHello, hasAccepted, hasRunning, hasResult, receivedOps)
	}

	ins, err := fakeDocker.Inspect(ctx, "target-cnt")
	if err != nil {
		t.Fatal(err)
	}
	if !ins.State.Running {
		t.Fatal("expected container to be running")
	}
}
