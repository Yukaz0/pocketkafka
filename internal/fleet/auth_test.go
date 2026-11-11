package fleet

import (
	"strings"
	"testing"
	"time"
)

func SignedEnvelope(t testing.TB, direction, op string, payload []byte) *Envelope {
	t.Helper()
	env, err := Sign("key-1", direction, op, payload, time.Minute)
	if err != nil {
		t.Fatalf("Sign failed: %v", err)
	}
	return env
}

func TestHMACRejectsReplay(t *testing.T) {
	v := NewVerifier("key-1", time.Minute)
	env := SignedEnvelope(t, "agent→control", "job.result", []byte(`{}`))
	if err := v.Verify(env); err != nil {
		t.Fatal(err)
	}
	if err := v.Verify(env); err == nil {
		t.Fatal("pesan sama diulang harus ditolak")
	}
}

func TestRevokedMidStreamGetsNoJob(t *testing.T) {
	r, _ := NewRegistry(t.TempDir(), "test-master-key-32-chars-long!")
	tok, _ := r.Enroll("srv-1")
	agent, _ := r.ConsumeToken(tok)
	r.Revoke(agent.ID)
	if _, err := r.AuthorizeStream(agent.ID); err == nil {
		t.Fatal("stream terbuka agent tercabut tak boleh terima job")
	}
}

func TestHMACRejectsCrossEndpoint(t *testing.T) {
	v := NewVerifier("key-1", time.Minute)
	env := SignedEnvelope(t, "agent→control", "job.result", []byte(`{}`))
	env.Op = "server.delete"
	if err := v.Verify(env); err == nil {
		t.Fatal("op diubah setelah sign harus ditolak")
	}
}

func TestHMACRejectsExpired(t *testing.T) {
	v := NewVerifier("key-1", time.Minute)
	env, err := Sign("key-1", "agent→control", "heartbeat", []byte(`{}`), -time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := v.Verify(env); err == nil {
		t.Fatal("pesan expired harus ditolak")
	}
}

func TestHMACRejectsTamperedBody(t *testing.T) {
	v := NewVerifier("key-1", time.Minute)
	env := SignedEnvelope(t, "agent→control", "job.result", []byte(`{"status":"running"}`))
	env.Payload = []byte(`{"status":"succeeded"}`)
	if err := v.Verify(env); err == nil {
		t.Fatal("payload diubah harus ditolak")
	}
}

func TestRevokedConnectRejected(t *testing.T) {
	r, err := NewRegistry(t.TempDir(), "test-master-key-32-chars-long!")
	if err != nil {
		t.Fatal(err)
	}
	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Revoke(agent.ID); err != nil {
		t.Fatal(err)
	}

	v := NewVerifier("key-1", time.Minute).WithRegistry(r)
	env := SignedEnvelope(t, "agent→control", "hello", []byte(`{}`))
	env.AgentID = agent.ID
	env.HMAC = ComputeHMAC("key-1", env.Direction, env.Op, env.BodySHA256, env.IssuedAt, env.MessageID)

	if err := v.Verify(env); err == nil {
		t.Fatal("agent yang di-revoke harus ditolak saat connect")
	} else if !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("expected revoked error, got: %v", err)
	}
}
