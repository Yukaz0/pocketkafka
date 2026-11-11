package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestChannelTransportPair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	agentSide, controlSide := NewChannelTransportPair(16)
	defer agentSide.Close()
	defer controlSide.Close()

	env := &Envelope{
		ProtocolVersion: 1,
		MessageID:       "m-1",
		Direction:       DirectionAgentToControl,
		Op:              OpHello,
	}

	if err := agentSide.Send(env); err != nil {
		t.Fatal(err)
	}

	received, err := controlSide.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if received.MessageID != "m-1" || received.Op != OpHello {
		t.Fatalf("unexpected message: %+v", received)
	}
}

func TestAgentJobDispatchAndResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	secret := "test-secret-key"
	agentSide, controlSide := NewChannelTransportPair(16)
	defer agentSide.Close()
	defer controlSide.Close()

	fakeDocker := NewFakeDocker()
	fakeDocker.AddContainer(&ContainerInspect{
		ID:    "cnt-1",
		Name:  "/kafka-broker",
		Image: "pocketkafka:latest",
		State: ContainerState{Status: "exited", Running: false},
	})

	runner := NewAgentRunner(AgentRunnerConfig{
		AgentID:           "agent-1",
		ServerID:          "server-1",
		Secret:            secret,
		HeartbeatInterval: 50 * time.Millisecond,
		Docker:            fakeDocker,
		Transport:         agentSide,
	})

	go func() {
		_ = runner.Run(ctx)
	}()

	helloEnv, err := controlSide.Receive(ctx)
	if err != nil {
		t.Fatalf("receive hello failed: %v", err)
	}
	if helloEnv.Op != OpHello {
		t.Fatalf("expected hello op, got %s", helloEnv.Op)
	}

	dispatchPayload, _ := json.Marshal(JobDispatchMessage{
		JobID:  "job-100",
		Action: "start",
		Target: "cnt-1",
	})
	jobEnv, err := Sign(secret, DirectionControlToAgent, OpJobDispatch, dispatchPayload, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := controlSide.Send(jobEnv); err != nil {
		t.Fatal(err)
	}

	var gotAccepted, gotRunning, gotSucceeded bool
	deadline := time.After(2 * time.Second)

	for !gotSucceeded {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for job lifecycle completion")
		default:
		}

		respEnv, err := controlSide.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}

		switch respEnv.Op {
		case OpJobAccepted:
			var msg JobResultMessage
			if err := json.Unmarshal(respEnv.Payload, &msg); err == nil && msg.JobID == "job-100" {
				if msg.Status != JobStatusAccepted {
					t.Fatalf("expected status %s, got %s", JobStatusAccepted, msg.Status)
				}
				gotAccepted = true
			}
		case OpJobRunning:
			var msg JobResultMessage
			if err := json.Unmarshal(respEnv.Payload, &msg); err == nil && msg.JobID == "job-100" {
				gotRunning = true
			}
		case OpJobResult:
			var msg JobResultMessage
			if err := json.Unmarshal(respEnv.Payload, &msg); err == nil && msg.JobID == "job-100" {
				if msg.Status == JobStatusSucceeded {
					gotSucceeded = true
				} else {
					t.Fatalf("job failed: %s", msg.Error)
				}
			}
		}
	}

	if !gotAccepted || !gotRunning || !gotSucceeded {
		t.Fatalf("expected full lifecycle: accepted=%v, running=%v, succeeded=%v", gotAccepted, gotRunning, gotSucceeded)
	}

	ins, err := fakeDocker.Inspect(ctx, "cnt-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ins.State.Running {
		t.Fatal("expected container to be running")
	}
}

func TestAgentManagerTwoPointRevocation(t *testing.T) {
	ctx := context.Background()
	regDir := t.TempDir()
	jobsDir := t.TempDir()

	r, err := NewRegistry(regDir, "test-master-key-32-chars-long!")
	if err != nil {
		t.Fatal(err)
	}
	j := NewJobs(jobsDir)

	tok, err := r.Enroll("srv-1")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := r.ConsumeToken(tok)
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewAgentManager(r, j)
	agentSide, controlSide := NewChannelTransportPair(16)
	defer agentSide.Close()
	defer controlSide.Close()

	if _, err := mgr.RegisterSession(agent.ID, "agent-secret", controlSide); err != nil {
		t.Fatalf("session registration failed: %v", err)
	}

	jobID, err := j.Submit("srv-1", "restart", "cnt-1", "admin", "idem-key-1")
	if err != nil {
		t.Fatal(err)
	}
	job, err := j.Get(jobID)
	if err != nil {
		t.Fatal(err)
	}

	if err := mgr.DispatchJob(ctx, agent.ID, job); err != nil {
		t.Fatalf("expected dispatch to succeed before revoke: %v", err)
	}

	if err := r.Revoke(agent.ID); err != nil {
		t.Fatal(err)
	}

	jobID2, err := j.Submit("srv-1", "stop", "cnt-1", "admin", "idem-key-2")
	if err != nil {
		t.Fatal(err)
	}
	job2, err := j.Get(jobID2)
	if err != nil {
		t.Fatal(err)
	}

	err = mgr.DispatchJob(ctx, agent.ID, job2)
	if err == nil {
		t.Fatal("expected dispatch to fail on revoked agent (Point 2)")
	}
	if !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("expected revoked error, got: %v", err)
	}

	_, controlSide2 := NewChannelTransportPair(16)
	defer controlSide2.Close()
	_, err = mgr.RegisterSession(agent.ID, "agent-secret", controlSide2)
	if err == nil {
		t.Fatal("expected registration to fail on revoked agent (Point 1)")
	}
	if !strings.Contains(err.Error(), "revoked") {
		t.Fatalf("expected revoked error, got: %v", err)
	}
}

func TestComputeBackoffJitter(t *testing.T) {
	base := 100 * time.Millisecond
	maxBackoff := 2 * time.Second

	for i := range 10 {
		b := ComputeBackoff(i, base, maxBackoff)
		if b < base/2 {
			t.Fatalf("backoff %v below minimum", b)
		}
		if b > maxBackoff*13/10 {
			t.Fatalf("backoff %v exceeded maximum + jitter", b)
		}
	}
}
