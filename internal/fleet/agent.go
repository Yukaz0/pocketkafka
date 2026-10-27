package fleet

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	OpHello        = "hello"
	OpHelloAck     = "hello.ack"
	OpHeartbeat    = "heartbeat"
	OpHeartbeatAck = "heartbeat.ack"
	OpJobDispatch  = "job.dispatch"
	OpJobAccepted  = "job.accepted"
	OpJobRunning   = "job.running"
	OpJobResult    = "job.result"
)

type HelloMessage struct {
	AgentID  string `json:"agent_id"`
	ServerID string `json:"server_id"`
	Hostname string `json:"hostname,omitempty"`
	OS       string `json:"os,omitempty"`
	Version  string `json:"version,omitempty"`
}

type HeartbeatMessage struct {
	AgentID    string             `json:"agent_id"`
	Timestamp  int64              `json:"timestamp"`
	Containers []ContainerSummary `json:"containers,omitempty"`
}

type JobDispatchMessage struct {
	JobID          string `json:"job_id"`
	Action         string `json:"action"`
	Target         string `json:"target"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	TailLines      int    `json:"tail_lines,omitempty"`
	Force          bool   `json:"force,omitempty"`
}

type JobResultMessage struct {
	JobID  string    `json:"job_id"`
	Status JobStatus `json:"status"`
	Result string    `json:"result,omitempty"`
	Error  string    `json:"error,omitempty"`
}

type AgentTransport interface {
	Send(env *Envelope) error
	Receive(ctx context.Context) (*Envelope, error)
	Close() error
}

type ChannelTransport struct {
	inbound  chan *Envelope
	outbound chan *Envelope
	closed   atomic.Bool
	closeCh  chan struct{}
}

func NewChannelTransportPair(bufSize int) (*ChannelTransport, *ChannelTransport) {
	if bufSize <= 0 {
		bufSize = 64
	}
	chAtoB := make(chan *Envelope, bufSize)
	chBtoA := make(chan *Envelope, bufSize)
	closeChA := make(chan struct{})
	closeChB := make(chan struct{})

	sideA := &ChannelTransport{
		inbound:  chBtoA,
		outbound: chAtoB,
		closeCh:  closeChA,
	}
	sideB := &ChannelTransport{
		inbound:  chAtoB,
		outbound: chBtoA,
		closeCh:  closeChB,
	}
	return sideA, sideB
}

func (c *ChannelTransport) Send(env *Envelope) error {
	if c.closed.Load() {
		return errors.New("fleet: transport closed")
	}
	select {
	case c.outbound <- env:
		return nil
	case <-c.closeCh:
		return errors.New("fleet: transport closed")
	}
}

func (c *ChannelTransport) Receive(ctx context.Context) (*Envelope, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closeCh:
		return nil, errors.New("fleet: transport closed")
	case env, ok := <-c.inbound:
		if !ok {
			return nil, ioEOFOrClosed()
		}
		return env, nil
	}
}

func (c *ChannelTransport) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		close(c.closeCh)
	}
	return nil
}

func ioEOFOrClosed() error {
	return errors.New("fleet: transport closed")
}

func ComputeBackoff(attempt int, base, maxBackoff time.Duration) time.Duration {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	if attempt < 0 {
		attempt = 0
	}
	mult := 1 << attempt
	if mult <= 0 || mult > 1024 {
		mult = 1024
	}
	dur := base * time.Duration(mult)
	if dur > maxBackoff || dur <= 0 {
		dur = maxBackoff
	}

	var b [2]byte
	_, _ = rand.Read(b[:])
	jitterFrac := (float64(binary.BigEndian.Uint16(b[:]))/65535.0)*0.4 - 0.2
	jitter := time.Duration(float64(dur) * jitterFrac)
	result := dur + jitter
	if result < base/2 {
		result = base / 2
	}
	return result
}

type AgentRunnerConfig struct {
	AgentID           string
	ServerID          string
	Secret            string
	HeartbeatInterval time.Duration
	Docker            DockerClient
	Transport         AgentTransport
}

type AgentRunner struct {
	cfg      AgentRunnerConfig
	verifier *Verifier
}

func NewAgentRunner(cfg AgentRunnerConfig) *AgentRunner {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}
	return &AgentRunner{
		cfg:      cfg,
		verifier: NewVerifier(cfg.Secret, time.Minute, DirectionControlToAgent),
	}
}

func (a *AgentRunner) Run(ctx context.Context) error {
	if a.cfg.Transport == nil {
		return errors.New("fleet: agent transport cannot be nil")
	}

	hostname, _ := os.Hostname()
	helloPayload, err := json.Marshal(HelloMessage{
		AgentID:  a.cfg.AgentID,
		ServerID: a.cfg.ServerID,
		Hostname: hostname,
		OS:       runtime.GOOS,
		Version:  "1.0.0",
	})
	if err != nil {
		return err
	}

	helloEnv, err := Sign(a.cfg.Secret, DirectionAgentToControl, OpHello, helloPayload, time.Minute)
	if err != nil {
		return err
	}
	helloEnv.AgentID = a.cfg.AgentID
	helloEnv.HMAC = ComputeHMAC(a.cfg.Secret, helloEnv.Direction, helloEnv.Op, helloEnv.BodySHA256, helloEnv.IssuedAt, helloEnv.MessageID)

	if err := a.cfg.Transport.Send(helloEnv); err != nil {
		return fmt.Errorf("fleet: send hello failed: %w", err)
	}

	hbTicker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer hbTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hbTicker.C:
				a.sendHeartbeat(ctx)
			}
		}
	}()

	for {
		env, err := a.cfg.Transport.Receive(ctx)
		if err != nil {
			return err
		}

		if err := a.verifier.Verify(env); err != nil {
			continue
		}

		switch env.Op {
		case OpJobDispatch:
			var dispatch JobDispatchMessage
			if err := json.Unmarshal(env.Payload, &dispatch); err != nil {
				continue
			}
			go a.executeJob(ctx, dispatch)
		}
	}
}

func (a *AgentRunner) sendHeartbeat(ctx context.Context) {
	var containers []ContainerSummary
	if a.cfg.Docker != nil {
		if cList, err := a.cfg.Docker.List(ctx); err == nil {
			containers = cList
		}
	}
	hbPayload, err := json.Marshal(HeartbeatMessage{
		AgentID:    a.cfg.AgentID,
		Timestamp:  time.Now().Unix(),
		Containers: containers,
	})
	if err != nil {
		return
	}
	hbEnv, err := Sign(a.cfg.Secret, DirectionAgentToControl, OpHeartbeat, hbPayload, time.Minute)
	if err != nil {
		return
	}
	hbEnv.AgentID = a.cfg.AgentID
	hbEnv.HMAC = ComputeHMAC(a.cfg.Secret, hbEnv.Direction, hbEnv.Op, hbEnv.BodySHA256, hbEnv.IssuedAt, hbEnv.MessageID)
	_ = a.cfg.Transport.Send(hbEnv)
}

func (a *AgentRunner) executeJob(ctx context.Context, job JobDispatchMessage) {
	a.sendJobResult(JobResultMessage{
		JobID:  job.JobID,
		Status: JobStatusAccepted,
	}, OpJobAccepted)

	a.sendJobResult(JobResultMessage{
		JobID:  job.JobID,
		Status: JobStatusRunning,
	}, OpJobRunning)

	if a.cfg.Docker == nil {
		a.sendJobResult(JobResultMessage{
			JobID:  job.JobID,
			Status: JobStatusFailed,
			Error:  "docker client not configured",
		}, OpJobResult)
		return
	}

	var result string
	var execErr error

	timeout := job.TimeoutSeconds
	if timeout <= 0 {
		timeout = 10
	}

	switch job.Action {
	case "start":
		execErr = a.cfg.Docker.Start(ctx, job.Target)
		result = fmt.Sprintf("container %s started", job.Target)
	case "stop":
		execErr = a.cfg.Docker.Stop(ctx, job.Target, timeout)
		result = fmt.Sprintf("container %s stopped", job.Target)
	case "restart":
		execErr = a.cfg.Docker.Restart(ctx, job.Target, timeout)
		result = fmt.Sprintf("container %s restarted", job.Target)
	case "remove":
		force := job.Force
		execErr = a.cfg.Docker.Remove(ctx, job.Target, force)
		result = fmt.Sprintf("container %s removed", job.Target)
	case "inspect":
		ins, err := a.cfg.Docker.Inspect(ctx, job.Target)
		if err != nil {
			execErr = err
		} else {
			raw, _ := json.Marshal(ins)
			result = string(raw)
		}
	case "logs":
		tail := job.TailLines
		if tail <= 0 {
			tail = 100
		}
		result, execErr = a.cfg.Docker.Logs(ctx, job.Target, tail)
	default:
		execErr = fmt.Errorf("unknown action: %s", job.Action)
	}

	a.sendHeartbeat(ctx)

	if execErr != nil {
		a.sendJobResult(JobResultMessage{
			JobID:  job.JobID,
			Status: JobStatusFailed,
			Error:  execErr.Error(),
		}, OpJobResult)
	} else {
		a.sendJobResult(JobResultMessage{
			JobID:  job.JobID,
			Status: JobStatusSucceeded,
			Result: result,
		}, OpJobResult)
	}
}
func (a *AgentRunner) sendJobResult(res JobResultMessage, op string) {
	payload, err := json.Marshal(res)
	if err != nil {
		return
	}
	env, err := Sign(a.cfg.Secret, DirectionAgentToControl, op, payload, time.Minute)
	if err != nil {
		return
	}
	env.AgentID = a.cfg.AgentID
	env.HMAC = ComputeHMAC(a.cfg.Secret, env.Direction, env.Op, env.BodySHA256, env.IssuedAt, env.MessageID)
	_ = a.cfg.Transport.Send(env)
}

type AgentManager struct {
	registry *Registry
	jobs     *Jobs

	mu       sync.RWMutex
	sessions map[string]*ControlSession
}

type ControlSession struct {
	agentID       string
	serverID      string
	transport     AgentTransport
	verifier      *Verifier
	secret        string
	containers    []ContainerSummary
	lastHeartbeat time.Time
}

func NewAgentManager(registry *Registry, jobs *Jobs) *AgentManager {
	return &AgentManager{
		registry: registry,
		jobs:     jobs,
		sessions: make(map[string]*ControlSession),
	}
}

func (m *AgentManager) RegisterSession(agentID, secret string, transport AgentTransport) (*ControlSession, error) {
	var serverID string
	if m.registry != nil {
		ag, err := m.registry.AuthorizeStream(agentID)
		if err != nil {
			return nil, fmt.Errorf("fleet: connection rejected: %w", err)
		}
		serverID = ag.ServerID
	}

	if secret == "" && m.registry != nil {
		sec, err := m.registry.agentKey(agentID)
		if err != nil {
			return nil, fmt.Errorf("fleet: decrypt agent key: %w", err)
		}
		secret = sec
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	sess := &ControlSession{
		agentID:   agentID,
		serverID:  serverID,
		transport: transport,
		verifier:  NewVerifier(secret, time.Minute, DirectionAgentToControl).WithRegistry(m.registry),
		secret:    secret,
	}
	m.sessions[agentID] = sess
	return sess, nil
}

func (m *AgentManager) UnregisterServerSessions(serverID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sess := range m.sessions {
		if sess.serverID == serverID {
			delete(m.sessions, id)
		}
	}
}

func (m *AgentManager) UnregisterSession(agentID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, agentID)
}

func (m *AgentManager) ListContainers() []ContainerSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var out []ContainerSummary
	for _, sess := range m.sessions {
		for _, c := range sess.containers {
			c.ServerID = sess.serverID
			out = append(out, c)
		}
	}
	return out
}

func (m *AgentManager) DispatchJob(ctx context.Context, agentID string, job *Job) error {
	if m.registry != nil {
		if _, err := m.registry.AuthorizeStream(agentID); err != nil {
			return fmt.Errorf("fleet: stream revoked: %w", err)
		}
	}

	m.mu.RLock()
	sess, ok := m.sessions[agentID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("fleet: no active session for agent %q", agentID)
	}

	payload, err := json.Marshal(JobDispatchMessage{
		JobID:     job.ID,
		Action:    job.Action,
		Target:    job.Target,
		Force:     job.Force,
		TailLines: job.TailLines,
	})
	if err != nil {
		return err
	}

	env, err := Sign(sess.secret, DirectionControlToAgent, OpJobDispatch, payload, time.Minute)
	if err != nil {
		return err
	}
	env.AgentID = agentID
	env.HMAC = ComputeHMAC(sess.secret, env.Direction, env.Op, env.BodySHA256, env.IssuedAt, env.MessageID)

	return sess.transport.Send(env)
}

func (m *AgentManager) HandleInboundEnvelope(env *Envelope) error {
	m.mu.RLock()
	sess, ok := m.sessions[env.AgentID]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("fleet: session not found for agent %q", env.AgentID)
	}

	if err := sess.verifier.Verify(env); err != nil {
		return err
	}

	switch env.Op {
	case OpHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(env.Payload, &hb); err == nil {
			m.mu.Lock()
			sess.containers = hb.Containers
			sess.lastHeartbeat = time.Now()
			serverID := sess.serverID
			m.mu.Unlock()
			if m.registry != nil && serverID != "" {
				m.registry.TouchServer(serverID)
			}
		}
	case OpJobAccepted:

	case OpJobRunning:
		if m.jobs != nil {
			var res JobResultMessage
			if err := json.Unmarshal(env.Payload, &res); err == nil {
				if job, err := m.jobs.Get(res.JobID); err == nil && job != nil && job.ServerID == sess.serverID {
					_ = m.jobs.Start(res.JobID)
				}
			}
		}
	case OpJobResult:
		if m.jobs != nil {
			var res JobResultMessage
			if err := json.Unmarshal(env.Payload, &res); err == nil {
				if job, err := m.jobs.Get(res.JobID); err == nil && job != nil && job.ServerID == sess.serverID {
					if res.Status == JobStatusSucceeded {
						_ = m.jobs.SetResult(res.JobID, res.Result)
						_ = m.jobs.Complete(res.JobID, JobStatusSucceeded)
					} else if res.Status == JobStatusFailed {
						_ = m.jobs.Complete(res.JobID, JobStatusFailed, res.Error)
					}
				}
			}
		}
	}
	return nil
}
