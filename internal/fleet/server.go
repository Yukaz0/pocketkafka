package fleet

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
)

// ProxyPrincipalHeader carries the authenticated principal from the dashboard
// reverse proxy to the fleet listener. It must never be trusted from a direct
// client connection: the proxy strips any client-sent value and sets it only
// after session authentication, alongside ProxyTokenHeader.
const ProxyPrincipalHeader = "X-Pocketkafka-Principal"

// ProxyTokenHeader is a per-process random token shared only between the
// dashboard proxy and the fleet listener. Direct requests without it are
// rejected even on loopback, so a client that reaches the fleet port
// cannot forge a principal header.
const ProxyTokenHeader = "X-Pocketkafka-Fleet-Proxy"

// NewProxyToken generates a per-process token shared between the dashboard
func NewProxyToken() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("fleet: cannot generate proxy token: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// ServerConfig holds dependencies for FleetServer.
type ServerConfig struct {
	Registry     *Registry
	Jobs         *Jobs
	Audit        *Audit
	Manager      *AgentManager
	Authorizer   authz.Authorizer
	MaxBodyBytes int64
	// ProxyToken authenticates the dashboard reverse proxy to this listener.
	// Generated per process; direct requests without it are rejected.
	ProxyToken string
}
type FleetServer struct {
	mu           sync.RWMutex
	mux          *http.ServeMux
	registry     *Registry
	jobs         *Jobs
	audit        *Audit
	manager      *AgentManager
	authorizer   authz.Authorizer
	maxBodyBytes int64
	proxyToken   string

	agentChannels map[string]*ChannelTransport
}

// NewServer constructs a new FleetServer with isolated mux and configured routes.
func NewServer(cfg ServerConfig) *FleetServer {
	return NewFleetServer(cfg)
}

// NewFleetServer constructs a new FleetServer with isolated mux and configured routes.
func NewFleetServer(cfg ServerConfig) *FleetServer {
	maxBytes := cfg.MaxBodyBytes
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024 // 2MB default
	}

	s := &FleetServer{
		mux:           http.NewServeMux(),
		registry:      cfg.Registry,
		jobs:          cfg.Jobs,
		audit:         cfg.Audit,
		manager:       cfg.Manager,
		authorizer:    cfg.Authorizer,
		maxBodyBytes:  maxBytes,
		proxyToken:    cfg.ProxyToken,
		agentChannels: make(map[string]*ChannelTransport),
	}

	s.routes()
	return s
}

// NewDisabledHandler returns an http.Handler that responds with 404 Not Found to indicate fleet is disabled.
func NewDisabledHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"fleet disabled"}`))
	})
}

// Handler returns the HTTP handler for the fleet server mux.
func (s *FleetServer) Handler() http.Handler {
	return s.mux
}

// ServeHTTP satisfies http.Handler.
func (s *FleetServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *FleetServer) wrap(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("fleet: panic recovered", "panic", rec, "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal server error"}`))
			}
		}()
		if s.maxBodyBytes > 0 && r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
		}
		// Agent transport endpoints authenticate via HMAC/tokens, not the proxy.
		// POST /enroll carries the single-use enrollment token as its credential.
		if !strings.HasPrefix(r.URL.Path, "/api/v1/fleet/agent/") && r.URL.Path != "/api/v1/fleet/enroll" {
			if s.proxyToken == "" || !validProxyToken(r, s.proxyToken) {
				slog.Error("fleet: rejected direct request without proxy token", "path", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":"forbidden"}`))
				return
			}
		}
		h(w, r)
	}
}

func (s *FleetServer) principalFromRequest(r *http.Request) string {
	if p := r.Header.Get(ProxyPrincipalHeader); p != "" {
		return strings.TrimSpace(p)
	}
	return authz.AnonymPrincipal
}

func validProxyToken(r *http.Request, want string) bool {
	got := r.Header.Get(ProxyTokenHeader)
	if got == "" || want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (s *FleetServer) checkAuthz(w http.ResponseWriter, r *http.Request, op authz.Operation, resType, resName string) (string, bool) {
	principal := s.principalFromRequest(r)
	if s.authorizer == nil {
		slog.Error("fleet: authorizer missing, denying request", "path", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "forbidden: authorizer not configured"})
		return principal, false
	}
	res := authz.Resource{Type: resType, Name: resName}
	if err := s.authorizer.Authorize(principal, op, res); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "forbidden: " + err.Error()})
		return principal, false
	}
	return principal, true
}

func (s *FleetServer) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *FleetServer) readJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if r.Body == nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty body"})
		return false
	}
	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
			return false
		}
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body: " + err.Error()})
		return false
	}
	return true
}

func (s *FleetServer) routes() {
	s.mux.HandleFunc("GET /api/v1/fleet/status", s.wrap(s.handleStatus))
	s.mux.HandleFunc("GET /api/v1/fleet/servers", s.wrap(s.handleListServers))
	s.mux.HandleFunc("GET /api/v1/fleet/servers/{id}", s.wrap(s.handleGetServer))
	s.mux.HandleFunc("POST /api/v1/fleet/servers/{id}/revoke", s.wrap(s.handleRevokeServer))
	s.mux.HandleFunc("DELETE /api/v1/fleet/servers/{id}", s.wrap(s.handleRevokeServer))

	s.mux.HandleFunc("GET /api/v1/fleet/enrollments", s.wrap(s.handleListEnrollments))
	s.mux.HandleFunc("POST /api/v1/fleet/enrollments", s.wrap(s.handleCreateEnrollment))
	s.mux.HandleFunc("POST /api/v1/fleet/enroll", s.wrap(s.handleAgentEnroll))

	s.mux.HandleFunc("GET /api/v1/fleet/containers", s.wrap(s.handleListContainers))
	s.mux.HandleFunc("POST /api/v1/fleet/containers/{id}/start", s.wrap(s.handleContainerAction("start")))
	s.mux.HandleFunc("POST /api/v1/fleet/containers/{id}/stop", s.wrap(s.handleContainerAction("stop")))
	s.mux.HandleFunc("POST /api/v1/fleet/containers/{id}/restart", s.wrap(s.handleContainerAction("restart")))
	s.mux.HandleFunc("GET /api/v1/fleet/containers/{id}/logs", s.wrap(s.handleContainerLogs))
	s.mux.HandleFunc("GET /api/v1/fleet/logs", s.wrap(s.handleContainerLogs))

	s.mux.HandleFunc("GET /api/v1/fleet/jobs", s.wrap(s.handleListJobs))
	s.mux.HandleFunc("GET /api/v1/fleet/jobs/{id}", s.wrap(s.handleGetJob))

	s.mux.HandleFunc("GET /api/v1/fleet/audit-events", s.wrap(s.handleListAudit))
	s.mux.HandleFunc("GET /api/v1/fleet/audit", s.wrap(s.handleListAudit))

	s.mux.HandleFunc("GET /api/v1/fleet/agent/inbound", s.wrap(s.handleAgentInbound))
	s.mux.HandleFunc("POST /api/v1/fleet/agent/envelope", s.wrap(s.handleAgentEnvelope))
}

func (s *FleetServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetServer, "*"); !ok {
		return
	}

	servers := 0
	agents := 0
	if s.registry != nil {
		servers = len(s.registry.ListServers())
		agents = len(s.registry.ListAgents())
	}

	activeJobs := 0
	if s.jobs != nil {
		for _, j := range s.jobs.List() {
			if j.Status == JobStatusQueued || j.Status == JobStatusAccepted || j.Status == JobStatusRunning {
				activeJobs++
			}
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":      "ok",
		"enabled":     true,
		"servers":     servers,
		"agents":      agents,
		"active_jobs": activeJobs,
	})
}

func (s *FleetServer) handleListServers(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetServer, "*"); !ok {
		return
	}

	var servers []*Server
	if s.registry != nil {
		servers = s.registry.ListServers()
	}
	if servers == nil {
		servers = []*Server{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"servers": servers,
	})
}

func (s *FleetServer) handleGetServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetServer, id); !ok {
		return
	}

	if s.registry == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "server not found"})
		return
	}

	srv, ok := s.registry.GetServer(id)
	if !ok {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "server not found"})
		return
	}
	s.writeJSON(w, http.StatusOK, srv)
}

func (s *FleetServer) handleRevokeServer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	principal, ok := s.checkAuthz(w, r, authz.OpAdmin, authz.ResourceFleetServer, id)
	if !ok {
		return
	}

	if s.registry == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "registry not configured"})
		return
	}

	err := s.registry.RevokeServer(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if s.audit != nil {
		s.audit.Log(AuditEvent{
			Actor:  principal,
			Target: id,
			Action: "server.revoke",
			Time:   time.Now(),
			Result: "success",
			Detail: "server revoked by admin",
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":    "revoked",
		"server_id": id,
	})
}

func (s *FleetServer) handleListEnrollments(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetServer, "*"); !ok {
		return
	}

	var list []*Enrollment
	if s.registry != nil {
		list = s.registry.ListEnrollments()
	}
	if list == nil {
		list = []*Enrollment{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"enrollments": list,
	})
}

func (s *FleetServer) handleCreateEnrollment(w http.ResponseWriter, r *http.Request) {
	principal, ok := s.checkAuthz(w, r, authz.OpAdmin, authz.ResourceFleetServer, "*")
	if !ok {
		return
	}

	var req struct {
		ServerID string `json:"server_id"`
		Duration string `json:"duration,omitempty"`
	}
	if !s.readJSON(w, r, &req) {
		return
	}

	if strings.TrimSpace(req.ServerID) == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "server_id is required"})
		return
	}

	if s.registry == nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "registry not configured"})
		return
	}

	dur := 15 * time.Minute
	if req.Duration != "" {
		if parsed, err := time.ParseDuration(req.Duration); err == nil && parsed > 0 {
			dur = parsed
		}
	}

	tok, err := s.registry.enrollWithExpiry(req.ServerID, dur)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if s.audit != nil {
		s.audit.Log(AuditEvent{
			Actor:  principal,
			Target: req.ServerID,
			Action: "enrollment.create",
			Time:   time.Now(),
			Result: "success",
			Detail: "token issued",
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":      tok,
		"server_id":  req.ServerID,
		"expires_in": dur.String(),
	})
}

func (s *FleetServer) handleAgentEnroll(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if !s.readJSON(w, r, &req) {
		return
	}

	if s.registry == nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "registry not configured"})
		return
	}

	agent, err := s.registry.ConsumeToken(strings.TrimSpace(req.Token))
	if err != nil {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}

	if s.audit != nil {
		s.audit.Log(AuditEvent{
			Actor:  "agent",
			Target: agent.ServerID,
			Action: "agent.enroll",
			Time:   time.Now(),
			Result: "success",
			Detail: "agent_id=" + agent.ID,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]string{
		"agent_id":  agent.ID,
		"secret":    agent.RawSecret,
		"server_id": agent.ServerID,
	})
}

func (s *FleetServer) handleListContainers(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetContainer, "*"); !ok {
		return
	}

	var containers []ContainerSummary
	if s.manager != nil {
		containers = s.manager.ListContainers()
	}
	if containers == nil {
		containers = []ContainerSummary{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"containers": containers,
	})
}

func (s *FleetServer) handleContainerAction(action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		principal, ok := s.checkAuthz(w, r, authz.OpControl, authz.ResourceFleetContainer, id)
		if !ok {
			return
		}

		var req struct {
			ServerID string `json:"server_id"`
		}
		if r.Body != nil && r.ContentLength > 0 {
			if !s.readJSON(w, r, &req) {
				return
			}
		}
		serverID := req.ServerID
		if serverID == "" {
			serverID = r.URL.Query().Get("server_id")
		}

		if serverID == "" && s.registry != nil {
			servers := s.registry.ListServers()
			if len(servers) == 1 {
				serverID = servers[0].ID
			}
		}

		if serverID == "" {
			s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "server_id is required"})
			return
		}

		if s.jobs == nil {
			s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "jobs store not configured"})
			return
		}

		jobID, err := s.jobs.Submit(serverID, action, id, principal, "")
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		job, _ := s.jobs.Get(jobID)

		if s.audit != nil {
			s.audit.Log(AuditEvent{
				Actor:  principal,
				Target: id,
				Action: "container." + action,
				Time:   time.Now(),
				Result: "accepted",
				Detail: fmt.Sprintf("server_id=%s job_id=%s", serverID, job.ID),
			})
		}

		s.dispatchJobToAgent(serverID, job)

		s.writeJSON(w, http.StatusAccepted, map[string]interface{}{
			"job_id": job.ID,
			"status": job.Status,
		})
	}
}

func (s *FleetServer) dispatchJobToAgent(serverID string, job *Job) {
	if s.manager == nil || s.registry == nil {
		return
	}
	var agentID string
	for _, a := range s.registry.ListAgents() {
		if a.ServerID == serverID && !a.Revoked {
			agentID = a.ID
			break
		}
	}
	if agentID != "" {
		_ = s.manager.DispatchJob(context.Background(), agentID, job)
	}
}

func (s *FleetServer) handleContainerLogs(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		id = r.URL.Query().Get("container_id")
	}
	if id == "" {
		id = "*"
	}
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetContainer, id); !ok {
		return
	}

	tail := 100
	if tStr := r.URL.Query().Get("tail"); tStr != "" {
		if n, err := strconv.Atoi(tStr); err == nil && n > 0 {
			tail = n
		}
	}
	if tail > 1000 {
		tail = 1000 // capped
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"container_id": id,
		"tail":         tail,
		"lines":        []string{},
	})
}

func (s *FleetServer) handleListJobs(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetJob, "*"); !ok {
		return
	}

	var jobs []*Job
	if s.jobs != nil {
		serverID := r.URL.Query().Get("server_id")
		if serverID != "" {
			jobs = s.jobs.ListByServer(serverID)
		} else {
			jobs = s.jobs.List()
		}
	}
	if jobs == nil {
		jobs = []*Job{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"jobs": jobs,
	})
}

func (s *FleetServer) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetJob, id); !ok {
		return
	}

	if s.jobs == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}

	job, err := s.jobs.Get(id)
	if err != nil || job == nil {
		s.writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	s.writeJSON(w, http.StatusOK, job)
}

func (s *FleetServer) handleListAudit(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetAudit, "*"); !ok {
		return
	}

	var events []*AuditEvent
	if s.audit != nil {
		events = s.audit.List()
	}
	if events == nil {
		events = []*AuditEvent{}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"events": events,
	})
}

func (s *FleetServer) getOrCreateAgentChannel(agentID string) *ChannelTransport {
	s.mu.Lock()
	defer s.mu.Unlock()

	ch, ok := s.agentChannels[agentID]
	if !ok {
		c1, c2 := NewChannelTransportPair(64)
		s.agentChannels[agentID] = c1
		if s.manager != nil {
			_, _ = s.manager.RegisterSession(agentID, "", c2)
		}
		ch = c1
	}
	return ch
}

func (s *FleetServer) handleAgentInbound(w http.ResponseWriter, r *http.Request) {
	agentID := r.URL.Query().Get("agent_id")
	if agentID == "" {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "agent_id query parameter required"})
		return
	}

	tsStr := r.Header.Get("X-Fleet-Timestamp")
	mac := r.Header.Get("X-Fleet-HMAC")
	if tsStr == "" || mac == "" {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing authentication headers"})
		return
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || time.Since(time.Unix(ts, 0)).Abs() > 5*time.Minute {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "timestamp skew exceeded"})
		return
	}

	if s.registry == nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "registry not configured"})
		return
	}

	if _, err := s.registry.AuthorizeStream(agentID); err != nil {
		s.writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		return
	}

	secret, err := s.registry.AgentKey(agentID)
	if err != nil {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "agent key unavailable"})
		return
	}

	expectedMAC := ComputeHMAC(secret, DirectionAgentToControl, "poll", "", ts, agentID)
	if subtle.ConstantTimeCompare([]byte(mac), []byte(expectedMAC)) != 1 {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "HMAC signature mismatch"})
		return
	}

	ch := s.getOrCreateAgentChannel(agentID)

	var batch []*Envelope
	for {
		select {
		case env := <-ch.inbound:
			if env != nil {
				batch = append(batch, env)
			}
		default:
			goto drained
		}
	}
drained:

	if len(batch) == 0 {
		s.writeJSON(w, http.StatusOK, []*Envelope{})
		return
	}
	s.writeJSON(w, http.StatusOK, batch)
}

func (s *FleetServer) handleAgentEnvelope(w http.ResponseWriter, r *http.Request) {
	var env Envelope
	if !s.readJSON(w, r, &env) {
		return
	}

	if s.manager == nil {
		s.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "manager not configured"})
		return
	}

	// Verify HMAC statelessly BEFORE allocating any per-agent state: an
	// anonymous POST with a forged AgentID must not grow agentChannels.
	gate := NewVerifier("", time.Minute, DirectionAgentToControl).WithRegistry(s.registry)
	if err := gate.Verify(&env); err != nil {
		s.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}

	// Register session with ChannelTransport if not yet registered
	s.getOrCreateAgentChannel(env.AgentID)

	if err := s.manager.HandleInboundEnvelope(&env); err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
