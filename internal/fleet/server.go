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
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
func isValidServerID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := range id {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			continue
		}
		return false
	}
	return true
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
	ProxyToken      string
	MaxConnections  int
	MaxActiveJobs   int
	MaxAgents       int
	EnrollRateLimit int
	OpRateLimit     int
}
type FleetServer struct {
	mu             sync.RWMutex
	mux            *http.ServeMux
	registry       *Registry
	jobs           *Jobs
	audit          *Audit
	manager        *AgentManager
	authorizer     authz.Authorizer
	maxBodyBytes   int64
	proxyToken     string
	maxConnections int
	activeConns    atomic.Int64
	enrollLimiter  *RateLimiter
	opLimiter      *RateLimiter

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

	maxConnections := cfg.MaxConnections
	if maxConnections <= 0 {
		maxConnections = 256
	}

	maxActiveJobs := cfg.MaxActiveJobs
	if maxActiveJobs <= 0 {
		maxActiveJobs = 1000
	}
	if cfg.Jobs != nil {
		cfg.Jobs.SetMaxActiveJobs(maxActiveJobs)
	}

	maxAgents := cfg.MaxAgents
	if maxAgents <= 0 {
		maxAgents = 500
	}
	if cfg.Registry != nil {
		cfg.Registry.SetMaxAgents(maxAgents)
	}

	enrollRateLimit := cfg.EnrollRateLimit
	if enrollRateLimit <= 0 {
		enrollRateLimit = 30
	}

	opRateLimit := cfg.OpRateLimit
	if opRateLimit <= 0 {
		opRateLimit = 120
	}

	s := &FleetServer{
		mux:            http.NewServeMux(),
		registry:       cfg.Registry,
		jobs:           cfg.Jobs,
		audit:          cfg.Audit,
		manager:        cfg.Manager,
		authorizer:     cfg.Authorizer,
		maxBodyBytes:   maxBytes,
		proxyToken:     cfg.ProxyToken,
		maxConnections: maxConnections,
		enrollLimiter:  NewRateLimiter(enrollRateLimit, time.Minute),
		opLimiter:      NewRateLimiter(opRateLimit, time.Minute),
		agentChannels:  make(map[string]*ChannelTransport),
	}

	s.routes()
	return s
}

func generateRequestID() string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return "req-" + hex.EncodeToString(b[:])
}

func clientIP(r *http.Request) string {
	// RemoteAddr only: X-Forwarded-For / X-Real-IP are client-controlled on
	// the direct fleet listener and must not feed rate-limit keys.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *FleetServer) writeError(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	reqID := r.Header.Get("X-Request-ID")
	if reqID == "" {
		reqID = r.Header.Get("X-Correlation-ID")
	}
	if reqID == "" {
		reqID = generateRequestID()
	}
	w.Header().Set("X-Request-ID", reqID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Code:      code,
		Message:   msg,
		RequestID: reqID,
		ErrorText: msg,
	})
}

// NewDisabledHandler returns an http.Handler that responds with 404 Not Found to indicate fleet is disabled.
func NewDisabledHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = r.Header.Get("X-Correlation-ID")
		}
		if reqID == "" {
			reqID = generateRequestID()
		}
		w.Header().Set("X-Request-ID", reqID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Code:      ErrCodeNotFound,
			Message:   "fleet disabled",
			RequestID: reqID,
			ErrorText: "fleet disabled",
		})
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
				s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "internal server error")
			}
		}()

		if s.maxConnections > 0 {
			if cur := s.activeConns.Add(1); cur > int64(s.maxConnections) {
				s.activeConns.Add(-1)
				s.writeError(w, r, http.StatusTooManyRequests, ErrCodeConnectionLimitExceeded, "connection limit reached")
				return
			}
			defer s.activeConns.Add(-1)
		}

		if s.maxBodyBytes > 0 && r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)
		}
		// Agent transport endpoints authenticate via HMAC/tokens, not the proxy.
		// POST /enroll carries the single-use enrollment token as its credential.
		if !strings.HasPrefix(r.URL.Path, "/api/v1/fleet/agent/") && r.URL.Path != "/api/v1/fleet/enroll" {
			if s.proxyToken == "" || !validProxyToken(r, s.proxyToken) {
				slog.Error("fleet: rejected direct request without proxy token", "path", r.URL.Path)
				s.writeError(w, r, http.StatusForbidden, ErrCodeForbidden, "forbidden: missing or invalid proxy token")
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
		s.writeError(w, r, http.StatusForbidden, ErrCodeForbidden, "forbidden: authorizer not configured")
		return principal, false
	}
	res := authz.Resource{Type: resType, Name: resName}
	if err := s.authorizer.Authorize(principal, op, res); err != nil {
		s.writeError(w, r, http.StatusForbidden, ErrCodeForbidden, "forbidden: "+err.Error())
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
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "empty body")
		return false
	}
	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.writeError(w, r, http.StatusRequestEntityTooLarge, ErrCodePayloadTooLarge, "request body too large")
			return false
		}
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON body: "+err.Error())
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
	s.mux.HandleFunc("POST /api/v1/fleet/containers/{id}/remove", s.wrap(s.handleContainerAction("remove")))
	s.mux.HandleFunc("GET /api/v1/fleet/containers/{id}/logs", s.wrap(s.handleContainerLogs))

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
		s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "server not found")
		return
	}

	srv, ok := s.registry.GetServer(id)
	if !ok {
		s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "server not found")
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
		s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "registry not configured")
		return
	}

	err := s.registry.RevokeServer(id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, err.Error())
			return
		}
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, err.Error())
		return
	}
	if s.manager != nil {
		s.manager.UnregisterServerSessions(id)
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
	if _, ok := s.checkAuthz(w, r, authz.OpAdmin, authz.ResourceFleetServer, "*"); !ok {
		return
	}

	type enrollmentView struct {
		TokenPrefix string     `json:"token_prefix"`
		ServerID    string     `json:"server_id"`
		CreatedAt   time.Time  `json:"created_at"`
		ExpiresAt   *time.Time `json:"expires_at,omitempty"`
		Used        bool       `json:"used"`
	}
	var list []enrollmentView
	if s.registry != nil {
		for _, e := range s.registry.ListEnrollments() {
			prefix := e.Token
			if len(prefix) > 8 {
				prefix = prefix[:8]
			}
			list = append(list, enrollmentView{
				TokenPrefix: prefix,
				ServerID:    e.ServerID,
				CreatedAt:   e.CreatedAt,
				ExpiresAt:   e.ExpiresAt,
				Used:        e.Used,
			})
		}
	}
	if list == nil {
		list = []enrollmentView{}
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

	if s.enrollLimiter != nil && !s.enrollLimiter.Allow(clientIP(r)) {
		s.writeError(w, r, http.StatusTooManyRequests, ErrCodeRateLimited, "rate limit exceeded for enroll")
		return
	}

	var req struct {
		ServerID string `json:"server_id"`
		Duration string `json:"duration,omitempty"`
	}
	if !s.readJSON(w, r, &req) {
		return
	}

	req.ServerID = strings.TrimSpace(req.ServerID)
	if req.ServerID == "" {
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "server_id is required")
		return
	}
	if !isValidServerID(req.ServerID) {
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "server_id must be 1-64 characters and contain only letters, numbers, hyphens, underscores, and dots")
		return
	}
	if s.registry == nil {
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "registry not configured")
		return
	}

	dur := 15 * time.Minute
	if req.Duration == "never" || req.Duration == "unlimited" || req.Duration == "0" {
		dur = 0
	} else if req.Duration != "" {
		if parsed, err := time.ParseDuration(req.Duration); err == nil && parsed > 0 {
			dur = parsed
		}
	}

	tok, err := s.registry.enrollWithExpiry(req.ServerID, dur)
	if err != nil {
		if errors.Is(err, ErrLimitExceeded) {
			s.writeError(w, r, http.StatusTooManyRequests, ErrCodeLimitExceeded, err.Error())
			return
		}
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, err.Error())
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
	if s.enrollLimiter != nil && !s.enrollLimiter.Allow(clientIP(r)) {
		s.writeError(w, r, http.StatusTooManyRequests, ErrCodeRateLimited, "rate limit exceeded for enroll")
		return
	}

	var req struct {
		Token string `json:"token"`
	}
	if !s.readJSON(w, r, &req) {
		return
	}

	if s.registry == nil {
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "registry not configured")
		return
	}

	agent, err := s.registry.ConsumeToken(strings.TrimSpace(req.Token))
	if err != nil {
		if errors.Is(err, ErrLimitExceeded) {
			s.writeError(w, r, http.StatusTooManyRequests, ErrCodeLimitExceeded, err.Error())
			return
		}
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, err.Error())
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
		op := authz.OpControl
		if action == "remove" {
			op = authz.OpAdmin
		}
		principal, ok := s.checkAuthz(w, r, op, authz.ResourceFleetContainer, id)
		if !ok {
			return
		}

		limiterKey := principal
		if limiterKey == "" || limiterKey == authz.AnonymPrincipal {
			limiterKey = clientIP(r)
		}
		if s.opLimiter != nil && !s.opLimiter.Allow("action:"+limiterKey) {
			s.writeError(w, r, http.StatusTooManyRequests, ErrCodeRateLimited, "rate limit exceeded for operation")
			return
		}

		var req struct {
			ServerID string `json:"server_id"`
			Force    bool   `json:"force"`
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
			s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "server_id is required")
			return
		}
		if s.registry != nil {
			if _, ok := s.registry.GetServer(serverID); !ok {
				s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "unknown server_id")
				return
			}
		}

		if s.jobs == nil {
			s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "jobs store not configured")
			return
		}

		force := req.Force || r.URL.Query().Get("force") == "true"
		jobID, err := s.jobs.SubmitForce(serverID, action, id, principal, "", force)
		if err != nil {
			if errors.Is(err, ErrJobLimitExceeded) {
				s.writeError(w, r, http.StatusTooManyRequests, ErrCodeLimitExceeded, err.Error())
				return
			}
			s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, err.Error())
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
	if s.manager == nil || s.registry == nil || s.jobs == nil {
		return
	}
	var agentID string
	for _, a := range s.registry.ListAgents() {
		if a.ServerID == serverID && !a.Revoked {
			agentID = a.ID
			break
		}
	}
	if agentID == "" {
		_ = s.jobs.Cancel(job.ID, "no active agent for server "+serverID)
		return
	}
	if err := s.manager.DispatchJob(context.Background(), agentID, job); err != nil {
		_ = s.jobs.Cancel(job.ID, err.Error())
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
	principal, ok := s.checkAuthz(w, r, authz.OpDescribe, authz.ResourceFleetContainer, id)
	if !ok {
		return
	}

	limiterKey := principal
	if limiterKey == "" || limiterKey == authz.AnonymPrincipal {
		limiterKey = clientIP(r)
	}
	if s.opLimiter != nil && !s.opLimiter.Allow("logs:"+limiterKey) {
		s.writeError(w, r, http.StatusTooManyRequests, ErrCodeRateLimited, "rate limit exceeded for operation")
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

	serverID := r.URL.Query().Get("server_id")
	if serverID == "" && s.registry != nil {
		if servers := s.registry.ListServers(); len(servers) == 1 {
			serverID = servers[0].ID
		}
	}
	if serverID != "" && s.jobs != nil {
		jobID, err := s.jobs.SubmitWithTail(serverID, "logs", id, principal, "", tail)
		if err == nil {
			if job, _ := s.jobs.Get(jobID); job != nil {
				s.dispatchJobToAgent(serverID, job)
			}
			lines := s.waitJobLines(jobID, 8*time.Second)
			if lines != nil {
				s.writeJSON(w, http.StatusOK, map[string]interface{}{
					"container_id": id,
					"tail":         tail,
					"lines":        lines,
				})
				return
			}
		}
	}
	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"container_id": id,
		"tail":         tail,
		"lines":        []string{},
	})
}

// waitJobLines polls a logs job until it completes, returning split lines.
// Returns nil on timeout/failure so callers fall back to an empty list.
func (s *FleetServer) waitJobLines(jobID string, timeout time.Duration) []string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := s.jobs.Get(jobID)
		if err != nil || job == nil {
			return nil
		}
		if job.Status == JobStatusSucceeded {
			if job.Result == "" {
				return []string{}
			}
			return strings.Split(strings.TrimRight(job.Result, "\n"), "\n")
		}
		if job.Status == JobStatusFailed {
			return nil
		}
		time.Sleep(400 * time.Millisecond)
	}
	return nil
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
		s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "job not found")
		return
	}

	job, err := s.jobs.Get(id)
	if err != nil || job == nil {
		s.writeError(w, r, http.StatusNotFound, ErrCodeNotFound, "job not found")
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
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, "agent_id query parameter required")
		return
	}

	tsStr := r.Header.Get("X-Fleet-Timestamp")
	mac := r.Header.Get("X-Fleet-HMAC")
	if tsStr == "" || mac == "" {
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, "missing authentication headers")
		return
	}

	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || time.Since(time.Unix(ts, 0)).Abs() > 5*time.Minute {
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, "timestamp skew exceeded")
		return
	}

	if s.registry == nil {
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "registry not configured")
		return
	}

	if _, err := s.registry.AuthorizeStream(agentID); err != nil {
		s.writeError(w, r, http.StatusForbidden, ErrCodeForbidden, err.Error())
		return
	}

	secret, err := s.registry.AgentKey(agentID)
	if err != nil {
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, "agent key unavailable")
		return
	}

	expectedMAC := ComputeHMAC(secret, DirectionAgentToControl, "poll", "", ts, agentID)
	if subtle.ConstantTimeCompare([]byte(mac), []byte(expectedMAC)) != 1 {
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, "HMAC signature mismatch")
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
		s.writeError(w, r, http.StatusInternalServerError, ErrCodeInternal, "manager not configured")
		return
	}

	// Verify HMAC statelessly BEFORE allocating any per-agent state: an
	// anonymous POST with a forged AgentID must not grow agentChannels.
	gate := NewVerifier("", time.Minute, DirectionAgentToControl).WithRegistry(s.registry)
	if err := gate.Verify(&env); err != nil {
		s.writeError(w, r, http.StatusUnauthorized, ErrCodeUnauthorized, err.Error())
		return
	}

	// Register session with ChannelTransport if not yet registered
	s.getOrCreateAgentChannel(env.AgentID)

	if err := s.manager.HandleInboundEnvelope(&env); err != nil {
		s.writeError(w, r, http.StatusBadRequest, ErrCodeBadRequest, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
