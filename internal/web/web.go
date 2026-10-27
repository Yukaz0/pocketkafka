package web

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/audit"
	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/fleet"
	"github.com/Yukaz0/pocketkafka/internal/gateway"
	"github.com/Yukaz0/pocketkafka/internal/metrics"
	"github.com/Yukaz0/pocketkafka/internal/ratelimit"
	"github.com/Yukaz0/pocketkafka/internal/schemaregistry"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
	webui "github.com/Yukaz0/pocketkafka/web"
)

type Server struct {
	store     *storage.Store
	gm        *coordinator.GroupManager
	sr        *schemaregistry.Registry
	brokerID  int32
	clusterID string
	version   string
	startTime time.Time
	metrics   *metrics.Registry
	monitor   *Monitor

	clusters        *clusterStore
	clusterToken    string
	delegatedTokens *delegatedTokenStore
	clusterCacheMu  sync.Mutex
	clusterCache    map[time.Duration]clusterCacheEntry

	externalMu sync.Mutex
	external   map[string]*externalCluster

	users              []config.SecurityUser
	authSecret         string
	authEnabled        bool
	authorize          bool
	authorizer         authz.Authorizer
	authorizerExplicit bool
	sessions           *sessionStore
	loginLimiter       *ratelimit.Limiter
	trustedProxies     []string
	allowedHosts       []string
	registered         []string

	dataDir string

	listeners    []string
	advertised   string
	securityMode string

	mqtt        *gateway.MQTTBridge
	mqttListen  string
	mqttRunning bool

	gatewayListen   string
	schemaRegListen string

	fleetEnabled    bool
	fleetListen     string
	fleetProxyToken string
	fleetUseTLS     bool
	fleetTLSCA      string
	fleetTLSServer  string

	cfg config.Config

	aclStore *authz.Store
	auditMu  sync.Mutex
	auditLog []AuditEntry

	rrMu      sync.Mutex
	rrCounter map[string]uint64
}

func (s *Server) WithDataDir(dir string) *Server {
	s.dataDir = dir
	path := ""
	if dir != "" {
		path = filepath.Join(dir, delegatedTokensFile)
	}
	s.delegatedTokens = newDelegatedTokenStore(path)
	return s
}

func (s *Server) WithACLStore(store *authz.Store) *Server {
	if store != nil {
		s.aclStore = store
		if !s.authorizerExplicit {
			s.authorizer = store
		}
	}
	return s
}

func (s *Server) WithAuthorizer(a authz.Authorizer) *Server {
	if a != nil {
		s.authorizer = a
		s.authorizerExplicit = true
	}
	return s
}

func (s *Server) WithMQTT(b *gateway.MQTTBridge, listen string, running bool) *Server {
	s.mqtt = b
	s.mqttListen = listen
	s.mqttRunning = running
	if b != nil && s.metrics != nil {
		s.metrics.RegisterGauge("pocketkafka_mqtt_connections", "Live MQTT client connections", func() float64 {
			return float64(b.ActiveClients())
		})
	}
	return s
}

func (s *Server) WithGateway(listen string) *Server {
	s.gatewayListen = listen
	return s
}

func (s *Server) WithSchemaRegistryListen(listen string) *Server {
	s.schemaRegListen = listen
	return s
}
func (s *Server) WithConfig(cfg config.Config) *Server {
	s.cfg = cfg
	s.fleetEnabled = cfg.Fleet.Enabled
	s.fleetListen = cfg.Fleet.Listen
	s.fleetUseTLS = cfg.Fleet.TLS.CertFile != "" && cfg.Fleet.TLS.KeyFile != ""
	s.fleetTLSCA = cfg.Fleet.TLS.CAFile
	s.fleetTLSServer = cfg.Fleet.TLS.ServerName
	return s
}

func (s *Server) WithFleet(enabled bool, listen, proxyToken string, useTLS bool, caFile, serverName string) *Server {
	s.fleetEnabled = enabled
	s.fleetListen = listen
	s.fleetProxyToken = proxyToken
	s.fleetUseTLS = useTLS
	s.fleetTLSCA = caFile
	s.fleetTLSServer = serverName
	return s
}

func (s *Server) WithBrokerInfo(listeners []string, advertised, securityMode string) *Server {
	s.listeners = listeners
	s.advertised = advertised
	s.securityMode = securityMode
	return s
}

func New(store *storage.Store, gm *coordinator.GroupManager, sr *schemaregistry.Registry, brokerID int32, clusterID string, version string) *Server {
	acl := authz.NewInMemory()
	return &Server{
		store:           store,
		gm:              gm,
		sr:              sr,
		brokerID:        brokerID,
		clusterID:       clusterID,
		version:         version,
		startTime:       time.Now(),
		metrics:         newWebMetrics(),
		monitor:         NewMonitor(),
		clusters:        newClusterStore("", nil, nil),
		delegatedTokens: newDelegatedTokenStore(""),
		aclStore:        acl,
		authorizer:      acl,
		sessions:        newSessionStore(0, 0),
		loginLimiter:    ratelimit.New(5, 2*time.Second, 5*time.Minute),
		rrCounter:       make(map[string]uint64),
	}
}

func newWebMetrics() *metrics.Registry {
	reg := metrics.NewRegistry()
	reg.RegisterGauge("pocketkafka_auth_failures_total", "Refused credentials since start (monotonic counter exposed as a gauge)", func() float64 {
		return float64(audit.AuthFailures())
	})
	reg.RegisterGauge("pocketkafka_authz_denials_total", "Refused permissions since start (monotonic counter exposed as a gauge)", func() float64 {
		return float64(audit.AuthzDenials())
	})
	return reg
}

func (s *Server) WithAuth(cfg config.Config) *Server {
	s.users = cfg.Security.Users
	s.authSecret = cfg.Web.AuthSecret
	s.authEnabled = cfg.Security.Enabled || (cfg.Web.Enabled && cfg.Web.Auth)
	s.authorize = cfg.Security.Enabled
	s.trustedProxies = cfg.Web.TrustedProxies
	s.allowedHosts = cfg.Web.AllowedHosts
	if cfg.Web.SessionTTLMinutes > 0 || cfg.Web.IdleTrimMinutes > 0 {
		s.sessions = newSessionStore(
			time.Duration(cfg.Web.SessionTTLMinutes)*time.Minute,
			time.Duration(cfg.Web.IdleTrimMinutes)*time.Minute,
		)
	}
	return s
}

func (s *Server) can(principal string, op authz.Operation, res authz.Resource) bool {
	if !s.authorize {
		return true
	}
	if principal == "" || s.authorizer == nil {
		return false
	}
	if s.authorizer.Authorize(principal, op, res) == nil {
		return true
	}
	if op == authz.OpDescribe && res.Type != authz.ResourceCluster {
		return s.authorizer.Authorize(principal, op, clusterResource) == nil
	}
	return false
}

func (s *Server) canList(principal, resourceType string) bool {
	if !s.authorize {
		return true
	}
	if principal == "" || s.authorizer == nil {
		return false
	}
	if l, ok := s.authorizer.(authz.Lister); ok && l.CanDescribeAny(principal, resourceType) {
		return true
	}
	return s.authorizer.Authorize(principal, authz.OpDescribe, clusterResource) == nil
}

func (s *Server) authorizeAPI(mux *http.ServeMux, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize || !isAPIPath(r.URL.Path) || apiPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		if s.clusterTokenPresented(r) {
			next.ServeHTTP(w, r)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api/v1/fleet/") || r.URL.Path == "/api/v1/fleet" {
			next.ServeHTTP(w, r)
			return
		}
		_, pattern := mux.Handler(r)
		rule, ok := lookupRule(pattern)
		if !ok {
			s.auditDeny(w, r, s.requestPrincipal(r), "auth.route", "no authorization rule for "+r.URL.Path)
			return
		}
		params, matched := matchPattern(patternPath(pattern), r.URL.Path)
		if !matched {
			s.auditDeny(w, r, s.requestPrincipal(r), "auth.route", "route pattern did not match the request path")
			return
		}
		principal := s.requestPrincipal(r)
		if rule.list {
			if !s.canList(principal, rule.listType) {
				s.auditDeny(w, r, principal, "auth.list", "may not list "+rule.listType+"s")
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		res := rule.res(params)
		if !s.can(principal, rule.op, res) {
			s.auditDeny(w, r, principal, "auth.request", fmt.Sprintf("needs %s on %s %q", rule.op, res.Type, res.Name))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) Handler() http.Handler {
	mux := s.routes()
	authed := s.authMiddleware(mux, s.authorizeAPI(mux, mux))
	return securityHeaders(csrfMiddleware(authed))
}

func (s *Server) route(mux *http.ServeMux, pattern string, h func(http.ResponseWriter, *http.Request)) {
	s.registered = append(s.registered, pattern)
	mux.HandleFunc(pattern, h)
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/", noCache(http.FileServer(http.FS(webui.FS()))))
	s.route(mux, "GET /api/v1/info", s.handleInfo)
	s.route(mux, "GET /api/v1/cluster", s.handleCluster)
	s.route(mux, "GET /api/v1/topics", s.handleTopics)
	s.route(mux, "POST /api/v1/topics", s.handleCreateTopic)
	s.route(mux, "DELETE /api/v1/topics/{topic}", s.handleDeleteTopic)
	s.route(mux, "GET /api/v1/topics/{topic}/messages", s.handleSearchMessages)
	s.route(mux, "POST /api/v1/topics/{topic}/messages", s.handlePostMessage)
	s.route(mux, "GET /api/v1/topics/{topic}/partitions", s.handleTopicPartitions)
	s.route(mux, "POST /api/v1/topics/{topic}/truncate", s.handleTruncateTopic)
	s.route(mux, "POST /api/v1/topics/{topic}/compact", s.handleCompactTopic)
	s.route(mux, "GET /api/v1/groups", s.handleGroups)
	s.route(mux, "GET /api/v1/groups/{group}", s.handleGroupDetail)
	s.route(mux, "DELETE /api/v1/groups/{group}", s.handleDeleteGroup)
	s.route(mux, "POST /api/v1/groups/{group}/offsets/reset", s.handleResetGroupOffset)
	s.route(mux, "GET /api/v1/groups/{group}/offsets/export", s.handleExportGroupOffsets)
	s.route(mux, "POST /api/v1/groups/{group}/offsets/import", s.handleImportGroupOffsets)
	s.route(mux, "GET /api/v1/topics/{topic}/config", s.handleTopicConfig)
	s.route(mux, "PUT /api/v1/topics/{topic}/config", s.handleTopicConfig)
	s.route(mux, "POST /api/v1/topics/{topic}/import", s.handleImportJSONL)
	s.route(mux, "GET /api/v1/logs", s.handleBrokerLogs)
	s.route(mux, "GET /api/v1/throughput", s.handleThroughput)
	s.route(mux, "POST /api/v1/schemas/register", s.handleRegisterSchema)
	s.route(mux, "GET /api/v1/mqtt", s.handleMQTTStatus)
	s.route(mux, "POST /api/v1/mqtt/publish", s.handleMQTTPublish)
	s.route(mux, "GET /api/v1/integrations", s.handleIntegrations)
	s.route(mux, "GET /api/v1/config", s.handleConfig)
	s.route(mux, "GET /api/v1/schemas", s.handleSchemas)
	s.route(mux, "GET /api/v1/schemas/{subject}", s.handleSchemaDetail)
	s.route(mux, "GET /api/v1/acls", s.handleListACLs)
	s.route(mux, "PUT /api/v1/acls", s.handleUpsertACL)
	s.route(mux, "DELETE /api/v1/acls", s.handleDeleteACL)
	s.route(mux, "GET /api/v1/audit", s.handleAudit)
	s.route(mux, "GET /api/v1/topics/{topic}/tail", s.handleTailWS)
	s.route(mux, "POST /api/v1/auth/login", s.handleLogin)
	s.route(mux, "POST /api/v1/auth/logout", s.handleLogout)
	s.route(mux, "GET /api/v1/auth/status", s.handleAuthStatus)
	s.route(mux, "GET /api/v1/auth/delegated-tokens", s.handleListDelegatedTokens)
	s.route(mux, "POST /api/v1/auth/delegated-tokens", s.handleCreateDelegatedToken)
	s.route(mux, "DELETE /api/v1/auth/delegated-tokens/{id}", s.handleRevokeDelegatedToken)
	s.route(mux, "GET /healthz", s.handleHealthz)
	s.route(mux, "GET /livez", s.handleLivez)
	s.route(mux, "GET /metrics", s.handleMetrics)
	s.route(mux, "GET /api/v1/health/overview", s.handleHealthOverview)
	s.route(mux, "GET /api/v1/health/clusters", s.handleClustersOverview)
	s.route(mux, "GET /api/v1/clusters", s.handleClusters)
	s.route(mux, "POST /api/v1/clusters", s.handleUpsertCluster)
	s.route(mux, "POST /api/v1/clusters/test", s.handleTestCluster)
	s.route(mux, "DELETE /api/v1/clusters/{name}", s.handleDeleteCluster)
	s.route(mux, "/api/v1/target/{name}/{path...}", s.handleClusterTarget)

	if s.fleetEnabled {
		u := fleetTargetURL(s.fleetListen, s.fleetUseTLS)
		proxy := httputil.NewSingleHostReverseProxy(u)
		if s.fleetUseTLS {
			tlsConf, err := fleetProxyTLS(s.fleetTLSCA, s.fleetTLSServer, u)
			if err != nil {
				slog.Error("fleet: proxy TLS config invalid", "error", err)
				mux.Handle("/api/v1/fleet/", fleet.NewDisabledHandler())
				mux.Handle("/api/v1/fleet", fleet.NewDisabledHandler())
				return mux
			}
			proxy.Transport = &http.Transport{TLSClientConfig: tlsConf}
		}
		origDirector := proxy.Director
		proxy.Director = func(req *http.Request) {
			origDirector(req)
			req.Header.Del("X-Pocketkafka-Principal")
			req.Header.Del("X-Remote-User")
			req.Header.Del("X-Pocketkafka-Fleet-Proxy")
			if p := s.requestPrincipal(req); p != "" {
				req.Header.Set("X-Pocketkafka-Principal", p)
			}
			req.Header.Set("X-Pocketkafka-Fleet-Proxy", s.fleetProxyToken)
		}
		mux.Handle("/api/v1/fleet/", proxy)
		mux.Handle("/api/v1/fleet", proxy)
	} else {
		disabled := fleet.NewDisabledHandler()
		mux.Handle("/api/v1/fleet/", disabled)
		mux.Handle("/api/v1/fleet", disabled)
	}
	return mux
}

func fleetTargetURL(listen string, useTLS bool) *url.URL {
	host := strings.TrimSpace(listen)
	if host == "" {
		host = "127.0.0.1:18090"
	}
	if h, p, err := net.SplitHostPort(host); err == nil {
		if h == "" || h == "0.0.0.0" || h == "::" {
			host = net.JoinHostPort("127.0.0.1", p)
		}
	} else if strings.HasPrefix(host, ":") {
		host = "127.0.0.1" + host
	}
	scheme := "http"
	if useTLS {
		scheme = "https"
	}
	u, err := url.Parse(scheme + "://" + host)
	if err != nil {
		u, _ = url.Parse(scheme + "://127.0.0.1:18090")
	}
	return u
}

func fleetProxyTLS(caFile, serverName string, u *url.URL) (*tls.Config, error) {
	conf := &tls.Config{MinVersion: tls.VersionTLS12}
	host := u.Hostname()
	if serverName != "" {
		conf.ServerName = serverName
	} else if host != "" {
		conf.ServerName = host
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("fleet: read ca_file: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("fleet: ca_file has no valid certificates")
		}
	}
	conf.RootCAs = pool
	return conf, nil
}
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(s.metrics.Render(s.store, s.gm, s.clusterID)))
}

func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]interface{}{
		"product":      "pocketkafka",
		"vendor":       "Muhammad Jafar Ikbal (Yukaz0)",
		"upstream":     "https://github.com/Yukaz0/pocketkafka",
		"license":      "MIT",
		"version":      s.version,
		"fleetEnabled": s.fleetEnabled,
		"mqttEnabled":  s.mqtt != nil && s.mqttRunning,
	})
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	topics := s.store.TopicsSnapshot()
	partitions := 0
	var totalBytes int64
	for _, t := range topics {
		partitions += len(t.Partitions)
		for _, p := range t.Partitions {
			totalBytes += p.SizeBytes()
		}
	}
	writeJSON(w, 200, map[string]interface{}{
		"clusterId":    s.clusterID,
		"brokerId":     s.brokerID,
		"version":      s.version,
		"status":       "green",
		"topics":       len(topics),
		"partitions":   partitions,
		"totalBytes":   totalBytes,
		"diskUsagePct": s.store.DiskUsagePct(),
		"groups":       len(s.gm.ListGroups()),
		"listeners":    s.listeners,
		"advertised":   s.advertised,
		"security":     s.securityMode,
	})
}

type topicInfo struct {
	Name       string          `json:"name"`
	Partitions []partitionInfo `json:"partitions"`
	TotalBytes int64           `json:"totalBytes"`
}

type partitionInfo struct {
	Partition int32 `json:"partition"`
	Leader    int32 `json:"leader"`
	LogEnd    int64 `json:"logEndOffset"`
	HighWater int64 `json:"highWatermark"`
	Earliest  int64 `json:"earliestOffset"`
	Bytes     int64 `json:"bytes"`
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	principal := s.requestPrincipal(r)
	names := s.store.TopicNames()
	out := make([]topicInfo, 0, len(names))
	for _, name := range names {
		if !s.can(principal, authz.OpDescribe, authz.Resource{Type: authz.ResourceTopic, Name: name}) {
			continue
		}
		t := s.store.GetTopic(name)
		if t == nil {
			continue
		}
		ti := topicInfo{Name: name}
		for i := range len(t.Partitions) {
			p := t.Partitions[int32(i)]
			ti.Partitions = append(ti.Partitions, partitionInfo{
				Partition: int32(i),
				Leader:    s.brokerID,
				LogEnd:    p.LogEndOffset(),
				HighWater: p.HighWatermark(),
				Earliest:  p.EarliestOffset(),
				Bytes:     p.SizeBytes(),
			})
			ti.TotalBytes += p.SizeBytes()
		}
		out = append(out, ti)
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if req.Name == "" {
		writeErr(w, 400, "name required")
		return
	}
	if req.Partitions <= 0 {
		req.Partitions = 1
	}
	if err := storage.ValidateTopicName(req.Name); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if s.store.GetTopic(req.Name) != nil {
		writeErr(w, 409, "topic already exists")
		return
	}
	if _, err := s.store.CreateTopic(req.Name, req.Partitions); err != nil {
		s.auditFailure(r, "topic.create", req.Name, err.Error())
		writeErr(w, 500, err.Error())
		return
	}
	s.auditRequest(r, "topic.create", req.Name, fmt.Sprintf("partitions=%d", req.Partitions))
	writeJSON(w, 201, map[string]interface{}{"name": req.Name, "partitions": req.Partitions})
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if s.store.GetTopic(topic) == nil {
		writeErr(w, 404, "unknown topic")
		return
	}
	if err := s.store.DeleteTopic(topic); err != nil {
		s.auditFailure(r, "topic.delete", topic, err.Error())
		writeErr(w, 500, err.Error())
		return
	}
	s.auditRequest(r, "topic.delete", topic, "deleted topic")
	writeJSON(w, 200, map[string]string{"deleted": topic})
}

type messageRecord struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp int64             `json:"timestamp"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value,omitempty"`
	ValueB64  string            `json:"valueB64,omitempty"`
	ValueSize int               `json:"valueSize"`
	IsJSON    bool              `json:"isJSON"`
	Headers   map[string]string `json:"headers"`
}

func (s *Server) pickPartition(topic, key string, want int32) *storage.Partition {
	t := s.store.GetTopic(topic)
	if t == nil {
		return nil
	}
	n := int32(len(t.Partitions))
	if n == 0 {
		return nil
	}
	if want >= 0 && want < n {
		return t.Partitions[want]
	}
	if n == 1 {
		return t.Partitions[0]
	}
	if key == "" {
		s.rrMu.Lock()
		s.rrCounter[topic]++
		idx := s.rrCounter[topic] % uint64(n)
		s.rrMu.Unlock()
		return t.Partitions[int32(idx)]
	}
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return t.Partitions[int32(h%uint64(n))]
}

func (s *Server) handlePostMessage(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Partition *int32            `json:"partition"`
		Headers   map[string]string `json:"headers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if s.store.GetTopic(topic) == nil {
		s.store.EnsureTopic(topic, 1)
	}
	want := int32(-1)
	if req.Partition != nil {
		want = *req.Partition
	}
	p := s.pickPartition(topic, req.Key, want)
	if p == nil {
		writeErr(w, 500, "partition unavailable")
		return
	}

	now := time.Now().UnixMilli()
	var headers []protocol.RecordHeader
	for k, v := range req.Headers {
		headers = append(headers, protocol.RecordHeader{Key: k, Value: []byte(v)})
	}
	batch := &protocol.RecordBatch{
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records: []protocol.Record{{
			Key:     []byte(req.Key),
			Value:   []byte(req.Value),
			Headers: headers,
		}},
	}
	raw, err := protocol.EncodeRecordBatch(batch)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	off, err := p.Append(raw)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]interface{}{"topic": topic, "partition": p.PartitionID(), "offset": off})
}

func decodeRecords(topic string, partition int32, raw []byte) []messageRecord {
	out := []messageRecord{}
	pos := 0
	for pos < len(raw) {
		h, err := storage.ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(storage.BatchTotalSize(h))
		if pos+total > len(raw) {
			break
		}
		b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err == nil {
			for _, rec := range b.Records {
				value := rec.Value
				headers := map[string]string{}
				for _, hd := range rec.Headers {
					headers[hd.Key] = string(hd.Value)
				}
				out = append(out, messageRecord{
					Topic:     topic,
					Partition: partition,
					Offset:    b.BaseOffset + int64(rec.OffsetDelta),
					Timestamp: b.BaseTimestamp + rec.TimestampDelta,
					Key:       string(rec.Key),
					Value:     string(value),
					ValueB64:  base64.StdEncoding.EncodeToString(value),
					ValueSize: len(value),
					IsJSON:    json.Valid(value),
					Headers:   headers,
				})
			}
		}
		pos += total
	}
	return out
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	infos := s.gm.ListGroups()
	type g struct {
		Name       string           `json:"name"`
		State      string           `json:"state"`
		Generation int32            `json:"generation"`
		Leader     string           `json:"leader"`
		Members    []string         `json:"members"`
		Lag        map[string]int64 `json:"lag"`
	}
	out := make([]g, 0, len(infos))
	principal := s.requestPrincipal(r)
	for _, info := range infos {
		if !s.can(principal, authz.OpDescribe, authz.Resource{Type: authz.ResourceGroup, Name: info.Name}) {
			continue
		}
		item := g{Name: info.Name, State: info.State, Generation: info.Generation, Leader: info.LeaderID, Members: info.Members, Lag: map[string]int64{}}
		for topic, parts := range info.Offsets {
			if !s.can(principal, authz.OpDescribe, authz.Resource{Type: authz.ResourceTopic, Name: topic}) {
				continue
			}
			for part, committed := range parts {
				leo := int64(0)
				if p := s.store.GetPartition(topic, part); p != nil {
					leo = p.LogEndOffset()
				}
				key := fmt.Sprintf("%s-%d", topic, part)
				lag := leo - committed
				if lag < 0 {
					lag = 0
				}
				item.Lag[key] = lag
			}
		}
		out = append(out, item)
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleSchemas(w http.ResponseWriter, r *http.Request) {
	if s.sr == nil {
		writeJSON(w, 200, []string{})
		return
	}
	writeJSON(w, 200, s.sr.ListSubjects())
}

func (s *Server) handleTailWS(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	q := r.URL.Query()
	qp := q.Get("partition")
	allPart := qp == "" || qp == "all"
	var partID int32
	if !allPart {
		pid, _ := strconv.Atoi(qp)
		partID = int32(pid)
	}
	conn, err := s.upgradeWebSocket(w, r)
	if err != nil {
		writeErr(w, 400, "websocket upgrade failed: "+err.Error())
		return
	}
	defer conn.Close()

	type tailPart struct {
		id   int32
		part *storage.Partition
		next int64
	}
	var parts []tailPart
	if allPart {
		if t := s.store.GetTopic(topic); t != nil {
			for id := int32(0); id < int32(len(t.Partitions)); id++ {
				if p := t.Partitions[id]; p != nil {
					parts = append(parts, tailPart{id: id, part: p, next: p.HighWatermark()})
				}
			}
		}
	} else {
		if p := s.store.GetPartition(topic, partID); p != nil {
			parts = append(parts, tailPart{id: partID, part: p, next: p.HighWatermark()})
		}
	}
	if len(parts) == 0 {
		writeWSFrame(conn, []byte(`{"error":"unknown topic"}`))
		return
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := wsDrain(conn); err != nil {
			return
		}

		var pending []messageRecord
		var unread int64
		for i := range parts {
			tp := &parts[i]
			if hwm := tp.part.HighWatermark(); hwm > tp.next {
				want := hwm - tp.next
				got := int64(0)
				raw, _, err := tp.part.Read(tp.next, 1<<20)
				if err == nil && len(raw) > 0 {
					recs := decodeRecords(topic, tp.id, raw)
					got = int64(len(recs))
					pending = append(pending, recs...)
				}
				if got < want {
					unread += want - got
				}
				tp.next = hwm
			}
		}
		if len(pending) > 0 {
			frame, capped := capTailFrame(pending, tailMaxPerFrame)
			if err := writeWSFrame(conn, mustJSON(frame)); err != nil {
				return
			}
			unread += int64(capped)
		}
		if unread > 0 {
			if err := writeWSFrame(conn, mustJSON(map[string]int64{"skipped": unread})); err != nil {
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

const tailMaxPerFrame = 2000

func capTailFrame(pending []messageRecord, max int) ([]messageRecord, int) {
	if max <= 0 || len(pending) <= max {
		return pending, 0
	}
	return pending[len(pending)-max:], len(pending) - max
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("[]")
	}
	return b
}
