package web

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/gateway"
	"github.com/Yukaz0/pocketkafka/internal/metrics"
	"github.com/Yukaz0/pocketkafka/internal/schemaregistry"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
	webui "github.com/Yukaz0/pocketkafka/web"
)

// Server is the embedded monitoring dashboard. It exposes a REST API and a
// WebSocket live-tail endpoint on top of the storage engine and coordinator.
type Server struct {
	store     *storage.Store
	gm        *coordinator.GroupManager
	sr        *schemaregistry.Registry
	brokerID  int32
	clusterID string
	version   string
	startTime time.Time
	metrics   *metrics.Registry
	// monitor keeps the health sample ring behind /api/v1/health/overview,
	// which the dashboard renders in its Health view.
	monitor *Monitor

	// Multi-cluster monitoring: the registry of peers this dashboard reports
	// on, the bearer token this broker accepts from an aggregating peer, and a
	// short-lived cache so a polling UI does not fan out on every tick.
	clusters        *clusterStore
	clusterToken    string
	delegatedTokens *delegatedTokenStore
	clusterCacheMu  sync.Mutex
	clusterCache    map[time.Duration]clusterCacheEntry

	// External Kafka clusters, sampled read-only as a client. Sampling runs on
	// its own interval and handlers serve the last snapshot.
	externalMu sync.Mutex
	external   map[string]*externalCluster

	// Auth (Fitur 12).
	users       []config.SecurityUser
	authSecret  string
	authEnabled bool
	// authorize turns on ACL enforcement for state-changing web API calls. It
	// is set when security is enabled, so the dashboard cannot act as an
	// authorization bypass.
	authorize bool

	// Data dir for persisted UI state (ACLs, etc.).
	dataDir string

	// Broker identity info for the cluster endpoint.
	listeners    []string
	advertised   string
	securityMode string

	// MQTT bridge status (nil when the bridge is disabled). running is false
	// when Start failed (for example the port is already bound), so status
	// reports the bridge as down instead of LISTENING.
	mqtt        *gateway.MQTTBridge
	mqttListen  string
	mqttRunning bool

	// Configured addresses of the other ingress surfaces. They come from config
	// so the Integrations page reports what the operator configured even when a
	// listener failed to bind.
	gatewayListen   string
	schemaRegListen string

	// cfg is the effective configuration (defaults applied) behind the
	// read-only broker-configuration card.
	cfg config.Config

	// Enterprise (Fitur 4.5): shared ACL store + audit trail. The ACL store is
	// the same one the Kafka/MQTT/REST surfaces authorize against, so an ACL
	// edited in the UI takes effect everywhere.
	aclStore *authz.Store
	auditMu  sync.Mutex
	auditLog []AuditEntry

	// Partition round-robin counters for keyless UI/REST produce.
	rrMu      sync.Mutex
	rrCounter map[string]uint64
}

// WithDataDir records the broker data dir for persisted UI state, including delegated tokens.
func (s *Server) WithDataDir(dir string) *Server {
	s.dataDir = dir
	path := ""
	if dir != "" {
		path = filepath.Join(dir, delegatedTokensFile)
	}
	s.delegatedTokens = newDelegatedTokenStore(path)
	return s
}

// WithACLStore installs the shared ACL store. The web UI manages exactly the
// same rules the Kafka/MQTT/REST surfaces enforce.
func (s *Server) WithACLStore(store *authz.Store) *Server {
	if store != nil {
		s.aclStore = store
	}
	return s
}

// WithMQTT records the MQTT bridge for the status endpoints. running is false
// when Start failed: listen is still the configured address, because that is
// what the operator sees in config, but the bridge is not serving clients.
func (s *Server) WithMQTT(b *gateway.MQTTBridge, listen string, running bool) *Server {
	s.mqtt = b
	s.mqttListen = listen
	s.mqttRunning = running
	return s
}

// WithGateway records the configured REST proxy address for the Integrations page.
func (s *Server) WithGateway(listen string) *Server {
	s.gatewayListen = listen
	return s
}

// WithSchemaRegistryListen records the configured schema registry address for
// the Integrations page.
func (s *Server) WithSchemaRegistryListen(listen string) *Server {
	s.schemaRegListen = listen
	return s
}

// WithConfig records the effective configuration (defaults applied) for the
// read-only broker-configuration card.
func (s *Server) WithConfig(cfg config.Config) *Server {
	s.cfg = cfg
	return s
}

// WithBrokerInfo records listener/advertised/security info for the cluster
// endpoint so the UI does not have to hardcode connection details.
func (s *Server) WithBrokerInfo(listeners []string, advertised, securityMode string) *Server {
	s.listeners = listeners
	s.advertised = advertised
	s.securityMode = securityMode
	return s
}

// New builds a web server bound to the given storage and coordinator.
func New(store *storage.Store, gm *coordinator.GroupManager, sr *schemaregistry.Registry, brokerID int32, clusterID string, version string) *Server {
	return &Server{
		store:           store,
		gm:              gm,
		sr:              sr,
		brokerID:        brokerID,
		clusterID:       clusterID,
		version:         version,
		startTime:       time.Now(),
		metrics:         metrics.NewRegistry(),
		monitor:         NewMonitor(),
		clusters:        newClusterStore("", nil, nil),
		delegatedTokens: newDelegatedTokenStore(""),
		aclStore:        authz.NewInMemory(),
		rrCounter:       make(map[string]uint64),
	}
}

// WithAuth enables web UI login using the given credentials. Security enabled
// implies web login: otherwise the dashboard would be an unauthenticated way
// around the broker's ACLs.
func (s *Server) WithAuth(cfg config.Config) *Server {
	s.users = cfg.Security.Users
	s.authSecret = cfg.Web.AuthSecret
	s.authEnabled = cfg.Security.Enabled || (cfg.Web.Enabled && cfg.Web.Auth)
	s.authorize = cfg.Security.Enabled
	return s
}

// requestPrincipal returns the verified cookie user or the principal mapped to
// a delegated bearer token. The cluster health token never grants a principal.
func (s *Server) requestPrincipal(r *http.Request) string {
	if authorization := r.Header.Get("Authorization"); strings.HasPrefix(authorization, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(authorization, "Bearer "))
		if s.clusterToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.clusterToken)) == 1 {
			return ""
		}
		if s.delegatedTokens == nil || !s.authEnabled || !s.authorize {
			return ""
		}
		principal, ok := s.delegatedTokens.Principal(token)
		if ok && userExists(s.users, principal) {
			return principal
		}
		return ""
	}
	cookie, err := r.Cookie(authCookieName)
	if err != nil {
		return ""
	}
	payload, ok := verifyToken(cookie.Value, s.authSecret)
	if !ok {
		return ""
	}
	principal := authPayloadToUser(payload)
	if !userExists(s.users, principal) {
		return ""
	}
	return principal
}

// authorizeMutations requires cluster Admin (or the more specific ACL) for
// state-changing API calls when security is enabled. Read-only requests are
// left to the authentication middleware.
func (s *Server) authorizeMutations(next http.Handler) http.Handler {
	if !s.authorize {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/auth/") {
			next.ServeHTTP(w, r)
			return
		}
		op := authz.OpWrite
		if strings.Contains(r.URL.Path, "/acls") {
			op = authz.OpAdmin
		}
		res := authz.Resource{Type: authz.ResourceCluster, Name: "cluster"}
		if err := s.aclStore.Authorize(s.requestPrincipal(r), op, res); err != nil {
			writeErr(w, http.StatusForbidden, "forbidden: "+err.Error())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Handler returns the HTTP handler exposing the dashboard and API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Serve the embedded SPA without caching so a rebuilt container always shows
	// the latest frontend (no stale HTML/CSS/JS after docker compose up).
	mux.Handle("/", noCache(http.FileServer(http.FS(webui.FS()))))
	mux.HandleFunc("GET /api/v1/cluster", s.handleCluster)
	mux.HandleFunc("GET /api/v1/topics", s.handleTopics)
	mux.HandleFunc("POST /api/v1/topics", s.handleCreateTopic)
	mux.HandleFunc("DELETE /api/v1/topics/{topic}", s.handleDeleteTopic)
	mux.HandleFunc("GET /api/v1/topics/{topic}/messages", s.handleSearchMessages)
	mux.HandleFunc("POST /api/v1/topics/{topic}/messages", s.handlePostMessage)
	mux.HandleFunc("GET /api/v1/topics/{topic}/partitions", s.handleTopicPartitions)
	mux.HandleFunc("POST /api/v1/topics/{topic}/truncate", s.handleTruncateTopic)
	mux.HandleFunc("POST /api/v1/topics/{topic}/compact", s.handleCompactTopic)
	mux.HandleFunc("GET /api/v1/groups", s.handleGroups)
	mux.HandleFunc("GET /api/v1/groups/{group}", s.handleGroupDetail)
	mux.HandleFunc("DELETE /api/v1/groups/{group}", s.handleDeleteGroup)
	mux.HandleFunc("POST /api/v1/groups/{group}/offsets/reset", s.handleResetGroupOffset)
	mux.HandleFunc("GET /api/v1/groups/{group}/offsets/export", s.handleExportGroupOffsets)
	mux.HandleFunc("POST /api/v1/groups/{group}/offsets/import", s.handleImportGroupOffsets)
	mux.HandleFunc("GET /api/v1/topics/{topic}/config", s.handleTopicConfig)
	mux.HandleFunc("PUT /api/v1/topics/{topic}/config", s.handleTopicConfig)
	mux.HandleFunc("POST /api/v1/topics/{topic}/import", s.handleImportJSONL)
	mux.HandleFunc("GET /api/v1/logs", s.handleBrokerLogs)
	mux.HandleFunc("GET /api/v1/throughput", s.handleThroughput)
	mux.HandleFunc("POST /api/v1/schemas/register", s.handleRegisterSchema)
	mux.HandleFunc("GET /api/v1/mqtt", s.handleMQTTStatus)
	mux.HandleFunc("GET /api/v1/integrations", s.handleIntegrations)
	mux.HandleFunc("GET /api/v1/config", s.handleConfig)
	mux.HandleFunc("GET /api/v1/schemas", s.handleSchemas)
	mux.HandleFunc("GET /api/v1/schemas/{subject}", s.handleSchemaDetail)
	mux.HandleFunc("GET /api/v1/acls", s.handleListACLs)
	mux.HandleFunc("PUT /api/v1/acls", s.handleUpsertACL)
	mux.HandleFunc("DELETE /api/v1/acls", s.handleDeleteACL)
	mux.HandleFunc("GET /api/v1/audit", s.handleAudit)
	mux.HandleFunc("GET /api/v1/topics/{topic}/tail", s.handleTailWS)
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/v1/auth/logout", s.handleLogout)
	mux.HandleFunc("GET /api/v1/auth/status", s.handleAuthStatus)
	mux.HandleFunc("GET /api/v1/auth/delegated-tokens", s.handleListDelegatedTokens)
	mux.HandleFunc("POST /api/v1/auth/delegated-tokens", s.handleCreateDelegatedToken)
	mux.HandleFunc("DELETE /api/v1/auth/delegated-tokens/{id}", s.handleRevokeDelegatedToken)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	// Health monitoring: the aggregate report. The dashboard renders it in the
	// Health view of the embedded SPA (web/dist/index.html).
	mux.HandleFunc("GET /api/v1/health/overview", s.handleHealthOverview)
	// Multi-cluster monitoring: the overview fans out to every registered peer
	// (including this broker), while the registry endpoints edit the list.
	mux.HandleFunc("GET /api/v1/health/clusters", s.handleClustersOverview)
	mux.HandleFunc("GET /api/v1/clusters", s.handleClusters)
	mux.HandleFunc("POST /api/v1/clusters", s.handleUpsertCluster)
	mux.HandleFunc("POST /api/v1/clusters/test", s.handleTestCluster)
	mux.HandleFunc("DELETE /api/v1/clusters/{name}", s.handleDeleteCluster)
	mux.HandleFunc("/api/v1/target/{name}/{path...}", s.handleClusterTarget)
	authed := newAuthMiddleware(s.users, s.authSecret, s.authEnabled, s.clusterToken, func(token string) (string, bool) {
		if s.delegatedTokens == nil || !s.authorize {
			return "", false
		}
		principal, ok := s.delegatedTokens.Principal(token)
		return principal, ok && userExists(s.users, principal)
	})(s.authorizeMutations(mux))
	// securityHeaders is outermost so every response (pages included) carries
	// them; csrfMiddleware then rejects state-changing calls that a cross-site
	// page could have made with the operator's cookie.
	return securityHeaders(csrfMiddleware(authed))
}

// handleMetrics exposes pocketkafka metrics in Prometheus text format.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Write([]byte(s.metrics.Render(s.store, s.gm, s.clusterID)))
}

// noCache disables HTTP caching for embedded frontend assets.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store, max-age=0")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		next.ServeHTTP(w, r)
	})
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// Cluster

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

// Topics

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
	names := s.store.TopicNames()
	out := make([]topicInfo, 0, len(names))
	for _, name := range names {
		t := s.store.GetTopic(name)
		ti := topicInfo{Name: name}
		for i := 0; i < len(t.Partitions); i++ {
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
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]interface{}{"name": req.Name, "partitions": req.Partitions})
}

func (s *Server) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if s.store.GetTopic(topic) == nil {
		writeErr(w, 404, "unknown topic")
		return
	}
	if err := s.store.DeleteTopic(topic); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"deleted": topic})
}

// Messages

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

// pickPartition resolves the target partition for a UI/REST produce:
// explicit partition >= 0 wins; otherwise key hash (FNV-1a) for keyed
// messages; nil key round-robins per topic.
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
	var h uint64 = 14695981039346656037 // FNV-1a 64 offset basis
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
	// Non-nil: an empty decode must serialise as [] rather than null.
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

// Groups

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
	for _, info := range infos {
		item := g{Name: info.Name, State: info.State, Generation: info.Generation, Leader: info.LeaderID, Members: info.Members, Lag: map[string]int64{}}
		for topic, parts := range info.Offsets {
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

// WebSocket live tail

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
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		writeErr(w, 400, "websocket upgrade failed: "+err.Error())
		return
	}
	defer conn.Close()

	// Collect the partitions to tail: one explicit partition or all.
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
		// Drain client frames without waiting for one: a blocking read here paces
		// the whole tail at the read deadline, so records arrive in one-second
		// bursts instead of as they land.
		conn.SetReadDeadline(time.Now())
		readWSPing(conn)

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
					// The read window missed offsets this tick: counting them here
					// is what makes the notice the dashboard shows truthful.
					unread += want - got
				}
				tp.next = hwm
			}
		}
		if len(pending) > 0 {
			// Oldest first; tag each record with its partition (already set).
			frame, capped := capTailFrame(pending, tailMaxPerFrame)
			if err := writeWSFrame(conn, mustJSON(frame)); err != nil {
				return
			}
			unread += int64(capped)
		}
		if unread > 0 {
			// A notice frame, not records: the dashboard keeps its view bounded
			// and tells the operator how much it passed over instead of silently
			// falling behind under heavy ingest.
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

// tailMaxPerFrame bounds one live-tail frame: a browser cannot render tens of
// thousands of rows per tick, so the newest are sent and the rest reported as
// skipped. A live tail shows what just happened, not a backlog it cannot display.
const tailMaxPerFrame = 2000

// capTailFrame keeps at most max records, preferring the newest, and returns how
// many were dropped.
func capTailFrame(pending []messageRecord, max int) ([]messageRecord, int) {
	if max <= 0 || len(pending) <= max {
		return pending, 0
	}
	return pending[len(pending)-max:], len(pending) - max
}

// mustJSON marshals or returns an empty array on failure.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("[]")
	}
	return b
}
