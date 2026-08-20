package web

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
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

	// Auth (Fitur 12).
	users       []config.SecurityUser
	authSecret  string
	authEnabled bool

	// Enterprise (Fitur 4.5): in-memory visual ACL + audit trail.
	aclMu    sync.RWMutex
	acls     map[string]ACLRule // key "principal|resourceType|resourceName"
	auditMu  sync.Mutex
	auditLog []AuditEntry
}

// New builds a web server bound to the given storage and coordinator.
func New(store *storage.Store, gm *coordinator.GroupManager, sr *schemaregistry.Registry, brokerID int32, clusterID string, version string) *Server {
	return &Server{
		store:     store,
		gm:        gm,
		sr:        sr,
		brokerID:  brokerID,
		clusterID: clusterID,
		version:   version,
		startTime: time.Now(),
		metrics:   metrics.NewRegistry(),
		acls:      make(map[string]ACLRule),
	}
}

// WithAuth enables web UI login using the given credentials.
func (s *Server) WithAuth(cfg config.Config) *Server {
	s.users = cfg.Security.Users
	s.authSecret = cfg.Web.AuthSecret
	s.authEnabled = cfg.Security.Enabled && cfg.Web.Auth
	return s
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
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /livez", s.handleLivez)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return newAuthMiddleware(s.users, s.authSecret, s.authEnabled)(mux)
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

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

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
	})
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

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

func (s *Server) handlePostMessage(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Key     string            `json:"key"`
		Value   string            `json:"value"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if s.store.GetTopic(topic) == nil {
		s.store.EnsureTopic(topic, 1)
	}
	p := s.store.GetPartition(topic, 0)
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
	writeJSON(w, 201, map[string]interface{}{"topic": topic, "partition": 0, "offset": off})
}

func decodeRecords(topic string, partition int32, raw []byte) []messageRecord {
	var out []messageRecord
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

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// WebSocket live tail
// ---------------------------------------------------------------------------

func (s *Server) handleTailWS(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		writeErr(w, 400, "websocket upgrade failed: "+err.Error())
		return
	}
	defer conn.Close()

	// Find a partition (partition 0 for single-partition demo topics).
	var part *storage.Partition
	for _, t := range s.store.TopicsSnapshot() {
		if t.Name == topic {
			part = t.Partitions[0]
			break
		}
	}
	if part == nil {
		writeWSFrame(conn, []byte(`{"error":"unknown topic"}`))
		return
	}

	next := part.HighWatermark()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		// Read a client frame (ping/close) without blocking indefinitely.
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		readWSPing(conn)

		if hwm := part.HighWatermark(); hwm > next {
			raw, _, err := part.Read(next, 1<<20)
			if err == nil && len(raw) > 0 {
				records := decodeRecords(topic, 0, raw)
				data, _ := json.Marshal(records)
				if err := writeWSFrame(conn, data); err != nil {
					return
				}
				next = hwm
			} else {
				next = hwm
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
