package web

// Batches 2-3 backend features: broker log ring buffer, JSONL import,
// group offset snapshot export/import, topic config (compaction toggle),
// schema registration proxy, and MQTT bridge status.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// ---------------------------------------------------------------------------
// Broker log ring buffer + viewer
// ---------------------------------------------------------------------------

// logRing keeps the most recent broker log lines in memory for the UI viewer.
type logRing struct {
	mu    sync.Mutex
	lines []string
}

const logRingMax = 500

func (r *logRing) add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	if len(r.lines) > logRingMax {
		r.lines = r.lines[len(r.lines)-logRingMax:]
	}
}

func (r *logRing) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

// brokerLogs is the process-wide log ring. main calls InstallLogSink after
// logger.Init so slog lines and stdlib log lines both land in the ring.
var brokerLogs = &logRing{}

// LogRing implements io.Writer and captures broker log lines for the viewer.
var LogRing ringWriter = ringWriter{brokerLogs}

// InstallLogSink routes stdlib log output into the ring while still writing
// to stderr. slog lines reach the ring via logger.InitWithSink from main.
func (s *Server) InstallLogSink() {
	log.SetOutput(io.MultiWriter(os.Stderr, LogRing))
}

// ringWriter adapts logRing to io.Writer, splitting on newlines.
type ringWriter struct{ ring *logRing }

func (w ringWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(string(p), "\n") {
		if strings.TrimSpace(line) != "" {
			w.ring.add(line)
		}
	}
	return len(p), nil
}

// handleBrokerLogs returns the recent broker log lines.
func (s *Server) handleBrokerLogs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, brokerLogs.snapshot())
}

// ---------------------------------------------------------------------------
// JSONL batch import (kebalikan export .jsonl di Data Browser)
// ---------------------------------------------------------------------------

func (s *Server) handleImportJSONL(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

	// Prepare (or auto-create) the target topic.
	if s.store.GetTopic(topic) == nil {
		if _, _, err := s.store.EnsureTopic(topic, 1); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
	}
	p := s.store.GetPartition(topic, 0)
	if p == nil {
		writeErr(w, 500, "partition unavailable")
		return
	}

	type row struct {
		Key     string            `json:"key"`
		Value   json.RawMessage   `json:"value"`
		Headers map[string]string `json:"headers"`
	}
	count := 0
	now := time.Now().UnixMilli()
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec row
		if err := json.Unmarshal(line, &rec); err != nil {
			writeErr(w, 400, fmt.Sprintf("line %d: %v", count+1, err))
			return
		}
		// value: raw JSON stays raw; a quoted string is unquoted.
		var value []byte
		if len(rec.Value) > 0 {
			var sv string
			if json.Unmarshal(rec.Value, &sv) == nil {
				value = []byte(sv)
			} else {
				value = rec.Value
			}
		}
		headers := make([]protocol.RecordHeader, 0, len(rec.Headers))
		for k, v := range rec.Headers {
			headers = append(headers, protocol.RecordHeader{Key: k, Value: []byte(v)})
		}
		batch := &protocol.RecordBatch{
			BaseTimestamp: now,
			MaxTimestamp:  now,
			ProducerID:    -1,
			ProducerEpoch: -1,
			BaseSequence:  -1,
			Records:       []protocol.Record{{Key: []byte(rec.Key), Value: value, Headers: headers}},
		}
		raw, err := protocol.EncodeRecordBatch(batch)
		if err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		if _, err := p.Append(raw); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		count++
	}
	if err := sc.Err(); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"topic": topic, "imported": count})
}

// ---------------------------------------------------------------------------
// Group offset snapshot export / import
// ---------------------------------------------------------------------------

// offsetSnapshotRow is one committed offset in a group snapshot file.
type offsetSnapshotRow struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
}

// handleExportGroupOffsets dumps a group's committed offsets as JSON.
func (s *Server) handleExportGroupOffsets(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	if !s.gm.GroupExists(group) {
		writeErr(w, 404, "unknown group")
		return
	}
	offs := s.gm.GroupOffsetsSnapshot(group)
	writeJSON(w, 200, map[string]any{
		"group":    group,
		"exported": time.Now().UTC().Format(time.RFC3339),
		"offsets":  offs,
	})
}

// handleImportGroupOffsets applies a snapshot exported by handleExportGroupOffsets.
func (s *Server) handleImportGroupOffsets(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	var req struct {
		Offsets []offsetSnapshotRow `json:"offsets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if len(req.Offsets) == 0 {
		writeErr(w, 400, "offsets array required")
		return
	}
	applied := 0
	for _, row := range req.Offsets {
		if row.Topic == "" {
			continue
		}
		if p := s.store.GetPartition(row.Topic, row.Partition); p == nil {
			continue // skip unknown topic/partition
		}
		if err := s.gm.ResetOffsets(group, row.Topic, row.Partition, row.Offset); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		applied++
	}
	writeJSON(w, 200, map[string]any{"group": group, "applied": applied})
}

// ---------------------------------------------------------------------------
// Topic config: cleanup policy (compaction) editor
// ---------------------------------------------------------------------------

func (s *Server) handleTopicConfig(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if r.Method == http.MethodGet {
		if s.store.GetTopic(topic) == nil {
			writeErr(w, 404, "unknown topic")
			return
		}
		writeJSON(w, 200, map[string]any{"topic": topic, "compacted": s.store.IsCompacted(topic)})
		return
	}
	// PUT: toggle compaction, persisted via the topic manifest.
	var req struct {
		Compacted bool `json:"compacted"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if s.store.GetTopic(topic) == nil {
		writeErr(w, 404, "unknown topic")
		return
	}
	if err := s.store.MarkCompacted(topic, req.Compacted); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	s.recordAudit(s.actorFrom(r), "topic.config", topic, fmt.Sprintf("cleanup.policy=%s", map[bool]string{true: "compact", false: "delete"}[req.Compacted]))
	writeJSON(w, 200, map[string]any{"topic": topic, "compacted": req.Compacted})
}

// ---------------------------------------------------------------------------
// Schema registration proxy (uses the embedded registry's Confluent API)
// ---------------------------------------------------------------------------

func (s *Server) handleRegisterSchema(w http.ResponseWriter, r *http.Request) {
	if s.sr == nil {
		writeErr(w, 404, "schema registry disabled")
		return
	}
	var req struct {
		Subject    string `json:"subject"`
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if req.Subject == "" || req.Schema == "" {
		writeErr(w, 400, "subject and schema required")
		return
	}
	if req.SchemaType == "" {
		req.SchemaType = "AVRO"
	}
	// Validate the schema parses as JSON; forward to the registry handler
	// semantics by calling Register through its HTTP handler shape.
	if !json.Valid([]byte(req.Schema)) {
		writeErr(w, 400, "schema must be valid JSON")
		return
	}
	body, _ := json.Marshal(map[string]any{"schema": req.Schema, "schemaType": req.SchemaType})
	httpReq, _ := http.NewRequest("POST", "/subjects/"+req.Subject+"/versions", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	rec := &captureResponse{header: make(http.Header)}
	s.sr.Handler().ServeHTTP(rec, httpReq)
	if rec.code >= 400 {
		writeErr(w, rec.code, strings.TrimSpace(rec.buf.String()))
		return
	}
	s.recordAudit(s.actorFrom(r), "schema.register", req.Subject, fmt.Sprintf("type=%s bytes=%d", req.SchemaType, len(req.Schema)))
	writeJSON(w, rec.code, json.RawMessage(rec.buf.Bytes()))
}

// captureResponse collects the downstream registry response.
type captureResponse struct {
	header http.Header
	buf    bytes.Buffer
	code   int
}

func (c *captureResponse) Header() http.Header         { return c.header }
func (c *captureResponse) Write(b []byte) (int, error) { return c.buf.Write(b) }
func (c *captureResponse) WriteHeader(code int)        { c.code = code }

// ---------------------------------------------------------------------------
// MQTT bridge status
// ---------------------------------------------------------------------------

func (s *Server) handleMQTTStatus(w http.ResponseWriter, r *http.Request) {
	if s.mqtt == nil {
		writeJSON(w, 200, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, 200, map[string]any{
		"enabled":     true,
		"listen":      s.mqttListen,
		"connections": s.mqtt.ActiveClients(),
		"bridged":     s.mqtt.BridgedCount(),
	})
}

// ACL persistence now lives in internal/authz (shared across every ingress);
// the web server only reads and writes through its *authz.Store.

// ---------------------------------------------------------------------------
// Avro decode (minimal): parse the Confluent wire format and interpret the
// schema to walk records without a full Avro codegen.
// ---------------------------------------------------------------------------

// avroInfo reports whether a message value carries the Confluent Avro magic.

// ---------------------------------------------------------------------------
// Throughput samples (Batch 2): the UI polls /api/v1/cluster every few
// seconds; the server tracks message-count deltas here so the sparkline can
// render messages/sec without any client-side history.
// ---------------------------------------------------------------------------

var tpMu sync.Mutex
var tpLastCount int64 = -1
var tpLastTime time.Time
var tpSamples []float64

// NoteThroughput records the current total record count and derives msg/s.
func NoteThroughput(s *storage.Store) {
	var total int64
	for _, t := range s.TopicsSnapshot() {
		for _, p := range t.Partitions {
			total += p.LogEndOffset()
		}
	}
	tpMu.Lock()
	defer tpMu.Unlock()
	now := time.Now()
	if tpLastCount >= 0 && tpLastTime.Before(now) {
		rate := float64(total-tpLastCount) / now.Sub(tpLastTime).Seconds()
		tpSamples = append(tpSamples, rate)
		if len(tpSamples) > 120 {
			tpSamples = tpSamples[len(tpSamples)-120:]
		}
	}
	tpLastCount = total
	tpLastTime = now
}

// ThroughputSamples returns the recent msg/s samples (oldest first).
func ThroughputSamples() []float64 {
	tpMu.Lock()
	defer tpMu.Unlock()
	out := make([]float64, len(tpSamples))
	copy(out, tpSamples)
	return out
}

// handleThroughput exposes the samples plus totals for the UI sparkline.
func (s *Server) handleThroughput(w http.ResponseWriter, r *http.Request) {
	NoteThroughput(s.store)
	var total int64
	for _, t := range s.store.TopicsSnapshot() {
		for _, p := range t.Partitions {
			total += p.LogEndOffset()
		}
	}
	writeJSON(w, 200, map[string]any{"samples": ThroughputSamples(), "totalMessages": total})
}
