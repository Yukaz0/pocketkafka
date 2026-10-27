package web

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/gateway"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

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

var brokerLogs = &logRing{}

var LogRing ringWriter = ringWriter{brokerLogs}

func (s *Server) InstallLogSink() {
	log.SetOutput(io.MultiWriter(os.Stderr, LogRing))
}

type ringWriter struct{ ring *logRing }

func (w ringWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(string(p), "\n") {
		if strings.TrimSpace(line) != "" {
			w.ring.add(line)
		}
	}
	return len(p), nil
}

func (s *Server) handleBrokerLogs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, brokerLogs.snapshot())
}

func (s *Server) handleImportJSONL(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)

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
	s.auditRequest(r, "topic.import", topic, fmt.Sprintf("imported=%d records", count))
	writeJSON(w, 200, map[string]any{"topic": topic, "imported": count})
}

type offsetSnapshotRow struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata,omitempty"`
}

func (s *Server) handleExportGroupOffsets(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	if !s.gm.GroupExists(group) {
		writeErr(w, 404, "unknown group")
		return
	}
	principal := s.requestPrincipal(r)
	offs := s.gm.GroupOffsetsSnapshot(group)
	visible := make(map[string]map[int32]int64, len(offs))
	for topic, parts := range offs {
		if s.can(principal, authz.OpDescribe, authz.Resource{Type: authz.ResourceTopic, Name: topic}) {
			visible[topic] = parts
		}
	}
	writeJSON(w, 200, map[string]any{
		"group":    group,
		"exported": time.Now().UTC().Format(time.RFC3339),
		"offsets":  visible,
	})
}

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
	principal := s.requestPrincipal(r)
	for _, row := range req.Offsets {
		if row.Topic == "" {
			continue
		}
		if !s.can(principal, authz.OpDescribe, authz.Resource{Type: authz.ResourceTopic, Name: row.Topic}) {
			writeErr(w, http.StatusForbidden, "forbidden: may not set offsets for topic "+row.Topic)
			return
		}
	}
	applied := 0
	for _, row := range req.Offsets {
		if row.Topic == "" {
			continue
		}
		if p := s.store.GetPartition(row.Topic, row.Partition); p == nil {
			continue
		}
		if err := s.gm.ResetOffsets(group, row.Topic, row.Partition, row.Offset); err != nil {
			writeErr(w, 500, err.Error())
			return
		}
		applied++
	}
	writeJSON(w, 200, map[string]any{"group": group, "applied": applied})
}

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

type captureResponse struct {
	header http.Header
	buf    bytes.Buffer
	code   int
}

func (c *captureResponse) Header() http.Header         { return c.header }
func (c *captureResponse) Write(b []byte) (int, error) { return c.buf.Write(b) }
func (c *captureResponse) WriteHeader(code int)        { c.code = code }

func (s *Server) handleMQTTStatus(w http.ResponseWriter, r *http.Request) {
	clients := []gateway.MQTTClientInfo{}
	status := map[string]any{
		"enabled":     false,
		"listen":      s.mqttListen,
		"connections": int32(0),
		"bridged":     int64(0),
		"clients":     clients,
	}
	if s.mqtt != nil && s.mqttRunning {
		status["enabled"] = true
		status["connections"] = s.mqtt.ActiveClients()
		status["bridged"] = s.mqtt.BridgedCount()
		snap := s.mqtt.ClientsSnapshot()
		if snap != nil {
			status["clients"] = snap
		}
	}
	writeJSON(w, 200, status)
}

func (s *Server) handleMQTTPublish(w http.ResponseWriter, r *http.Request) {
	if s.mqtt == nil || !s.mqttRunning {
		writeErr(w, 503, "mqtt bridge is not running")
		return
	}
	var req struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
		QoS     int    `json:"qos"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	req.Topic = strings.TrimSpace(req.Topic)
	if req.Topic == "" {
		writeErr(w, 400, "topic is required")
		return
	}
	if req.QoS < 0 || req.QoS > 2 {
		writeErr(w, 400, "qos must be 0, 1, or 2")
		return
	}
	principal := s.actorFrom(r)
	if err := s.mqtt.Publish(principal, req.Topic, []byte(req.Payload)); err != nil {
		var denied *authz.DeniedError
		if errors.As(err, &denied) {
			writeErr(w, 403, err.Error())
			return
		}
		writeErr(w, 502, err.Error())
		return
	}
	kafkaTopic := gateway.MQTTTopicToKafka(req.Topic)
	s.auditRequest(r, "mqtt.publish", req.Topic, fmt.Sprintf("kafka_topic=%s qos=%d bytes=%d", kafkaTopic, req.QoS, len(req.Payload)))
	writeJSON(w, 201, map[string]string{
		"topic":      req.Topic,
		"kafkaTopic": kafkaTopic,
	})
}

func (s *Server) handleIntegrations(w http.ResponseWriter, r *http.Request) {
	mqtt := map[string]any{
		"enabled":           false,
		"listen":            s.mqttListen,
		"connections":       int32(0),
		"maxConnections":    gateway.MQTTMaxConnections,
		"bridged":           int64(0),
		"topicPattern":      gateway.MQTTTopicPattern,
		"kafkaTopicPattern": gateway.MQTTKafkaTopicPattern,
		"topicHeader":       gateway.MQTTTopicHeader,
	}
	if s.mqtt != nil && s.mqttRunning {
		mqtt["enabled"] = true
		mqtt["connections"] = s.mqtt.ActiveClients()
		mqtt["bridged"] = s.mqtt.BridgedCount()
	}
	subjects := 0
	if s.sr != nil {
		subjects = len(s.sr.ListSubjects())
	}
	writeJSON(w, 200, map[string]any{
		"mqtt": mqtt,
		"restProxy": map[string]any{
			"enabled": s.gatewayListen != "",
			"listen":  s.gatewayListen,
			"auth":    s.authorize,
			"endpoints": []string{
				"POST /topics/{topic}/messages",
				"GET /topics/{topic}/messages",
			},
		},
		"schemaRegistry": map[string]any{
			"enabled":  s.sr != nil,
			"listen":   s.schemaRegListen,
			"subjects": subjects,
		},
		"metrics": map[string]any{
			"path":             "/metrics",
			"authRequired":     s.authEnabled,
			"grafanaDashboard": "deploy/grafana/dashboard.json",
		},
	})
}

var tpMu sync.Mutex
var tpLastCount int64 = -1
var tpLastTime time.Time
var tpSamples []float64

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

func ThroughputSamples() []float64 {
	tpMu.Lock()
	defer tpMu.Unlock()
	out := make([]float64, len(tpSamples))
	copy(out, tpSamples)
	return out
}

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
