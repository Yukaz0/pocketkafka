// Package gateway implements multi-protocol HTTP ingress for go-kafka-neu: a
// lightweight REST proxy (port 8082) that lets non-Kafka services publish and
// consume messages over plain HTTP.
package gateway

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/neu/go-kafka-neu/internal/storage"
	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// RESTProxy is the HTTP REST proxy gateway.
type RESTProxy struct {
	store *storage.Store
}

// NewRESTProxy builds a REST proxy bound to the storage engine.
func NewRESTProxy(store *storage.Store) *RESTProxy {
	return &RESTProxy{store: store}
}

// Handler returns the HTTP handler for the REST proxy.
func (p *RESTProxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /topics/{topic}/messages", p.publish)
	mux.HandleFunc("GET /topics/{topic}/messages", p.consume)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// publish produces a message to a topic via the storage engine.
func (p *RESTProxy) publish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Key     string            `json:"key"`
		Value   interface{}       `json:"value"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON body"})
		return
	}
	// Accept both raw JSON values and strings for the value.
	var value []byte
	switch v := req.Value.(type) {
	case nil:
		value = nil
	case string:
		value = []byte(v)
	default:
		value, _ = json.Marshal(v)
	}

	if p.store.GetTopic(topic) == nil {
		p.store.EnsureTopic(topic, 1)
	}
	part := p.store.GetPartition(topic, 0)
	if part == nil {
		writeJSON(w, 500, map[string]string{"error": "partition unavailable"})
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
			Value:   value,
			Headers: headers,
		}},
	}
	raw, err := protocol.EncodeRecordBatch(batch)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	off, err := part.Append(raw)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 201, map[string]interface{}{"topic": topic, "partition": 0, "offset": off})
}

// consume fetches messages from a topic with optional offset/limit pagination.
func (p *RESTProxy) consume(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	partition, _ := strconv.Atoi(r.URL.Query().Get("partition"))

	part := p.store.GetPartition(topic, int32(partition))
	if part == nil {
		writeJSON(w, 404, map[string]string{"error": "unknown topic or partition"})
		return
	}
	raw, _, err := part.Read(offset, int32(limit*4096+4096))
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	records := decodeRecords(topic, int32(partition), raw)
	if len(records) > limit {
		records = records[:limit]
	}
	writeJSON(w, 200, records)
}

type restRecord struct {
	Topic     string            `json:"topic"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value,omitempty"`
	Headers   map[string]string `json:"headers"`
}

func decodeRecords(topic string, partition int32, raw []byte) []restRecord {
	var out []restRecord
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
				headers := map[string]string{}
				for _, hd := range rec.Headers {
					headers[hd.Key] = string(hd.Value)
				}
				out = append(out, restRecord{
					Topic:     topic,
					Partition: partition,
					Offset:    b.BaseOffset + int64(rec.OffsetDelta),
					Key:       string(rec.Key),
					Value:     string(rec.Value),
					Headers:   headers,
				})
			}
		}
		pos += total
	}
	return out
}
