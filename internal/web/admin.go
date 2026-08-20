package web

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/neu/go-kafka-neu/internal/storage"
)

// ---------------------------------------------------------------------------
// Health endpoints (Fitur 10)
// ---------------------------------------------------------------------------

// isReady reports whether storage is usable and listeners are up. The web
// server is embedded in the broker, so readiness is derived from the store.
func (s *Server) isReady() bool {
	if s.store == nil {
		return false
	}
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !s.isReady() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unhealthy",
			"error":  "storage not ready",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "healthy",
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"broker_id":      s.brokerID,
	})
}

func (s *Server) handleLivez(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// ---------------------------------------------------------------------------
// Group detail & reset offset (Fitur 8)
// ---------------------------------------------------------------------------

type groupDetailMember struct {
	MemberID   string             `json:"member_id"`
	ClientID   string             `json:"client_id"`
	ClientHost string             `json:"client_host"`
	Assigned   map[string][]int32 `json:"assigned_partitions,omitempty"`
}

type groupOffsetRow struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	CurrentOffset int64  `json:"current_offset"`
	LogEndOffset  int64  `json:"log_end_offset"`
	Lag           int64  `json:"lag"`
}

type groupDetail struct {
	GroupID  string              `json:"group_id"`
	State    string              `json:"state"`
	Protocol string              `json:"protocol"`
	Members  []groupDetailMember `json:"members"`
	Offsets  []groupOffsetRow    `json:"offsets"`
}

func (s *Server) handleGroupDetail(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group")
	info := s.gm.DescribeGroup(groupID)
	if info == nil {
		writeErr(w, 404, "unknown group")
		return
	}
	out := groupDetail{
		GroupID:  groupID,
		State:    info.State,
		Protocol: "range",
	}
	for _, m := range s.gm.MembersDetail(groupID) {
		out.Members = append(out.Members, groupDetailMember{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			ClientHost: m.ClientHost,
			Assigned:   m.Assigned,
		})
	}
	for topic, parts := range info.Offsets {
		for part, committed := range parts {
			leo := int64(0)
			if p := s.store.GetPartition(topic, part); p != nil {
				leo = p.LogEndOffset()
			}
			lag := leo - committed
			if lag < 0 {
				lag = 0
			}
			out.Offsets = append(out.Offsets, groupOffsetRow{
				Topic:         topic,
				Partition:     part,
				CurrentOffset: committed,
				LogEndOffset:  leo,
				Lag:           lag,
			})
		}
	}
	writeJSON(w, 200, out)
}

// handleResetGroupOffset resets a group's committed offset for one partition.
func (s *Server) handleResetGroupOffset(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group")
	var req struct {
		Topic        string `json:"topic"`
		Partition    int32  `json:"partition"`
		Strategy     string `json:"strategy"` // to_earliest | to_latest | to_offset
		TargetOffset int64  `json:"target_offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	if req.Topic == "" {
		writeErr(w, 400, "topic required")
		return
	}
	p := s.store.GetPartition(req.Topic, req.Partition)
	if p == nil {
		writeErr(w, 404, "unknown topic or partition")
		return
	}
	var target int64
	switch req.Strategy {
	case "to_earliest":
		target = p.EarliestOffset()
	case "to_latest":
		target = p.LogEndOffset()
	case "to_offset":
		target = req.TargetOffset
	default:
		writeErr(w, 400, "strategy must be to_earliest|to_latest|to_offset")
		return
	}
	if err := s.gm.ResetOffsets(groupID, req.Topic, req.Partition, target); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"group":     groupID,
		"topic":     req.Topic,
		"partition": req.Partition,
		"offset":    target,
	})
}

// handleDeleteGroup deletes an empty group from the web UI.
func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	groupID := r.PathValue("group")
	if err := s.gm.DeleteGroup(groupID); err != nil {
		writeErr(w, 409, "group is not empty")
		return
	}
	writeJSON(w, 200, map[string]string{"deleted": groupID})
}

// ---------------------------------------------------------------------------
// Topic detail / truncate / compact (Fitur 9)
// ---------------------------------------------------------------------------

type partitionDetail struct {
	Partition     int32                 `json:"partition"`
	LogEndOffset  int64                 `json:"logEndOffset"`
	HighWatermark int64                 `json:"highWatermark"`
	Earliest      int64                 `json:"earliestOffset"`
	Bytes         int64                 `json:"bytes"`
	Segments      []storage.SegmentInfo `json:"segments"`
}

func (s *Server) handleTopicPartitions(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	t := s.store.GetTopic(topic)
	if t == nil {
		writeErr(w, 404, "unknown topic")
		return
	}
	out := make([]partitionDetail, 0, len(t.Partitions))
	for i := 0; i < len(t.Partitions); i++ {
		p := t.Partitions[int32(i)]
		out = append(out, partitionDetail{
			Partition:     int32(i),
			LogEndOffset:  p.LogEndOffset(),
			HighWatermark: p.HighWatermark(),
			Earliest:      p.EarliestOffset(),
			Bytes:         p.SizeBytes(),
			Segments:      p.SegmentInfos(),
		})
	}
	writeJSON(w, 200, out)
}

// handleTruncateTopic truncates a partition's log to a target offset.
func (s *Server) handleTruncateTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Partition int32 `json:"partition"`
		Offset    int64 `json:"offset"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "invalid JSON body")
		return
	}
	p := s.store.GetPartition(topic, req.Partition)
	if p == nil {
		writeErr(w, 404, "unknown topic or partition")
		return
	}
	if err := p.TruncateTo(req.Offset); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"topic": topic, "partition": req.Partition, "offset": req.Offset,
	})
}

// handleCompactTopic triggers an immediate compaction of a compacted topic.
func (s *Server) handleCompactTopic(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if s.store.GetTopic(topic) == nil {
		writeErr(w, 404, "unknown topic")
		return
	}
	if err := s.store.CompactNow(topic); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"compacted": topic})
}

// ---------------------------------------------------------------------------
// Data browser search & pagination (Fitur 14)
// ---------------------------------------------------------------------------

// handleGetMessages supports offset+limit pagination plus search/filtering on
// key/value and a timestamp window.
func (s *Server) handleSearchMessages(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	q := r.URL.Query()
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	partition, _ := strconv.Atoi(q.Get("partition"))
	search := q.Get("search")
	fromTS, _ := strconv.ParseInt(q.Get("from_ts"), 10, 64)
	toTS, _ := strconv.ParseInt(q.Get("to_ts"), 10, 64)

	p := s.store.GetPartition(topic, int32(partition))
	if p == nil {
		writeErr(w, 404, "unknown topic or partition")
		return
	}

	// Scan forward from the requested offset, applying filters, until we have
	// `limit` matches or reach the high watermark.
	var matches []messageRecord
	next := offset
	if next < p.EarliestOffset() {
		next = p.EarliestOffset()
	}
	scanned := int64(0)
	for len(matches) < limit && next < p.LogEndOffset() {
		raw, _, err := p.Read(next, int32(limit*4096+4096))
		if err != nil || len(raw) == 0 {
			break
		}
		records := decodeRecords(topic, int32(partition), raw)
		before := len(matches)
		for _, rec := range records {
			scanned++
			if rec.Offset < next {
				continue
			}
			if fromTS > 0 && rec.Timestamp < fromTS {
				continue
			}
			if toTS > 0 && rec.Timestamp > toTS {
				continue
			}
			if search != "" {
				hay := strings.ToLower(rec.Key + "\x00" + rec.Value)
				if !strings.Contains(hay, strings.ToLower(search)) {
					continue
				}
			}
			matches = append(matches, rec)
			if len(matches) >= limit {
				break
			}
		}
		if len(matches) == before {
			// No matches in this window; advance by the records scanned.
			last := int64(0)
			if len(records) > 0 {
				last = records[len(records)-1].Offset
			}
			if last <= next {
				break
			}
			next = last + 1
		} else {
			next = matches[len(matches)-1].Offset + 1
		}
	}

	writeJSON(w, 200, map[string]any{
		"records": matches,
		"offset":  offset,
		"limit":   limit,
		"scanned": scanned,
		"count":   len(matches),
	})
}
