package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

const (
	peerTargetTimeout   = 15 * time.Second
	peerTargetMaxBody   = 4 << 20
	peerTargetUserAgent = "PocketKafka-dashboard-proxy"
)

var peerTargetTransport = &http.Transport{
	Proxy:                 nil,
	DialContext:           (&net.Dialer{Timeout: peerTargetTimeout, KeepAlive: 30 * time.Second}).DialContext,
	TLSHandshakeTimeout:   peerTargetTimeout,
	ResponseHeaderTimeout: peerTargetTimeout,
	IdleConnTimeout:       90 * time.Second,
	MaxIdleConns:          100,
	MaxIdleConnsPerHost:   10,
	ExpectContinueTimeout: time.Second,
}

var peerTargetClient = &http.Client{
	Transport: peerTargetTransport,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func peerTargetRouteAllowed(method, path string) bool {
	get := method == http.MethodGet || method == http.MethodHead
	if get {
		switch path {
		case "/api/v1/cluster", "/api/v1/topics", "/api/v1/groups", "/api/v1/logs", "/api/v1/throughput", "/api/v1/mqtt", "/api/v1/integrations", "/api/v1/config", "/api/v1/schemas", "/api/v1/acls", "/api/v1/audit", "/api/v1/health/overview":
			return true
		}
	}
	if (method == http.MethodPost && path == "/api/v1/topics") ||
		((method == http.MethodPut || method == http.MethodDelete) && path == "/api/v1/acls") ||
		(method == http.MethodPost && path == "/api/v1/schemas/register") {
		return true
	}

	if !strings.HasPrefix(path, "/api/v1/") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) < 4 || parts[0] != "api" || parts[1] != "v1" {
		return false
	}
	switch parts[2] {
	case "topics":
		if storage.ValidateTopicName(parts[3]) != nil {
			return false
		}
		if len(parts) == 4 {
			return method == http.MethodDelete
		}
		if len(parts) != 5 {
			return false
		}
		switch parts[4] {
		case "messages":
			return get || method == http.MethodPost
		case "partitions":
			return get
		case "truncate", "compact", "import":
			return method == http.MethodPost
		case "config":
			return get || method == http.MethodPut
		case "tail":
			return method == http.MethodGet
		}
	case "groups":
		if !validTargetSegment(parts[3]) {
			return false
		}
		if len(parts) == 4 {
			return get || method == http.MethodDelete
		}
		if len(parts) == 6 && parts[4] == "offsets" {
			switch parts[5] {
			case "reset", "import":
				return method == http.MethodPost
			case "export":
				return get
			}
		}
	case "schemas":
		if len(parts) == 4 && validTargetSegment(parts[3]) {
			return get
		}
	}
	return false
}

func validTargetSegment(segment string) bool {
	return segment != "" && segment != "." && segment != ".." && !strings.ContainsAny(segment, "/\\")
}

func (s *Server) handleClusterTarget(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	entry, ok := s.clusters.Get(name)
	if !ok {
		writeErr(w, http.StatusNotFound, "target cluster not found")
		return
	}
	path := "/" + strings.TrimLeft(r.PathValue("path"), "/")
	if normalizeKind(entry.Kind) == clusterKindKafka {
		s.handleKafkaClusterTarget(w, r, entry, path)
		return
	}
	if normalizeKind(entry.Kind) != clusterKindPeer {
		writeErr(w, http.StatusNotFound, "target cluster not found")
		return
	}
	if !peerTargetRouteAllowed(r.Method, path) {
		writeErr(w, http.StatusNotFound, "target route not allowed")
		return
	}

	token := s.clusters.DashboardToken(name)
	mutating := r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions
	if mutating {
		if !s.authorize || !s.authEnabled {
			writeErr(w, http.StatusForbidden, "peer mutations require source security and ACLs")
			return
		}
		if token == "" {
			writeErr(w, http.StatusForbidden, "peer mutations require a delegated dashboard token")
			return
		}
	} else if token == "" {
		token = s.clusters.Token(name)
	}

	if strings.HasSuffix(path, "/tail") && isWebSocketUpgrade(r) {
		s.proxyPeerWebSocket(w, r, entry, path, token)
		return
	}
	s.proxyPeerAPI(w, r, entry, path, token)
}

type externalTopicInfo struct {
	Name       string                   `json:"name"`
	Partitions []externalTopicPartition `json:"partitions"`
	TotalBytes *int64                   `json:"totalBytes"`
}

type externalTopicPartition struct {
	Partition int32  `json:"partition"`
	Leader    int32  `json:"leader"`
	LogEnd    int64  `json:"logEndOffset"`
	HighWater *int64 `json:"highWatermark"`
	Earliest  int64  `json:"earliestOffset"`
	Bytes     *int64 `json:"bytes"`
}

type externalPartitionDetail struct {
	Partition     int32                 `json:"partition"`
	LogEndOffset  int64                 `json:"logEndOffset"`
	HighWatermark *int64                `json:"highWatermark"`
	Earliest      int64                 `json:"earliestOffset"`
	Bytes         *int64                `json:"bytes"`
	Segments      []storage.SegmentInfo `json:"segments"`
}

type externalGroupInfo struct {
	Name       string           `json:"name"`
	State      string           `json:"state"`
	Generation *int32           `json:"generation"`
	Leader     *string          `json:"leader"`
	Members    []string         `json:"members"`
	Lag        map[string]int64 `json:"lag"`
}

type externalHistoryPoint struct {
	T              int64    `json:"t"`
	MessagesPerSec float64  `json:"messagesPerSec"`
	BytesPerSec    *float64 `json:"bytesPerSec"`
	ConsumedPerSec float64  `json:"consumedPerSec"`
	Lag            int64    `json:"lag"`
	DiskUsagePct   *float64 `json:"diskUsagePct"`
}

type externalHealthOverview struct {
	GeneratedAtMs int64                  `json:"generatedAtMs"`
	Thresholds    map[string]any         `json:"thresholds"`
	Sampling      samplingInfo           `json:"sampling"`
	Cluster       clusterHealth          `json:"cluster"`
	Topics        []topicHealth          `json:"topics"`
	Attention     []topicHealth          `json:"attention"`
	Groups        []groupSummary         `json:"groups"`
	History       []externalHistoryPoint `json:"history"`
}

func externalHealthReport(report overviewResponse) externalHealthOverview {
	history := make([]externalHistoryPoint, 0, len(report.History))
	for _, point := range report.History {
		history = append(history, externalHistoryPoint{
			T: point.T, MessagesPerSec: point.MessagesPerSec,
			ConsumedPerSec: point.ConsumedPerSec, Lag: point.Lag,
		})
	}
	return externalHealthOverview{
		GeneratedAtMs: report.GeneratedAtMs, Thresholds: report.Thresholds,
		Sampling: report.Sampling, Cluster: report.Cluster, Topics: report.Topics,
		Attention: report.Attention, Groups: report.Groups, History: history,
	}
}

func (s *Server) handleKafkaClusterTarget(w http.ResponseWriter, r *http.Request, entry ClusterEntry, path string) {
	if !kafkaReadEndpoint(path) {
		writeErr(w, http.StatusNotImplemented, "operation is not supported for external Kafka targets")
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "external Kafka target endpoint is read-only")
		return
	}
	// The live tail is long-lived: it dials its own client so the stream never
	// holds the sampler's clientMu.
	if strings.HasSuffix(path, "/tail") {
		if !isWebSocketUpgrade(r) {
			writeErr(w, http.StatusBadRequest, "live tail requires a websocket upgrade")
			return
		}
		s.handleKafkaTailWS(w, r, entry, path)
		return
	}
	if path == "/api/v1/health/overview" || path == "/api/v1/cluster" {
		window := defaultWindow
		if value := r.URL.Query().Get("window"); value != "" {
			if parsed, err := parseWindow(value); err == nil {
				window = parsed
			}
		}
		s.externalFor(entry)
		report, err := s.externalOverview(entry.Name, window, time.Now())
		if err != nil {
			writeErr(w, http.StatusBadGateway, "cluster "+entry.Name+": "+err.Error())
			return
		}
		if path == "/api/v1/cluster" {
			writeJSON(w, http.StatusOK, report.Cluster)
			return
		}
		writeJSON(w, http.StatusOK, externalHealthReport(report))
		return
	}

	ec := s.externalFor(entry)
	// The dashboard polls the topic and group lists every few seconds, and one
	// live read of a remote cluster costs tens of round trips (34 groups = 34
	// OffsetFetch calls). The cache answers a poll or a page reload from the last
	// read, so a reload never waits for the scan again and one tab cannot keep
	// re-reading production brokers on every tick.
	cacheable := path == "/api/v1/topics" || path == "/api/v1/groups"
	if cacheable {
		if body, ok := ec.cachedList(path, func() { s.refreshKafkaList(entry, path) }); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Write(body)
			return
		}
	}
	if !cacheable {
		err := ec.withReadClient(func(cc externalClusterClient) error {
			s.dispatchKafkaTarget(w, cc, path, r.URL.Query())
			return nil
		})
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to connect to external Kafka target")
		}
		return
	}
	// Cold cache: the first request after a restart reads the cluster inline and
	// leaves the answer for every request after it.
	captured := &captureResponse{header: make(http.Header)}
	err := ec.withReadClient(func(cc externalClusterClient) error {
		s.dispatchKafkaTarget(captured, cc, path, r.URL.Query())
		return nil
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to connect to external Kafka target")
		return
	}
	if captured.code == http.StatusOK {
		ec.storeList(path, captured.buf.Bytes())
	}
	for key, values := range captured.header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(captured.code)
	w.Write(captured.buf.Bytes())
}

// dispatchKafkaTarget routes one read to the external cluster's handler.
func (s *Server) dispatchKafkaTarget(w http.ResponseWriter, cc externalClusterClient, path string, q url.Values) {
	switch {
	case path == "/api/v1/topics":
		s.handleKafkaTopics(w, cc)
	case strings.HasPrefix(path, "/api/v1/topics/"):
		parts := strings.Split(path, "/")
		topic := parts[4]
		if len(parts) == 6 && parts[5] == "messages" {
			s.handleKafkaMessages(w, cc, topic, q)
		} else {
			s.handleKafkaTopicPartitions(w, cc, topic)
		}
	case path == "/api/v1/groups":
		s.handleKafkaGroups(w, cc)
	default:
		group := strings.TrimPrefix(path, "/api/v1/groups/")
		s.handleKafkaGroupDetail(w, cc, group)
	}
}

// refreshKafkaList re-reads one cached target list in the background.
func (s *Server) refreshKafkaList(entry ClusterEntry, path string) {
	ec := s.externalFor(entry)
	captured := &captureResponse{header: make(http.Header)}
	if err := ec.withReadClient(func(cc externalClusterClient) error {
		s.dispatchKafkaTarget(captured, cc, path, url.Values{})
		return nil
	}); err != nil {
		return
	}
	if captured.code == http.StatusOK {
		ec.storeList(path, captured.buf.Bytes())
	}
}

func kafkaReadEndpoint(path string) bool {
	switch path {
	case "/api/v1/cluster", "/api/v1/topics", "/api/v1/groups", "/api/v1/health/overview":
		return true
	}
	parts := strings.Split(path, "/")
	if len(parts) == 6 && parts[1] == "api" && parts[2] == "v1" && parts[3] == "topics" && storage.ValidateTopicName(parts[4]) == nil {
		if parts[5] == "partitions" || parts[5] == "messages" || parts[5] == "tail" {
			return true
		}
	}
	if len(parts) == 5 && parts[1] == "api" && parts[2] == "v1" && parts[3] == "groups" && validTargetSegment(parts[4]) {
		return true
	}
	return false
}

// kafkaTopicOffsets reads one timestamp position (-1 latest, -2 earliest) for
// every partition of every topic. One bulk request per broker replaces two round
// trips per topic, which is what makes a 49-topic cluster answer in a second
// instead of twelve over a remote link. Internal topics are not part of the bulk
// pass, so they are read individually. If the bulk request fails outright the
// slower per-topic path still answers.
func kafkaTopicOffsets(cc externalClusterClient, topics []client.TopicInfo, ts int64) (map[string]map[int32]int64, error) {
	bulk, err := cc.ListOffsetsBulk(ts)
	if err != nil {
		bulk = map[string]map[int32]int64{}
	}
	out := make(map[string]map[int32]int64, len(topics))
	for _, topic := range topics {
		if offsets, ok := bulk[topic.Name]; ok && !topic.Internal {
			out[topic.Name] = offsets
			continue
		}
		var offsets map[int32]int64
		if ts == -2 {
			offsets, err = cc.ListStartOffsets(topic.Name)
		} else {
			offsets, err = cc.ListEndOffsets(topic.Name)
		}
		if err != nil {
			return nil, err
		}
		out[topic.Name] = offsets
	}
	return out, nil
}

func (s *Server) handleKafkaTopics(w http.ResponseWriter, cc externalClusterClient) {
	topics := cc.Topics()
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	ends, err := kafkaTopicOffsets(cc, topics, -1)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
		return
	}
	starts, err := kafkaTopicOffsets(cc, topics, -2)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic start offsets")
		return
	}
	out := make([]externalTopicInfo, 0, len(topics))
	for _, topic := range topics {
		partitions := append([]client.PartitionInfo(nil), topic.Partitions...)
		sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
		item := externalTopicInfo{Name: topic.Name, Partitions: make([]externalTopicPartition, 0, len(partitions))}
		for _, partition := range partitions {
			end, hasEnd := ends[topic.Name][partition.ID]
			start, hasStart := starts[topic.Name][partition.ID]
			if !hasEnd || !hasStart {
				writeErr(w, http.StatusBadGateway, "external Kafka topic offsets are incomplete")
				return
			}
			item.Partitions = append(item.Partitions, externalTopicPartition{
				Partition: partition.ID,
				Leader:    partition.Leader,
				LogEnd:    end,
				Earliest:  start,
			})
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleKafkaTopicPartitions(w http.ResponseWriter, cc externalClusterClient, topicName string) {
	var topic *client.TopicInfo
	for _, candidate := range cc.Topics() {
		if candidate.Name == topicName {
			found := candidate
			topic = &found
			break
		}
	}
	if topic == nil {
		writeErr(w, http.StatusNotFound, "unknown topic")
		return
	}
	ends, err := kafkaTopicOffsets(cc, []client.TopicInfo{*topic}, -1)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
		return
	}
	starts, err := kafkaTopicOffsets(cc, []client.TopicInfo{*topic}, -2)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic start offsets")
		return
	}
	partitions := append([]client.PartitionInfo(nil), topic.Partitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
	out := make([]externalPartitionDetail, 0, len(partitions))
	for _, partition := range partitions {
		end, hasEnd := ends[topicName][partition.ID]
		start, hasStart := starts[topicName][partition.ID]
		if !hasEnd || !hasStart {
			writeErr(w, http.StatusBadGateway, "external Kafka topic offsets are incomplete")
			return
		}
		out = append(out, externalPartitionDetail{Partition: partition.ID, LogEndOffset: end, Earliest: start})
	}
	writeJSON(w, http.StatusOK, out)
}

// handleKafkaMessages serves a bounded page of records read straight from the
// external cluster's log. It mirrors the local Data Browser response shape so
// the SPA renders both targets with the same code.
func (s *Server) handleKafkaMessages(w http.ResponseWriter, cc externalClusterClient, topic string, q url.Values) {
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	partition := int32(0)
	if v := q.Get("partition"); v != "" && v != "all" {
		p, _ := strconv.Atoi(v)
		partition = int32(p)
	}
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	if offset < 0 {
		offset = 0
	}
	search := q.Get("search")
	fromTS, _ := strconv.ParseInt(q.Get("from_ts"), 10, 64)
	toTS, _ := strconv.ParseInt(q.Get("to_ts"), 10, 64)

	// No offset means "the newest page". A monitoring view opens on a live
	// production topic, where the beginning of the log is both useless and
	// expensive to reach: a compacted partition with hundreds of millions of
	// offsets behind the request takes seconds to scan. An explicit offset
	// (offset=0 from the Beginning button, or a number) is honoured as given.
	var endOffset int64 = -1
	if ends, err := cc.ListEndOffsets(topic); err == nil {
		if end, ok := ends[partition]; ok {
			endOffset = end
			if q.Get("offset") == "" {
				offset = end - int64(limit)
				if offset < 0 {
					offset = 0
				}
			}
		}
	}

	starts, err := cc.ListStartOffsets(topic)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic start offsets")
		return
	}
	if earliest, ok := starts[partition]; ok && offset < earliest {
		offset = earliest
	}

	// Each fetch returns a whole record batch, so a filtered page costs a
	// bounded number of round trips rather than one request per record.
	const pageBytes = 1 << 20
	matches := []messageRecord{}
	next := offset
	truncated := false
	searchLower := strings.ToLower(search)
	for rounds := 0; len(matches) < limit && rounds < 16; rounds++ {
		if endOffset >= 0 && next >= endOffset {
			break
		}
		records, after, err := cc.FetchRecords(topic, partition, next, pageBytes)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to read external Kafka records")
			return
		}
		if len(records) == 0 {
			break
		}
		for _, rec := range records {
			if fromTS > 0 && rec.Timestamp < fromTS {
				continue
			}
			if toTS > 0 && rec.Timestamp > toTS {
				continue
			}
			if searchLower != "" && !kafkaRecordMatches(rec, searchLower) {
				continue
			}
			matches = append(matches, kafkaMessageRecord(topic, partition, rec))
			if len(matches) >= limit {
				truncated = true
				break
			}
		}
		if after <= next || (endOffset >= 0 && after >= endOffset) {
			break
		}
		next = after
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records":    matches,
		"offset":     offset,
		"limit":      limit,
		"count":      len(matches),
		"nextOffset": next,
		"truncated":  truncated,
	})
}

func kafkaRecordMatches(rec client.FetchedRecord, searchLower string) bool {
	return strings.Contains(strings.ToLower(string(rec.Key)), searchLower) ||
		strings.Contains(strings.ToLower(string(rec.Value)), searchLower)
}

func kafkaMessageRecord(topic string, partition int32, rec client.FetchedRecord) messageRecord {
	headers := map[string]string{}
	for _, h := range rec.Headers {
		headers[h.Key] = string(h.Value)
	}
	return messageRecord{
		Topic:     topic,
		Partition: partition,
		Offset:    rec.Offset,
		Timestamp: rec.Timestamp,
		Key:       string(rec.Key),
		Value:     string(rec.Value),
		ValueB64:  base64.StdEncoding.EncodeToString(rec.Value),
		ValueSize: len(rec.Value),
		IsJSON:    json.Valid(rec.Value),
		Headers:   headers,
	}
}

// handleKafkaTailWS streams records that arrive after the connection opens. It
// starts each partition at its current end and forwards what it finds, exactly
// like the local tail. It holds its own client, never the sampler's.
func (s *Server) handleKafkaTailWS(w http.ResponseWriter, r *http.Request, entry ClusterEntry, path string) {
	topic := strings.Split(path, "/")[4]
	qp := r.URL.Query().Get("partition")
	all := qp == "" || qp == "all"
	var partID int32
	if !all {
		pid, _ := strconv.Atoi(qp)
		partID = int32(pid)
	}
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "websocket upgrade failed: "+err.Error())
		return
	}
	defer conn.Close()

	ec := s.externalFor(entry)
	cc, err := ec.newReadClient()
	if err != nil {
		writeWSFrame(conn, []byte(`{"error":"unable to connect to external Kafka target"}`))
		return
	}
	defer cc.Close()

	type tailPart struct {
		id   int32
		next int64
	}
	var parts []tailPart
	if ends, err := cc.ListEndOffsets(topic); err == nil {
		if all {
			for id, off := range ends {
				parts = append(parts, tailPart{id: id, next: off})
			}
			sort.Slice(parts, func(i, j int) bool { return parts[i].id < parts[j].id })
		} else if off, ok := ends[partID]; ok {
			parts = []tailPart{{id: partID, next: off}}
		}
	}
	if len(parts) == 0 {
		writeWSFrame(conn, []byte(`{"error":"unknown topic"}`))
		return
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	// fetchPart reads one partition from its cursor and returns the records plus
	// the cursor that follows them. Each partition is fetched on its own goroutine:
	// a tick costs one round trip instead of one per partition, which is what the
	// stream's smoothness depends on over a remote link.
	fetchPart := func(tp *tailPart) []messageRecord {
		records, after, err := cc.FetchRecords(topic, tp.id, tp.next, 1<<20)
		if err != nil {
			// A transient broker error must not tear the stream down; the next
			// tick retries from the same offset.
			return nil
		}
		if len(records) == 0 {
			if after > tp.next {
				tp.next = after
			}
			return nil
		}
		out := make([]messageRecord, 0, len(records))
		for _, rec := range records {
			out = append(out, kafkaMessageRecord(topic, tp.id, rec))
		}
		tp.next = after
		return out
	}

	for {
		// Drain client frames without waiting for one. A blocking read here paces
		// the whole tail at the read deadline, which delivers records in
		// one-second bursts instead of as they land.
		conn.SetReadDeadline(time.Now())
		readWSPing(conn)

		var pending []messageRecord
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := range parts {
			wg.Add(1)
			go func(tp *tailPart) {
				defer wg.Done()
				records := fetchPart(tp)
				if len(records) == 0 {
					return
				}
				mu.Lock()
				pending = append(pending, records...)
				mu.Unlock()
			}(&parts[i])
		}
		wg.Wait()
		if len(pending) > 1 {
			// Two partitions answer out of order; the table reads oldest first.
			sort.SliceStable(pending, func(i, j int) bool {
				if pending[i].Partition != pending[j].Partition {
					return pending[i].Partition < pending[j].Partition
				}
				return pending[i].Offset < pending[j].Offset
			})
		}
		if len(pending) > 0 {
			frame, capped := capTailFrame(pending, tailMaxPerFrame)
			if err := writeWSFrame(conn, mustJSON(frame)); err != nil {
				return
			}
			if capped > 0 {
				if err := writeWSFrame(conn, mustJSON(map[string]int64{"skipped": int64(capped)})); err != nil {
					return
				}
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) handleKafkaGroups(w http.ResponseWriter, cc externalClusterClient) {
	groups, err := cc.ListGroups()
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to list external Kafka consumer groups")
		return
	}
	sort.Strings(groups)
	// One DescribeGroups request per broker covers every group, and one bulk
	// offset pass covers every partition lag. Asking per group and per partition
	// turned a page of 34 groups into hundreds of round trips.
	states, err := cc.DescribeGroupsAll(groups)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to describe external Kafka consumer groups")
		return
	}
	ends, err := kafkaTopicOffsets(cc, cc.Topics(), -1)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
		return
	}
	out := make([]externalGroupInfo, 0, len(groups))
	for _, group := range groups {
		state, ok := states[group]
		if !ok {
			state = client.GroupState{Group: group, State: "Unknown"}
		}
		_, lag, err := externalGroupOffsetRows(cc, group, ends)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to read external Kafka consumer group offsets")
			return
		}
		out = append(out, externalGroupInfo{Name: group, State: state.State, Lag: lag})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleKafkaGroupDetail(w http.ResponseWriter, cc externalClusterClient, group string) {
	groups, err := cc.ListGroups()
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to list external Kafka consumer groups")
		return
	}
	found := false
	for _, candidate := range groups {
		if candidate == group {
			found = true
			break
		}
	}
	if !found {
		writeErr(w, http.StatusNotFound, "unknown group")
		return
	}
	state, err := cc.DescribeGroup(group)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to describe external Kafka consumer group")
		return
	}
	ends, err := kafkaTopicOffsets(cc, cc.Topics(), -1)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
		return
	}
	offsets, _, err := externalGroupOffsetRows(cc, group, ends)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka consumer group offsets")
		return
	}
	protocol := state.ProtocolType
	if protocol == "" {
		protocol = "range"
	}
	writeJSON(w, http.StatusOK, groupDetail{GroupID: group, State: state.State, Protocol: protocol, Offsets: offsets})
}

func externalGroupOffsetRows(cc externalClusterClient, group string, ends map[string]map[int32]int64) ([]groupOffsetRow, map[string]int64, error) {
	committed, err := cc.GroupOffsets(group)
	if err != nil {
		return nil, nil, err
	}
	topics := make([]string, 0, len(committed))
	for topic := range committed {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	rows := make([]groupOffsetRow, 0)
	lagByPartition := make(map[string]int64)
	// ends (one bulk pass over every partition) is what turns lag into a single
	// round trip per group; without it each committed topic costs one ListOffsets
	// request per partition, which is hundreds of requests for a page of groups.
	local := map[string]map[int32]int64{}
	for _, topic := range topics {
		topicEnds, ok := ends[topic]
		if !ok || len(topicEnds) == 0 {
			topicEnds, err = cc.ListEndOffsets(topic)
			if err != nil {
				return nil, nil, err
			}
		}
		local[topic] = topicEnds
	}
	for _, topic := range topics {
		ends := local[topic]
		partitions := make([]int, 0, len(committed[topic]))
		for partition := range committed[topic] {
			partitions = append(partitions, int(partition))
		}
		sort.Ints(partitions)
		for _, rawPartition := range partitions {
			partition := int32(rawPartition)
			end, ok := ends[partition]
			if !ok {
				return nil, nil, errors.New("external Kafka group offsets are incomplete")
			}
			current := committed[topic][partition]
			lag := nonNegative(end - current)
			rows = append(rows, groupOffsetRow{Topic: topic, Partition: partition, CurrentOffset: current, LogEndOffset: end, Lag: lag})
			lagByPartition[fmt.Sprintf("%s-%d", topic, partition)] = lag
		}
	}
	return rows, lagByPartition, nil
}

func isWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") && headerContains(r.Header.Get("Connection"), "upgrade")
}

func (s *Server) proxyPeerAPI(w http.ResponseWriter, r *http.Request, entry ClusterEntry, path, token string) {
	target, err := peerTargetURL(entry.URL, path, r.URL.RawQuery)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "peer target URL is invalid")
		return
	}
	if r.ContentLength > peerTargetMaxBody {
		writeErr(w, http.StatusRequestEntityTooLarge, "request body exceeds proxy limit")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), peerTargetTimeout)
	defer cancel()
	var body io.Reader
	if r.Body != nil {
		body = http.MaxBytesReader(w, r.Body, peerTargetMaxBody)
	}
	request, err := http.NewRequestWithContext(ctx, r.Method, target.String(), body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid peer request")
		return
	}
	request.ContentLength = r.ContentLength
	copyRequestHeader(request.Header, r.Header, "Accept")
	copyRequestHeader(request.Header, r.Header, "Content-Type")
	request.Header.Set("User-Agent", peerTargetUserAgent)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := peerTargetClient.Do(request)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "peer request failed")
		return
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		writeErr(w, http.StatusBadGateway, "peer redirects are not followed")
		return
	}
	if response.ContentLength > peerTargetMaxBody {
		writeErr(w, http.StatusBadGateway, "peer response exceeds proxy limit")
		return
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, peerTargetMaxBody+1))
	if err != nil || len(payload) > peerTargetMaxBody {
		writeErr(w, http.StatusBadGateway, "peer response failed or exceeds proxy limit")
		return
	}
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(response.StatusCode)
	if r.Method != http.MethodHead && response.StatusCode != http.StatusNoContent {
		_, _ = w.Write(payload)
	}
}

func (s *Server) proxyPeerWebSocket(w http.ResponseWriter, r *http.Request, entry ClusterEntry, path, token string) {
	target, err := peerTargetURL(entry.URL, path, r.URL.RawQuery)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "peer target URL is invalid")
		return
	}
	proxy := &httputil.ReverseProxy{
		Transport: peerTargetTransport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.Out.URL.Path = target.Path
			request.Out.URL.RawPath = ""
			request.Out.URL.RawQuery = target.RawQuery
			request.Out.Header = make(http.Header)
			for _, name := range []string{"Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
				copyRequestHeader(request.Out.Header, request.In.Header, name)
			}
			request.Out.Header.Set("Connection", "Upgrade")
			request.Out.Header.Set("Upgrade", "websocket")
			if token != "" {
				request.Out.Header.Set("Authorization", "Bearer "+token)
			}
		},
		ModifyResponse: func(response *http.Response) error {
			if response.StatusCode >= 300 && response.StatusCode < 400 {
				return errors.New("peer redirect rejected")
			}
			if response.ContentLength > peerTargetMaxBody {
				return errors.New("peer response exceeds proxy limit")
			}
			allowed := make(http.Header)
			if response.StatusCode == http.StatusSwitchingProtocols {
				for _, name := range []string{"Connection", "Upgrade", "Sec-WebSocket-Accept", "Sec-WebSocket-Protocol", "Sec-WebSocket-Extensions"} {
					copyRequestHeader(allowed, response.Header, name)
				}
			} else {
				copyRequestHeader(allowed, response.Header, "Content-Type")
			}
			response.Header = allowed
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeErr(w, http.StatusBadGateway, "peer websocket request failed")
		},
	}
	proxy.ServeHTTP(w, r)
}

func peerTargetURL(rawURL, path, query string) (*url.URL, error) {
	target, err := url.Parse(rawURL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" || target.User != nil || target.Fragment != "" {
		return nil, errors.New("invalid peer URL")
	}
	target.Path = strings.TrimRight(target.Path, "/") + path
	target.RawPath = ""
	target.RawQuery = query
	return target, nil
}

func copyRequestHeader(destination, source http.Header, name string) {
	for _, value := range source.Values(name) {
		destination.Add(name, value)
	}
}
