package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strings"
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
		case "/api/v1/cluster", "/api/v1/topics", "/api/v1/groups", "/api/v1/logs", "/api/v1/throughput", "/api/v1/mqtt", "/api/v1/schemas", "/api/v1/acls", "/api/v1/audit", "/api/v1/health/overview":
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
	err := ec.withClient(func(cc externalClusterClient) error {
		switch {
		case path == "/api/v1/topics":
			s.handleKafkaTopics(w, cc)
		case strings.HasPrefix(path, "/api/v1/topics/"):
			topic := strings.Split(path, "/")[4]
			s.handleKafkaTopicPartitions(w, cc, topic)
		case path == "/api/v1/groups":
			s.handleKafkaGroups(w, cc)
		default:
			group := strings.TrimPrefix(path, "/api/v1/groups/")
			s.handleKafkaGroupDetail(w, cc, group)
		}
		return nil
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to connect to external Kafka target")
	}
}

func kafkaReadEndpoint(path string) bool {
	switch path {
	case "/api/v1/cluster", "/api/v1/topics", "/api/v1/groups", "/api/v1/health/overview":
		return true
	}
	parts := strings.Split(path, "/")
	if len(parts) == 6 && parts[1] == "api" && parts[2] == "v1" && parts[3] == "topics" && parts[5] == "partitions" && storage.ValidateTopicName(parts[4]) == nil {
		return true
	}
	if len(parts) == 5 && parts[1] == "api" && parts[2] == "v1" && parts[3] == "groups" && validTargetSegment(parts[4]) {
		return true
	}
	return false
}

func (s *Server) handleKafkaTopics(w http.ResponseWriter, cc externalClusterClient) {
	topics := cc.Topics()
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	out := make([]externalTopicInfo, 0, len(topics))
	for _, topic := range topics {
		ends, err := cc.ListEndOffsets(topic.Name)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
			return
		}
		starts, err := cc.ListStartOffsets(topic.Name)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic start offsets")
			return
		}
		partitions := append([]client.PartitionInfo(nil), topic.Partitions...)
		sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
		item := externalTopicInfo{Name: topic.Name, Partitions: make([]externalTopicPartition, 0, len(partitions))}
		for _, partition := range partitions {
			end, hasEnd := ends[partition.ID]
			start, hasStart := starts[partition.ID]
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
	ends, err := cc.ListEndOffsets(topicName)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic end offsets")
		return
	}
	starts, err := cc.ListStartOffsets(topicName)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to read external Kafka topic start offsets")
		return
	}
	partitions := append([]client.PartitionInfo(nil), topic.Partitions...)
	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })
	out := make([]externalPartitionDetail, 0, len(partitions))
	for _, partition := range partitions {
		end, hasEnd := ends[partition.ID]
		start, hasStart := starts[partition.ID]
		if !hasEnd || !hasStart {
			writeErr(w, http.StatusBadGateway, "external Kafka topic offsets are incomplete")
			return
		}
		out = append(out, externalPartitionDetail{Partition: partition.ID, LogEndOffset: end, Earliest: start})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleKafkaGroups(w http.ResponseWriter, cc externalClusterClient) {
	groups, err := cc.ListGroups()
	if err != nil {
		writeErr(w, http.StatusBadGateway, "unable to list external Kafka consumer groups")
		return
	}
	sort.Strings(groups)
	out := make([]externalGroupInfo, 0, len(groups))
	for _, group := range groups {
		state, err := cc.DescribeGroup(group)
		if err != nil {
			writeErr(w, http.StatusBadGateway, "unable to describe external Kafka consumer group")
			return
		}
		_, lag, err := externalGroupOffsetRows(cc, group)
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
	offsets, _, err := externalGroupOffsetRows(cc, group)
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

func externalGroupOffsetRows(cc externalClusterClient, group string) ([]groupOffsetRow, map[string]int64, error) {
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
	for _, topic := range topics {
		ends, err := cc.ListEndOffsets(topic)
		if err != nil {
			return nil, nil, err
		}
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
