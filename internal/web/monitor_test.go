package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func newHealthTestServer(t *testing.T) (*Server, *storage.Store, *coordinator.GroupManager) {
	t.Helper()
	store, err := storage.NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	os, err := coordinator.NewOffsetStore("inmemory", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(os, 1, "localhost", 9092, 45000)
	return New(store, gm, nil, 1, "test-cluster", "dev"), store, gm
}

func appendMessages(t *testing.T, p *storage.Partition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		now := time.Now().UnixMilli()
		batch := &protocol.RecordBatch{
			BaseTimestamp: now,
			MaxTimestamp:  now,
			ProducerID:    -1,
			ProducerEpoch: -1,
			BaseSequence:  -1,
			Records:       []protocol.Record{{Key: []byte("k"), Value: []byte("v")}},
		}
		raw, err := protocol.EncodeRecordBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Append(raw); err != nil {
			t.Fatal(err)
		}
	}
}

func codesOf(reasons []healthReason) []string {
	out := make([]string, 0, len(reasons))
	for _, r := range reasons {
		out = append(out, r.Code)
	}
	return out
}

func hasCode(reasons []healthReason, code string) bool {
	for _, r := range reasons {
		if r.Code == code {
			return true
		}
	}
	return false
}

func TestEvaluateTopic(t *testing.T) {
	th := defaultThresholds()
	cases := []struct {
		name     string
		metrics  topicMetrics
		want     string
		wantCode string
	}{
		{
			name:    "flowing topic without consumers",
			metrics: topicMetrics{Partitions: 1, Messages: 500, MessagesPerSec: 12},
			want:    StatusHealthy,
		},
		{
			name:    "empty topic is idle not broken",
			metrics: topicMetrics{Partitions: 1, Messages: 0},
			want:    StatusIdle,
		},
		{
			name:     "backlog growing slowly",
			metrics:  topicMetrics{Partitions: 1, Messages: 5000, Lag: 400, LagPerSec: 80},
			want:     StatusDegraded,
			wantCode: "lag_growth",
		},
		{
			name:     "backlog growing fast is critical",
			metrics:  topicMetrics{Partitions: 1, Messages: 50000, Lag: 20000, LagPerSec: 900},
			want:     StatusCritical,
			wantCode: "lag_growth_critical",
		},
		{
			name:     "large static backlog",
			metrics:  topicMetrics{Partitions: 1, Messages: 50000, Lag: 12000},
			want:     StatusCritical,
			wantCode: "lag_growth_critical",
		},
		{
			name:     "backlog that does not drain is degraded",
			metrics:  topicMetrics{Partitions: 1, Messages: 5000, Lag: 2000},
			want:     StatusDegraded,
			wantCode: "lag_backlog",
		},
		{
			name:     "draining backlog is healthy with an info reason",
			metrics:  topicMetrics{Partitions: 1, Messages: 5000, Lag: 2000, LagPerSec: -300},
			want:     StatusHealthy,
			wantCode: "lag_recovering",
		},
		{
			name:     "hot partition skew",
			metrics:  topicMetrics{Partitions: 4, Messages: 4000, SkewRatio: 5.5},
			want:     StatusDegraded,
			wantCode: "partition_skew",
		},
		{
			name:    "skew on a near-empty topic is ignored",
			metrics: topicMetrics{Partitions: 3, Messages: 1, SkewRatio: 3},
			want:    StatusHealthy,
		},
		{
			name:     "consumer stopped with backlog",
			metrics:  topicMetrics{Partitions: 1, Messages: 4000, Lag: 40, StalledGroups: []string{"billing"}},
			want:     StatusDegraded,
			wantCode: "group_stalled",
		},
		{
			name:     "silent topic is idle",
			metrics:  topicMetrics{Partitions: 1, Messages: 900, LastAppendAge: 20 * time.Minute, HasActivity: true},
			want:     StatusIdle,
			wantCode: "no_recent_produce",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reasons := evaluateTopic(th, tc.metrics)
			if got != tc.want {
				t.Fatalf("status = %q, want %q (reasons %v)", got, tc.want, codesOf(reasons))
			}
			if tc.wantCode != "" && !hasCode(reasons, tc.wantCode) {
				t.Fatalf("missing reason %q, got %v", tc.wantCode, codesOf(reasons))
			}
		})
	}
}

func TestClusterStatusFollowsWorstTopic(t *testing.T) {
	th := defaultThresholds()
	topics := []topicHealth{
		{Name: "a", Status: StatusHealthy},
		{Name: "b", Status: StatusDegraded},
		{Name: "c", Status: StatusIdle},
	}
	status, _ := clusterStatus(th, 10, topics)
	if status != StatusDegraded {
		t.Fatalf("status = %q, want degraded", status)
	}

	status, reasons := clusterStatus(th, 97, topics)
	if status != StatusCritical {
		t.Fatalf("status = %q, want critical on full disk", status)
	}
	if !hasCode(reasons, "disk_critical") {
		t.Fatalf("missing disk_critical reason, got %v", codesOf(reasons))
	}

	status, _ = clusterStatus(th, 10, []topicHealth{{Name: "a", Status: StatusIdle}})
	if status != StatusIdle {
		t.Fatalf("status = %q, want idle when every topic is idle", status)
	}
}

func TestOverviewRatesFromLogEndOffsetDelta(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("orders", 2)
	if err != nil {
		t.Fatal(err)
	}

	base := time.Now().Add(-10 * time.Second)
	s.monitor.Record(store, gm, base, true)
	for i := int32(0); i < 2; i++ {
		appendMessages(t, topic.Partitions[i], 20)
	}
	now := time.Now()
	s.monitor.Record(store, gm, now, true)

	report := s.buildOverview(defaultThresholds(), time.Minute, now)
	if report.Sampling.Warmup {
		t.Fatal("report should not be in warmup after two samples")
	}
	row := report.Topics[0]
	if row.Messages != 40 {
		t.Fatalf("messages = %d, want 40", row.Messages)
	}
	if row.MessagesPerSec < 3 || row.MessagesPerSec > 5 {
		t.Fatalf("messagesPerSec = %v, want about 4", row.MessagesPerSec)
	}
	if report.Cluster.MessagesPerSec < 3 || report.Cluster.MessagesPerSec > 5 {
		t.Fatalf("cluster messagesPerSec = %v, want about 4", report.Cluster.MessagesPerSec)
	}
	if row.ReplicationFactor != 1 {
		t.Fatalf("replicationFactor = %d, want 1 (single node)", row.ReplicationFactor)
	}
	if len(row.PartitionsDetail) != 2 {
		t.Fatalf("partitionsDetail = %d entries, want 2", len(row.PartitionsDetail))
	}
	if len(row.Sparkline) == 0 {
		t.Fatal("expected a sparkline series")
	}
	if len(report.History) == 0 {
		t.Fatal("expected cluster history points")
	}
	if report.Cluster.Status != StatusHealthy {
		t.Fatalf("cluster status = %q, want healthy for a flowing topic", report.Cluster.Status)
	}
	if row.RateReliable {
		t.Fatal("rate should be flagged unreliable for a window the series does not cover")
	}
}

func TestOverviewFlagsStoppedConsumer(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("telemetry", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 50)
	if err := gm.ResetOffsets("scada-reader", "telemetry", 0, 0); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	s.monitor.Record(store, gm, now.Add(-10*time.Second), true)
	s.monitor.Record(store, gm, now, true)

	th := defaultThresholds()
	th.LagWarn = 10
	report := s.buildOverview(th, time.Minute, now)

	row := report.Topics[0]
	if row.Lag != 50 {
		t.Fatalf("lag = %d, want 50", row.Lag)
	}
	if row.Status != StatusDegraded {
		t.Fatalf("status = %q, want degraded (reasons %v)", row.Status, codesOf(row.Reasons))
	}
	if !hasCode(row.Reasons, "lag_backlog") || !hasCode(row.Reasons, "group_stalled") {
		t.Fatalf("expected lag_backlog and group_stalled, got %v", codesOf(row.Reasons))
	}
	if len(report.Groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(report.Groups))
	}
	g := report.Groups[0]
	if g.Name != "scada-reader" || !g.Stalled || g.Members != 0 || g.Lag != 50 || g.State != "Empty" {
		t.Fatalf("unexpected group summary: %+v", g)
	}
	if len(g.Topics) != 1 || g.Topics[0] != "telemetry" {
		t.Fatalf("group topics = %v, want [telemetry]", g.Topics)
	}
	if len(row.Groups) != 1 || row.Groups[0].Group != "scada-reader" {
		t.Fatalf("topic group rows = %+v, want the one group on this topic", row.Groups)
	}
	if report.Cluster.Status != StatusDegraded {
		t.Fatalf("cluster status = %q, want degraded", report.Cluster.Status)
	}
	if len(report.Attention) != 1 || report.Attention[0].Name != "telemetry" {
		t.Fatalf("attention = %+v, want the telemetry topic", report.Attention)
	}
}

func TestHealthOverviewHandler(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("orders", 1)
	if err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, time.Now().Add(-5*time.Second), true)
	appendMessages(t, topic.Partitions[0], 10)

	h := s.Handler()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/health/overview?window=120&lag_warn=5", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rr.Code, rr.Body.String())
	}
	var got struct {
		GeneratedAtMs int64 `json:"generatedAtMs"`
		Thresholds    map[string]any
		Sampling      struct {
			WindowSeconds int `json:"windowSeconds"`
		} `json:"sampling"`
		Cluster struct {
			Status    string `json:"status"`
			Topics    int    `json:"topics"`
			Version   string `json:"version"`
			ClusterID string `json:"clusterId"`
		} `json:"cluster"`
		Topics []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"topics"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON: %v (%s)", err, rr.Body.String())
	}
	if got.GeneratedAtMs == 0 || got.Sampling.WindowSeconds != 120 {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	if got.Cluster.ClusterID != "test-cluster" || got.Cluster.Version != "dev" || got.Cluster.Topics != 1 {
		t.Fatalf("unexpected cluster block: %+v", got.Cluster)
	}
	if len(got.Topics) != 1 || got.Topics[0].Name != "orders" {
		t.Fatalf("unexpected topics: %+v", got.Topics)
	}
	if got.Thresholds["lagWarn"] != float64(5) {
		t.Fatalf("lag_warn override not applied: %v", got.Thresholds["lagWarn"])
	}
}

func TestMonitorPageRetired(t *testing.T) {
	s, _, _ := newHealthTestServer(t)
	h := s.Handler()
	for _, path := range []string{"/monitor", "/monitor.html"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest("GET", path, nil))
		if rr.Code != http.StatusNotFound {
			t.Fatalf("%s: status = %d, want 404 (standalone page retired)", path, rr.Code)
		}
	}
}

func TestDashboardShipsHealthView(t *testing.T) {
	s, _, _ := newHealthTestServer(t)
	h := s.Handler()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, p := range []string{"/js/app.js", "/js/state.js", "/js/render.js", "/js/zones/kafka.js"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", p, nil))
		if rec.Code == http.StatusOK {
			body += rec.Body.String()
		}
	}
	for _, want := range []string{"/api/v1/health/overview", "viewHealth", "'health','Health'"} {
		if !strings.Contains(body, want) {
			t.Fatalf("dashboard is missing %q: the Health view is not wired into the SPA", want)
		}
	}
}

func TestOverviewTopicGroupAttribution(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	withData, err := store.CreateTopic("with-data", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("other", 1); err != nil {
		t.Fatal(err)
	}
	appendMessages(t, withData.Partitions[0], 20)
	if err := gm.ResetOffsets("g1", "with-data", 0, 10); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	s.monitor.Record(store, gm, now, true)
	report := s.buildOverview(defaultThresholds(), time.Minute, now)

	if len(report.Groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(report.Groups))
	}
	if topics := report.Groups[0].Topics; len(topics) != 1 || topics[0] != "with-data" {
		t.Fatalf("group topics = %v, want [with-data]", topics)
	}
	for _, row := range report.Topics {
		if row.Name == "other" && len(row.Groups) != 0 {
			t.Fatalf("topic %q lists %d unrelated groups", row.Name, len(row.Groups))
		}
		if row.Name == "with-data" && len(row.Groups) != 1 {
			t.Fatalf("topic with-data lists %d groups, want 1", len(row.Groups))
		}
	}
}

func TestLastAppendClockSeededOnFirstSight(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("existing", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 5)

	first := time.Now()
	s.monitor.Record(store, gm, first, true)
	later := first.Add(10 * time.Minute)
	s.monitor.Record(store, gm, later, true)

	report := s.buildOverview(defaultThresholds(), time.Minute, later)
	row := report.Topics[0]
	if row.LastAppendAgeMs == nil {
		t.Fatal("lastAppendAgeMs is nil, want the clock seeded at first sight")
	}
	if *row.LastAppendAgeMs != (10 * time.Minute).Milliseconds() {
		t.Fatalf("lastAppendAgeMs = %d, want %d", *row.LastAppendAgeMs, (10 * time.Minute).Milliseconds())
	}
	if row.Status != StatusIdle || !hasCode(row.Reasons, "no_recent_produce") {
		t.Fatalf("status = %q reasons = %v, want idle/no_recent_produce", row.Status, codesOf(row.Reasons))
	}
}

func TestRateReliableRequiresWindowCoverage(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	s.monitor.Record(store, gm, t0, true)
	s.monitor.Record(store, gm, t0.Add(10*time.Second), true)
	now := t0.Add(10 * time.Second)

	short := s.buildOverview(defaultThresholds(), 5*time.Second, now)
	if !short.Sampling.RateReliable {
		t.Fatal("cluster rate should be reliable when the window is covered")
	}
	if !short.Topics[0].RateReliable {
		t.Fatal("topic rate should be reliable when the window is covered")
	}

	long := s.buildOverview(defaultThresholds(), 5*time.Minute, now)
	if long.Sampling.RateReliable {
		t.Fatal("cluster rate must not claim reliability for a window it does not cover")
	}
	if long.Topics[0].RateReliable {
		t.Fatal("topic rate must not claim reliability for a window it does not cover")
	}
	if long.Topics[0].RateSpanSeconds != 10 {
		t.Fatalf("rateSpanSeconds = %v, want 10", long.Topics[0].RateSpanSeconds)
	}
}

func TestMonitorRecordRespectsInterval(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if !s.monitor.Record(store, gm, now, true) {
		t.Fatal("forced sample was not recorded")
	}
	if s.monitor.Record(store, gm, now.Add(time.Millisecond), false) {
		t.Fatal("sample inside the interval should be skipped")
	}
	if !s.monitor.Record(store, gm, now.Add(3*time.Second), false) {
		t.Fatal("sample past the interval should be recorded")
	}
}

func TestMonitorRecordSkipsWorkInsideInterval(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if !s.monitor.Record(store, gm, now, true) {
		t.Fatal("forced sample was not recorded")
	}
	if _, err := store.CreateTopic("late", 1); err != nil {
		t.Fatal(err)
	}
	s.monitor.mu.Lock()
	wantLen := len(s.monitor.topics["t"])
	wantAt := s.monitor.lastAt
	s.monitor.mu.Unlock()

	if s.monitor.Record(store, gm, now.Add(time.Millisecond), false) {
		t.Fatal("back-to-back sample inside the interval should be skipped")
	}

	s.monitor.mu.Lock()
	defer s.monitor.mu.Unlock()
	if got := len(s.monitor.topics["t"]); got != wantLen {
		t.Fatalf("skipped call changed the ring for t: %d -> %d", wantLen, got)
	}
	if _, ok := s.monitor.topics["late"]; ok {
		t.Fatal("skipped call recorded a series for a topic that appeared inside the interval")
	}
	if !s.monitor.lastAt.Equal(wantAt) {
		t.Fatalf("skipped call advanced lastAt: %v -> %v", wantAt, s.monitor.lastAt)
	}
}

func TestTrimSamplesKeepsAgeWindow(t *testing.T) {
	now := time.Now()
	retention := 10 * time.Second
	const n = 64
	orig := make([]topicSample, 0, n)
	for i := range n {
		orig = append(orig, topicSample{At: now.Add(time.Duration(i-(n-1)) * time.Second)})
	}
	got := trimSamples(orig, now, retention, n)
	if len(got) != 11 {
		t.Fatalf("len = %d, want 11", len(got))
	}
	if !got[0].At.Equal(now.Add(-10 * time.Second)) {
		t.Fatalf("first kept sample at %v, want %v", got[0].At, now.Add(-10*time.Second))
	}
	if !got[len(got)-1].At.Equal(now) {
		t.Fatalf("last kept sample at %v, want %v", got[len(got)-1].At, now)
	}
	if &got[0] != &orig[n-len(got)] {
		t.Fatal("age trim copied instead of reslicing the backing array")
	}
}

func TestTrimSamplesKeepsTailCap(t *testing.T) {
	now := time.Now()
	ring := make([]topicSample, 0, 8)
	for i := range 5 {
		ring = append(ring, topicSample{At: now.Add(time.Duration(i-5) * time.Second)})
	}
	ring = trimSamples(ring, now, 4*time.Second, 8)
	if len(ring) != 4 {
		t.Fatalf("len after age trim = %d, want 4", len(ring))
	}
	base := &ring[0]
	ring = append(ring, topicSample{At: now})
	if &ring[0] != base {
		t.Fatal("append after age trim reallocated the backing array")
	}
	if !ring[0].At.Equal(now.Add(-4 * time.Second)) {
		t.Fatalf("oldest sample at %v, want %v", ring[0].At, now.Add(-4*time.Second))
	}

	capped := trimSamples(ring, now, time.Hour, 3)
	if len(capped) != 3 {
		t.Fatalf("len after cap = %d, want 3", len(capped))
	}
	if &capped[0] != &ring[len(ring)-3] {
		t.Fatal("tail cap copied instead of reslicing the backing array")
	}
}

func TestTrimSamplesWindowAfterManyAppends(t *testing.T) {
	now := time.Now()
	retention := 30 * time.Second
	const max = 10
	var s []topicSample
	for i := range 1000 {
		s = append(s, topicSample{At: now.Add(time.Duration(i-999) * time.Second)})
		s = trimSamples(s, now, retention, max)
	}
	if len(s) != max {
		t.Fatalf("len = %d, want %d", len(s), max)
	}
	for i, ts := range s {
		want := now.Add(time.Duration(i-(max-1)) * time.Second)
		if !ts.At.Equal(want) {
			t.Fatalf("sample %d at %v, want %v", i, ts.At, want)
		}
	}
}
