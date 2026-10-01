package web

// Health monitoring for the embedded dashboard. Rates come from log-end-offset
// deltas, not producer counters, so they cover every ingress path and stay
// accurate whichever client happens to be polling.

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

// Topic/cluster verdicts. "idle" is not a fault: the topic is simply not
// receiving data right now.
const (
	StatusCritical = "critical"
	StatusDegraded = "degraded"
	StatusHealthy  = "healthy"
	StatusIdle     = "idle"
)

const (
	// monitorInterval is the minimum gap between two recorded samples. A
	// client polling faster than this reuses the newest sample instead of
	// diluting the history.
	monitorInterval = 2 * time.Second
	// historyRetention is how far back samples are kept. Every window the API
	// accepts (up to 3600 s) must have history to measure against, or its rate
	// is not reliable by construction.
	historyRetention = time.Hour
	// historyMaxSamples bounds memory if a client polls faster than the sampler.
	historyMaxSamples = 3600
	// historyPoints caps the series returned to the browser. The ring keeps more
	// than it ships, and the span actually measured is reported separately.
	historyPoints = 240
	// defaultWindow is the rate window when the request does not ask for one.
	defaultWindow = 60 * time.Second
	// sparklinePoints caps the per-topic series returned to the UI.
	sparklinePoints = 60
)

// healthThresholds configures the verdict rules. Defaults target small
// single-node deployments; every value can be overridden per request.
type healthThresholds struct {
	LagWarn     int64
	LagCritical int64
	LagRateWarn float64
	LagRateCrit float64
	SkewWarn    float64
	IdleAfter   time.Duration
	DiskWarnPct float64
	DiskCritPct float64
	// UnflushedWarnBytes is how many bytes may sit un-synced before the broker
	// says so: a page-cache backlog is only a durability risk once it is large.
	UnflushedWarnBytes int64
	// RetentionWarnSeconds and RetentionCriticalSeconds bound how soon a lagging
	// group is projected to lose offsets retention has not deleted yet.
	RetentionWarnSeconds     int64
	RetentionCriticalSeconds int64
}

func defaultThresholds() healthThresholds {
	return healthThresholds{
		LagWarn:                  1000,
		LagCritical:              10000,
		LagRateWarn:              50,
		LagRateCrit:              500,
		SkewWarn:                 3,
		IdleAfter:                5 * time.Minute,
		DiskWarnPct:              85,
		DiskCritPct:              95,
		UnflushedWarnBytes:       32 << 20,
		RetentionWarnSeconds:     1800,
		RetentionCriticalSeconds: 300,
	}
}

// retentionSummary condenses the projection: whether any group has already lost
// unread data, and the soonest projected loss.
func retentionSummary(risks []retentionRisk) (*int64, bool) {
	var soonest *int64
	for _, r := range risks {
		if r.AlreadyLost {
			return nil, true
		}
		if r.SecondsUntilLoss != nil && (soonest == nil || *r.SecondsUntilLoss < *soonest) {
			soonest = r.SecondsUntilLoss
		}
	}
	return soonest, false
}

// retentionUrgency orders the projection list, most urgent first.
func retentionUrgency(r retentionRisk) int64 {
	if r.AlreadyLost {
		return -1
	}
	if r.SecondsUntilLoss == nil {
		return int64(1) << 62
	}
	return *r.SecondsUntilLoss
}

// Source values: where a health report's numbers came from.
const (
	sourceLocal    = "local"
	sourcePeer     = "peer"
	sourceExternal = "external"
)

// int64Ptr and f64Ptr mark a number that only some sources can produce. A nil
// pointer serializes as null, which a renderer can show as "unknown" instead of
// reading a zero as a real value.
func int64Ptr(v int64) *int64   { return &v }
func f64Ptr(v float64) *float64 { return &v }

// shortDuration renders a span as the largest useful unit pair.
func shortDuration(secs int64) string {
	d := time.Duration(secs) * time.Second
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
}

// Samples

type partitionSample struct {
	Partition int32
	LogEnd    int64
	Bytes     int64
	Messages  int64 // logEnd - earliest
	Lag       int64 // max over consumer groups
}

type topicSample struct {
	At         time.Time
	Partitions []partitionSample
	Messages   int64
	Bytes      int64
	Lag        int64
	MaxLag     int64
	// Earliest is the lowest offset still retained on the topic (-1 unknown):
	// paired with a group's next-needed offset it projects when retention will
	// take data that group has not consumed yet.
	Earliest int64
	// GroupLag is the per-group total backlog on this topic at sample time, so
	// a group's trend can be read from the same series as the topic.
	GroupLag map[string]int64
}

type clusterSample struct {
	At       time.Time
	Messages int64
	Bytes    int64
	Lag      int64
	DiskPct  float64
	// Earliest is the lowest offset still retained anywhere in the cluster, and
	// Committed is each group's committed total: together they project when a
	// lagging group loses the offsets it still needs to retention.
	Earliest  int64
	Committed map[string]int64
}

// sampleTime lets one trim helper serve both rings.
func (t topicSample) sampleTime() time.Time   { return t.At }
func (c clusterSample) sampleTime() time.Time { return c.At }

// Monitor accumulates samples and answers "is this flowing?" questions.
type Monitor struct {
	mu         sync.Mutex
	topics     map[string][]topicSample
	cluster    []clusterSample
	lastSeen   map[string]time.Time // topic -> last observed log growth
	lastAt     time.Time
	started    time.Time
	interval   time.Duration
	retention  time.Duration
	maxSamples int
}

// NewMonitor builds an empty monitor.
func NewMonitor() *Monitor {
	return &Monitor{
		topics:     make(map[string][]topicSample),
		lastSeen:   make(map[string]time.Time),
		started:    time.Now(),
		interval:   monitorInterval,
		retention:  historyRetention,
		maxSamples: historyMaxSamples,
	}
}

// Record takes one sample of the live store, or returns false when the previous
// sample is younger than the monitor interval and force is false. The background
// sampler sets force so its cadence stays exact regardless of client polling.
func (m *Monitor) Record(store *storage.Store, gm *coordinator.GroupManager, now time.Time, force bool) bool {
	if store == nil {
		return false
	}
	views := mergedGroupViews(gm)
	lagByTopic, groupLag := lagMaps(views, store)

	topics := store.TopicsSnapshot()
	topicSamples := make(map[string]topicSample, len(topics))
	cs := clusterSample{At: now, DiskPct: store.DiskUsagePct(), Earliest: -1, Committed: committedTotals(views)}
	for name, t := range topics {
		ts := topicSample{At: now, Earliest: -1}
		for _, pid := range sortedPartitionIDs(t) {
			p := t.Partitions[pid]
			if p == nil {
				continue
			}
			leo := p.LogEndOffset()
			earliest := p.EarliestOffset()
			if ts.Earliest < 0 || earliest < ts.Earliest {
				ts.Earliest = earliest
			}
			lag := int64(0)
			if l, ok := lagByTopic[name]; ok {
				lag = l[pid]
			}
			ps := partitionSample{
				Partition: pid,
				LogEnd:    leo,
				Bytes:     p.SizeBytes(),
				Messages:  nonNegative(leo - earliest),
				Lag:       lag,
			}
			ts.Partitions = append(ts.Partitions, ps)
			ts.Messages += ps.Messages
			ts.Bytes += ps.Bytes
			ts.Lag += lag
			if lag > ts.MaxLag {
				ts.MaxLag = lag
			}
		}
		ts.GroupLag = groupLag[name]
		topicSamples[name] = ts
		if ts.Earliest >= 0 && (cs.Earliest < 0 || ts.Earliest < cs.Earliest) {
			cs.Earliest = ts.Earliest
		}
		cs.Messages += ts.Messages
		cs.Bytes += ts.Bytes
		cs.Lag += ts.Lag
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !force && !m.lastAt.IsZero() && now.Sub(m.lastAt) < m.interval {
		return false
	}
	m.lastAt = now

	for name, ts := range topicSamples {
		prev, hadPrev := m.latest(name)
		switch {
		case hadPrev && ts.Messages > prev.Messages:
			m.lastSeen[name] = now
		case !hadPrev && ts.Messages > 0:
			// First sight of an already-populated topic: start the staleness
			// clock now rather than leaving it unknown forever. This can only
			// overstate activity right after startup; it never hides silence.
			m.lastSeen[name] = now
		}
		m.topics[name] = trimSamples(append(m.topics[name], ts), now, m.retention, m.maxSamples)
	}
	// Drop series for topics that no longer exist.
	for name := range m.topics {
		if _, ok := topics[name]; !ok {
			delete(m.topics, name)
			delete(m.lastSeen, name)
		}
	}
	m.cluster = trimSamples(append(m.cluster, cs), now, m.retention, m.maxSamples)
	return true
}

func (m *Monitor) latest(topic string) (topicSample, bool) {
	s := m.topics[topic]
	if len(s) == 0 {
		return topicSample{}, false
	}
	return s[len(s)-1], true
}

// StartSampler records a sample on every tick until ctx is done. Callers use
// it from the broker so history exists even before the dashboard is opened.
func (m *Monitor) StartSampler(ctx context.Context, store *storage.Store, gm *coordinator.GroupManager, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	m.mu.Lock()
	m.interval = interval
	m.mu.Unlock()
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		m.Record(store, gm, time.Now(), true)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.Record(store, gm, time.Now(), true)
			}
		}
	}()
}

// Response model

type healthReason struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // info | warn | critical
	Message  string `json:"message"`
}

type groupLagRow struct {
	Group     string  `json:"group"`
	State     string  `json:"state"`
	Members   int     `json:"members"`
	Lag       int64   `json:"lag"`
	LagPerSec float64 `json:"lagPerSec"`
	Stalled   bool    `json:"stalled"`
}

type partitionHealth struct {
	Partition int32 `json:"partition"`
	Leader    int32 `json:"leader"`
	LogEnd    int64 `json:"logEndOffset"`
	Earliest  int64 `json:"earliestOffset"`
	Messages  int64 `json:"messages"`
	Lag       int64 `json:"lag"`
	// Bytes and byte rates are null for a cluster we only observe as a client:
	// the Kafka protocol has no API for on-disk size, and a zero here would be
	// read as "empty" rather than "unknown".
	Bytes          *int64   `json:"bytes"`
	MessagesPerSec float64  `json:"messagesPerSec"`
	BytesPerSec    *float64 `json:"bytesPerSec"`
	SkewRatio      float64  `json:"skewRatio"`
	// Broker holds facts only the process that owns the log can know. It is
	// null for an external cluster.
	Broker *partitionBrokerFacts `json:"broker"`
	// ISR carries the replica topology a real multi-broker cluster has and a
	// single-node broker cannot: null when the source is not a cluster report.
	ISR      []int32 `json:"isr,omitempty"`
	Replicas []int32 `json:"replicas,omitempty"`
}

// partitionBrokerFacts is what the broker owning the log knows about durability
// and policy debt.
type partitionBrokerFacts struct {
	DurableOffset      int64 `json:"durableOffset"`
	OffsetsAtRisk      int64 `json:"offsetsAtRisk"`
	BytesAtRisk        int64 `json:"bytesAtRisk"`
	RetentionDebtBytes int64 `json:"retentionDebtBytes"`
}

// brokerFacts is the topic- and cluster-level half of the same thing.
type brokerFacts struct {
	BytesAtRisk         int64  `json:"bytesAtRisk"`
	OffsetsAtRisk       int64  `json:"offsetsAtRisk"`
	LastSyncAgeMs       *int64 `json:"lastSyncAgeMs"`
	RetentionDebtBytes  int64  `json:"retentionDebtBytes"`
	CompactionDebtBytes int64  `json:"compactionDebtBytes"`
	LastCompactMs       int64  `json:"lastCompactMs,omitempty"`
}

// retentionRisk projects when a lagging group loses the offsets it still needs.
// It is derived from the broker's own earliest offset advancing, which no client
// can observe: a client only finds out by failing a read.
type retentionRisk struct {
	Group            string  `json:"group"`
	NextNeeded       int64   `json:"nextNeeded"`
	Earliest         int64   `json:"earliest"`
	EarliestPerSec   float64 `json:"earliestPerSec"`
	SecondsUntilLoss *int64  `json:"secondsUntilLoss,omitempty"`
	AlreadyLost      bool    `json:"alreadyLost"`
}

type topicHealth struct {
	Name              string            `json:"name"`
	Status            string            `json:"status"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Messages          int64             `json:"messages"`
	MessagesPerSec    float64           `json:"messagesPerSec"`
	RateSpanSeconds   float64           `json:"rateSpanSeconds"`
	RateReliable      bool              `json:"rateReliable"`
	Lag               int64             `json:"lag"`
	MaxPartitionLag   int64             `json:"maxPartitionLag"`
	LagPerSec         float64           `json:"lagPerSec"`
	Trend             string            `json:"trend"` // rising | falling | flat
	SkewRatio         float64           `json:"skewRatio"`
	LastAppendAgeMs   *int64            `json:"lastAppendAgeMs"`
	Groups            []groupLagRow     `json:"groups"`
	Reasons           []healthReason    `json:"reasons"`
	PartitionsDetail  []partitionHealth `json:"partitionsDetail"`
	Sparkline         []float64         `json:"sparkline"` // msg/s, oldest first
	// Source says where the numbers came from: "local" (this broker), "peer"
	// (another PocketKafka over its API), or "external" (any Kafka cluster we
	// observe as a client). Fields a client cannot know are null or listed in
	// unavailable, never zero.
	Source      string   `json:"source"`
	Unavailable []string `json:"unavailable,omitempty"`
	// Bytes and byte rates are null for an external cluster: the protocol has no
	// size API, and a zero would read as "empty" rather than "unknown".
	Bytes       *int64   `json:"bytes"`
	BytesPerSec *float64 `json:"bytesPerSec"`
	// Broker carries durability and policy debt, null when this process does not
	// own the log.
	Broker        *brokerFacts    `json:"broker"`
	RetentionRisk []retentionRisk `json:"retentionRisk"`
}

type clusterHealth struct {
	ClusterID     string  `json:"clusterId"`
	BrokerID      int32   `json:"brokerId"`
	Version       string  `json:"version"`
	Status        string  `json:"status"`
	UptimeSeconds float64 `json:"uptimeSeconds"`
	Topics        int     `json:"topics"`
	Partitions    int     `json:"partitions"`
	Groups        int     `json:"groups"`
	// TotalBytes and DiskUsagePct are null for an external cluster: no Kafka API
	// reports on-disk size, and zero would read as an empty cluster.
	TotalBytes      *int64 `json:"totalBytes"`
	TotalMessages   int64  `json:"totalMessages"`
	Lag             int64  `json:"lag"`
	MaxPartitionLag int64  `json:"maxPartitionLag"`
	// Source is "local", "peer", or "external".
	Source string `json:"source"`
	// Unavailable names the fields a client cannot observe for this source, so a
	// renderer can hide them instead of showing a zero.
	Unavailable     []string `json:"unavailable,omitempty"`
	RetentionRisks  int      `json:"retentionRisks"`
	MessagesPerSec  float64  `json:"messagesPerSec"`
	BytesPerSec     *float64 `json:"bytesPerSec"`
	ConsumedPerSec  float64  `json:"consumedPerSec"`
	RateSpanSeconds float64  `json:"rateSpanSeconds"`
	DiskUsagePct    *float64 `json:"diskUsagePct"`
	Listeners       []string `json:"listeners"`
	Advertised      string   `json:"advertised"`
	Security        string   `json:"security"`
	// Broker holds facts only the process owning the logs can know: null for an
	// external cluster, summed over topics for a local one.
	Broker  *brokerFacts   `json:"broker"`
	Reasons []healthReason `json:"reasons"`
}

type historyPoint struct {
	T              int64   `json:"t"` // unix ms
	MessagesPerSec float64 `json:"messagesPerSec"`
	BytesPerSec    float64 `json:"bytesPerSec"`
	ConsumedPerSec float64 `json:"consumedPerSec"`
	Lag            int64   `json:"lag"`
	DiskUsagePct   float64 `json:"diskUsagePct"`
}

// committedTotals sums each group's committed offsets, per group, so a consumed
// rate can be taken from forward progress only: a reset or a group that
// disappeared must not read as negative consumption.
func committedTotals(views []groupView) map[string]int64 {
	out := make(map[string]int64, len(views))
	for _, g := range views {
		var total int64
		for _, parts := range g.Offsets {
			for _, committed := range parts {
				total += committed
			}
		}
		out[g.Name] = total
	}
	return out
}

// progressBetween counts committed offsets advancing from one sample to the
// next, forward only: a reset must not read as negative consumption.
func progressBetween(prev, cur clusterSample) int64 {
	var progressed int64
	for group, committed := range cur.Committed {
		if before, ok := prev.Committed[group]; ok && committed > before {
			progressed += committed - before
		}
	}
	return progressed
}

// consumedRate sums forward progress over the consecutive sample pairs inside
// the window. Measuring it against the window baseline alone would read zero for
// a group that only appeared after that baseline, and would hide the traffic of
// a group whose first sample sits mid-window.
func consumedRate(series []clusterSample, window time.Duration, now time.Time) float64 {
	cut := now.Add(-window)
	var progressed int64
	var counted time.Duration
	for i := 1; i < len(series); i++ {
		prev, cur := series[i-1], series[i]
		if cur.At.Before(cut) {
			continue
		}
		d := cur.At.Sub(prev.At)
		if d <= 0 {
			continue
		}
		counted += d
		progressed += progressBetween(prev, cur)
	}
	if counted <= 0 {
		return 0
	}
	return rate(float64(progressed), counted)
}

// thinPoints keeps at most max points, preserving both ends so a long window
// still shows the whole span instead of only its newest tail.
func thinPoints(points []historyPoint, max int) []historyPoint {
	if max <= 2 || len(points) <= max {
		return points
	}
	out := make([]historyPoint, 0, max)
	step := float64(len(points)-1) / float64(max-1)
	for i := 0; i < max; i++ {
		out = append(out, points[int(float64(i)*step+0.5)])
	}
	return out
}

type samplingInfo struct {
	IntervalMs       int64   `json:"intervalMs"`
	Samples          int     `json:"samples"`
	WindowSeconds    int     `json:"windowSeconds"`
	RetentionSeconds int     `json:"retentionSeconds"`
	HistoryPoints    int     `json:"historyPoints"`
	Warmup           bool    `json:"warmup"`
	RateReliable     bool    `json:"rateReliable"`
	StartedAtMs      int64   `json:"startedAtMs"`
	AgeSeconds       float64 `json:"ageSeconds"`
}

// reportCtx carries the per-request settings the report builders need.
type reportCtx struct {
	th       healthThresholds
	window   time.Duration
	now      time.Time
	interval time.Duration
}

// rateReliable reports whether the series actually covers the requested window.
// A shorter span means the baseline already holds traffic that arrived before
// monitoring started (or before the window's edge), so the derived rate
// understates reality and must not be presented as this window's rate. One
// sample of slack is allowed, because the ring is trimmed by age.
func (c reportCtx) rateReliable(first, last time.Time, n int) bool {
	if n < 2 {
		return false
	}
	return c.now.Sub(first) >= c.window-c.interval
}

// topicRateReliable adapts rateReliable to a topic series.
func (c reportCtx) topicRateReliable(series []topicSample) bool {
	if len(series) == 0 {
		return false
	}
	return c.rateReliable(series[0].At, series[len(series)-1].At, len(series))
}

// clusterRateReliable adapts rateReliable to the cluster series.
func (c reportCtx) clusterRateReliable(series []clusterSample) bool {
	if len(series) == 0 {
		return false
	}
	return c.rateReliable(series[0].At, series[len(series)-1].At, len(series))
}

// groupSummary is one consumer group as reported by the dashboard.
type groupSummary struct {
	Name      string   `json:"name"`
	State     string   `json:"state"`
	Members   int      `json:"members"`
	Topics    []string `json:"topics"`
	Lag       int64    `json:"lag"`
	LagPerSec float64  `json:"lagPerSec"`
	Stalled   bool     `json:"stalled"`
}

type overviewResponse struct {
	GeneratedAtMs int64          `json:"generatedAtMs"`
	Thresholds    map[string]any `json:"thresholds"`
	Sampling      samplingInfo   `json:"sampling"`
	Cluster       clusterHealth  `json:"cluster"`
	Topics        []topicHealth  `json:"topics"`
	Attention     []topicHealth  `json:"attention"`
	Groups        []groupSummary `json:"groups"`
	History       []historyPoint `json:"history"`
}

// Verdict rules (pure, unit-tested)

// topicMetrics is the numeric view of one topic that the verdict depends on.
type topicMetrics struct {
	Partitions     int
	Messages       int64
	MessagesPerSec float64
	Lag            int64
	LagPerSec      float64
	SkewRatio      float64
	LastAppendAge  time.Duration
	HasActivity    bool
	StalledGroups  []string
	// RetentionLossSeconds is how soon a lagging group is projected to lose the
	// offsets it still needs; nil means no projection (retention is not
	// advancing), and RetentionLost means the data is already gone.
	RetentionLossSeconds *int64
	RetentionLost        bool
}

// evaluateTopic turns metrics into a status plus the reasons behind it. The
// rule order matters: the first matching rule sets the base status, then the
// secondary warnings are appended.
func evaluateTopic(th healthThresholds, m topicMetrics) (string, []healthReason) {
	// Always non-nil: a healthy topic must serialize as [] rather than null so
	// API consumers can iterate unconditionally.
	reasons := []healthReason{}
	status := StatusHealthy
	rising := m.LagPerSec > 0.5
	falling := m.LagPerSec < -0.5

	switch {
	case m.LagPerSec >= th.LagRateCrit || (m.Lag >= th.LagCritical && !falling):
		status = StatusCritical
		reasons = append(reasons, healthReason{
			Code:     "lag_growth_critical",
			Severity: "critical",
			Message:  fmt.Sprintf("consumer lag is growing by %.0f msgs/s (total %d)", m.LagPerSec, m.Lag),
		})
	case m.LagPerSec >= th.LagRateWarn:
		status = StatusDegraded
		reasons = append(reasons, healthReason{
			Code:     "lag_growth",
			Severity: "warn",
			Message:  fmt.Sprintf("consumer lag is growing by %.0f msgs/s (total %d)", m.LagPerSec, m.Lag),
		})
	case m.Lag >= th.LagWarn && !falling:
		status = StatusDegraded
		reasons = append(reasons, healthReason{
			Code:     "lag_backlog",
			Severity: "warn",
			Message:  fmt.Sprintf("backlog of %d messages is not draining", m.Lag),
		})
	case m.Lag > 0 && falling:
		reasons = append(reasons, healthReason{
			Code:     "lag_recovering",
			Severity: "info",
			Message:  fmt.Sprintf("backlog draining at %.0f msgs/s (%d left)", -m.LagPerSec, m.Lag),
		})
	}

	if m.Partitions >= 2 && m.SkewRatio >= th.SkewWarn {
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code:     "partition_skew",
			Severity: "warn",
			Message:  fmt.Sprintf("hot partition: the busiest partition holds %.1fx the average", m.SkewRatio),
		})
	}
	for _, g := range m.StalledGroups {
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code:     "group_stalled",
			Severity: "warn",
			Message:  fmt.Sprintf("group %q has a backlog but no active members", g),
		})
	}

	if status == StatusHealthy && rising {
		status = StatusDegraded
	}
	// Retention risk outranks the ordinary verdict: a client cannot see this
	// coming, it only finds out when a read fails.
	if m.RetentionLost {
		status = worseStatus(status, StatusCritical)
		reasons = append(reasons, healthReason{
			Code:     "retention_offset_lost",
			Severity: "critical",
			Message:  "retention has deleted offsets a consumer group has not read yet",
		})
	} else if m.RetentionLossSeconds != nil {
		secs := *m.RetentionLossSeconds
		switch {
		case secs <= th.RetentionCriticalSeconds:
			status = worseStatus(status, StatusCritical)
			reasons = append(reasons, healthReason{
				Code:     "retention_loss_imminent",
				Severity: "critical",
				Message:  fmt.Sprintf("retention will delete unread offsets in ~%s", shortDuration(secs)),
			})
		case secs <= th.RetentionWarnSeconds:
			status = worseStatus(status, StatusDegraded)
			reasons = append(reasons, healthReason{
				Code:     "retention_loss_projected",
				Severity: "warn",
				Message:  fmt.Sprintf("retention will delete unread offsets in ~%s", shortDuration(secs)),
			})
		}
	}
	if status == StatusHealthy && m.Messages == 0 {
		status = StatusIdle
		reasons = append(reasons, healthReason{
			Code:     "empty",
			Severity: "info",
			Message:  "topic has no messages yet",
		})
	}
	if status == StatusHealthy && m.HasActivity && m.LastAppendAge >= th.IdleAfter {
		status = StatusIdle
		reasons = append(reasons, healthReason{
			Code:     "no_recent_produce",
			Severity: "info",
			Message:  fmt.Sprintf("no new messages for %s", humanDuration(m.LastAppendAge)),
		})
	}
	return status, reasons
}

func clusterStatus(th healthThresholds, diskPct float64, topics []topicHealth) (string, []healthReason) {
	reasons := []healthReason{}
	status := StatusHealthy
	allIdle := len(topics) > 0
	for _, t := range topics {
		status = worseStatus(status, t.Status)
		if t.Status != StatusIdle {
			allIdle = false
		}
	}
	if len(topics) == 0 {
		allIdle = false
	}
	switch {
	case diskPct >= th.DiskCritPct:
		status = worseStatus(status, StatusCritical)
		reasons = append(reasons, healthReason{
			Code:     "disk_critical",
			Severity: "critical",
			Message:  fmt.Sprintf("data dir filesystem is %.1f%% full", diskPct),
		})
	case diskPct >= th.DiskWarnPct:
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code:     "disk_pressure",
			Severity: "warn",
			Message:  fmt.Sprintf("data dir filesystem is %.1f%% full", diskPct),
		})
	}
	if allIdle && status == StatusHealthy {
		status = StatusIdle
	}
	for _, t := range topics {
		if t.Status == StatusCritical || t.Status == StatusDegraded {
			reasons = append(reasons, healthReason{
				Code:     "topic_attention",
				Severity: t.Status,
				Message:  fmt.Sprintf("topic %q is %s", t.Name, t.Status),
			})
			if len(reasons) >= 6 {
				break
			}
		}
	}
	return status, reasons
}

func worseStatus(a, b string) string {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

func statusRank(s string) int {
	switch s {
	case StatusCritical:
		return 3
	case StatusDegraded:
		return 2
	case StatusHealthy:
		return 1
	default:
		return 0
	}
}

// Report builder

// buildOverview renders the current health report. It never mutates anything
// except the sample ring.
func (s *Server) buildOverview(th healthThresholds, window time.Duration, now time.Time) overviewResponse {
	samples, clusterSeries, lastAt := s.monitor.snapshot()
	interval, retention := s.monitor.limits()
	ctx := reportCtx{th: th, window: window, now: now, interval: interval}

	views := mergedGroupViews(s.gm)
	lagByTopic, _ := lagMaps(views, s.store)

	topics := s.store.TopicsSnapshot()
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)

	report := overviewResponse{
		GeneratedAtMs: now.UnixMilli(),
		Thresholds: map[string]any{
			"lagWarn":     th.LagWarn,
			"lagCritical": th.LagCritical,
			"lagRateWarn": th.LagRateWarn,
			"lagRateCrit": th.LagRateCrit,
			"skewWarn":    th.SkewWarn,
			"idleAfterMs": th.IdleAfter.Milliseconds(),
			"diskWarnPct": th.DiskWarnPct,
			"diskCritPct": th.DiskCritPct,
		},
	}
	// Sampling info: the cluster series is the sampling heartbeat, so it tells
	// us how much history the current window actually holds.
	samplesInWindow := 0
	for _, cs := range clusterSeries {
		if !cs.At.Before(now.Add(-window)) {
			samplesInWindow++
		}
	}
	warmup := samplesInWindow < 2
	intervalMs := ctx.interval.Milliseconds()
	age := 0.0
	if !lastAt.IsZero() {
		age = now.Sub(lastAt).Seconds()
	}
	report.Sampling = samplingInfo{
		IntervalMs:       intervalMs,
		Samples:          samplesInWindow,
		WindowSeconds:    int(window.Seconds()),
		RetentionSeconds: int(retention.Seconds()),
		Warmup:           warmup,
		RateReliable:     ctx.clusterRateReliable(clusterSeries),
		StartedAtMs:      s.monitor.started.UnixMilli(),
		AgeSeconds:       age,
	}

	// Cluster totals.
	var totalMessages, totalBytes, maxLag int64
	clusterPartitions := 0
	for _, name := range names {
		t := topics[name]
		clusterPartitions += len(t.Partitions)
	}
	cur, hasCur := lastCluster(clusterSeries)
	if hasCur {
		totalMessages, totalBytes = cur.Messages, cur.Bytes
	}
	old, dt := clusterBaseline(clusterSeries, window, now)
	var msgRate, byteRate, consumeRate float64
	if dt > 0 {
		msgRate = rate(float64(cur.Messages-old.Messages), dt)
		byteRate = rate(float64(cur.Bytes-old.Bytes), dt)
		consumeRate = consumedRate(clusterSeries, window, now)
	}

	topicReports := make([]topicHealth, 0, len(names))
	for _, name := range names {
		tr := s.buildTopicHealth(ctx, name, topics[name], samples[name], lagByTopic[name], views)
		if tr.MaxPartitionLag > maxLag {
			maxLag = tr.MaxPartitionLag
		}
		topicReports = append(topicReports, tr)
	}

	diskPct := 0.0
	if hasCur {
		diskPct = cur.DiskPct
	} else if s.store != nil {
		diskPct = s.store.DiskUsagePct()
	}
	report.Groups = groupSummariesOf(topicReports, views)
	status, reasons := clusterStatus(ctx.th, diskPct, topicReports)

	// Durability and policy debt roll up from the topic reports, and an
	// un-synced backlog big enough to matter becomes a cluster-level warning.
	var broker brokerFacts
	retentionRisks := 0
	for _, tr := range topicReports {
		if tr.Broker == nil {
			continue // external cluster: this process owns none of the logs
		}
		broker.BytesAtRisk += tr.Broker.BytesAtRisk
		broker.OffsetsAtRisk += tr.Broker.OffsetsAtRisk
		broker.RetentionDebtBytes += tr.Broker.RetentionDebtBytes
		broker.CompactionDebtBytes += tr.Broker.CompactionDebtBytes
		retentionRisks += len(tr.RetentionRisk)
		if tr.Broker.LastSyncAgeMs != nil &&
			(broker.LastSyncAgeMs == nil || *tr.Broker.LastSyncAgeMs > *broker.LastSyncAgeMs) {
			broker.LastSyncAgeMs = tr.Broker.LastSyncAgeMs
		}
	}
	if broker.BytesAtRisk >= ctx.th.UnflushedWarnBytes {
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code:     "unflushed_backlog",
			Severity: "warn",
			Message:  fmt.Sprintf("%.1f MiB written but not fsynced yet", float64(broker.BytesAtRisk)/(1<<20)),
		})
	}

	report.Cluster = clusterHealth{
		ClusterID:       s.clusterID,
		BrokerID:        s.brokerID,
		Version:         s.version,
		Status:          status,
		UptimeSeconds:   now.Sub(s.startTime).Seconds(),
		Topics:          len(names),
		Partitions:      clusterPartitions,
		Groups:          len(report.Groups),
		Source:          sourceLocal,
		TotalBytes:      int64Ptr(totalBytes),
		TotalMessages:   totalMessages,
		Lag:             totalLagWithTopics(topicReports),
		MaxPartitionLag: maxLag,
		RetentionRisks:  retentionRisks,
		MessagesPerSec:  msgRate,
		BytesPerSec:     f64Ptr(byteRate),
		ConsumedPerSec:  consumeRate,
		RateSpanSeconds: dt.Seconds(),
		DiskUsagePct:    f64Ptr(diskPct),
		Listeners:       s.listeners,
		Advertised:      s.advertised,
		Security:        s.securityMode,
		Broker:          &broker,
		Reasons:         reasons,
	}
	report.Topics = topicReports

	// Attention list: worst first, then by name.
	attention := make([]topicHealth, 0, len(topicReports))
	for _, t := range topicReports {
		if t.Status == StatusDegraded || t.Status == StatusCritical {
			attention = append(attention, t)
		}
	}
	sort.SliceStable(attention, func(i, j int) bool {
		if statusRank(attention[i].Status) != statusRank(attention[j].Status) {
			return statusRank(attention[i].Status) > statusRank(attention[j].Status)
		}
		return attention[i].Lag > attention[j].Lag
	})
	report.Attention = attention

	// Cluster history: consumed/s rides along with the produced rate so the two
	// can be compared, which is what makes a backlog visible as a shape.
	history := make([]historyPoint, 0, len(clusterSeries))
	for i := 1; i < len(clusterSeries); i++ {
		prev, cur := clusterSeries[i-1], clusterSeries[i]
		d := cur.At.Sub(prev.At)
		if d <= 0 {
			continue
		}
		history = append(history, historyPoint{
			T:              cur.At.UnixMilli(),
			MessagesPerSec: rate(float64(cur.Messages-prev.Messages), d),
			BytesPerSec:    rate(float64(cur.Bytes-prev.Bytes), d),
			ConsumedPerSec: rate(float64(progressBetween(prev, cur)), d),
			Lag:            cur.Lag,
			DiskUsagePct:   cur.DiskPct,
		})
	}
	report.History = thinPoints(history, historyPoints)
	report.Sampling.HistoryPoints = len(report.History)
	return report
}

// buildTopicHealth derives one topic's row from its samples plus live state.
func (s *Server) buildTopicHealth(ctx reportCtx, name string, t *storage.Topic, series []topicSample, lagByPart map[int32]int64, groups []groupView) topicHealth {
	window, now := ctx.window, ctx.now
	th := ctx.th
	row := topicHealth{
		Name:              name,
		ReplicationFactor: 1,
		Groups:            []groupLagRow{},
		Reasons:           []healthReason{},
		PartitionsDetail:  []partitionHealth{},
		Sparkline:         []float64{},
	}
	if t != nil {
		row.Partitions = len(t.Partitions)
	}

	// Parallel live snapshot (authoritative for offsets/bytes).
	type live struct {
		pid           int32
		leo           int64
		earliest      int64
		bytes         int64
		durable       int64
		atRisk        int64
		atRiskOffsets int64
		syncMs        int64
		debt          int64
	}
	var lives []live
	if t != nil {
		ret, hasRet := s.store.Retention()
		for _, pid := range sortedPartitionIDs(t) {
			p := t.Partitions[pid]
			if p == nil {
				continue
			}
			durable, atRisk, syncMs := p.Durability()
			debt := int64(0)
			if hasRet {
				debt = p.RetentionDebt(ret)
			}
			lives = append(lives, live{
				pid: pid, leo: p.LogEndOffset(), earliest: p.EarliestOffset(), bytes: p.SizeBytes(),
				durable: durable, atRisk: atRisk, atRiskOffsets: nonNegative(p.LogEndOffset() - durable),
				syncMs: syncMs, debt: debt,
			})
		}
	}

	var messages, bytes int64
	for _, l := range lives {
		messages += nonNegative(l.leo - l.earliest)
		bytes += l.bytes
	}
	row.Messages = messages
	row.Bytes = int64Ptr(bytes)
	row.Source = sourceLocal

	// Durability and policy debt, aggregated over the topic's partitions. A
	// topic this process does not own reports null instead.
	var oldestSync int64
	var broker brokerFacts
	for _, l := range lives {
		broker.BytesAtRisk += l.atRisk
		broker.OffsetsAtRisk += l.atRiskOffsets
		broker.RetentionDebtBytes += l.debt
		if l.syncMs > 0 && (oldestSync == 0 || l.syncMs < oldestSync) {
			oldestSync = l.syncMs
		}
	}
	if oldestSync > 0 {
		ageMs := now.Sub(time.UnixMilli(oldestSync)).Milliseconds()
		if ageMs < 0 {
			ageMs = 0
		}
		broker.LastSyncAgeMs = &ageMs
	}
	if s.store.IsCompacted(name) {
		var lastCompact int64
		for _, pid := range sortedPartitionIDs(t) {
			p := t.Partitions[pid]
			if p == nil {
				continue
			}
			debt, ms := p.CompactionDebt()
			broker.CompactionDebtBytes += debt
			if ms > lastCompact {
				lastCompact = ms
			}
		}
		broker.LastCompactMs = lastCompact
	}
	row.Broker = &broker

	cur, hasCur := lastTopic(series)
	base, dt := topicBaseline(series, window, now)
	row.RateSpanSeconds = round2(dt.Seconds())
	row.RateReliable = ctx.topicRateReliable(series)
	if hasCur && dt > 0 {
		row.MessagesPerSec = rate(float64(cur.Messages-base.Messages), dt)
		row.BytesPerSec = f64Ptr(rate(float64(cur.Bytes-base.Bytes), dt))
		row.LagPerSec = rate(float64(cur.Lag-base.Lag), dt)
	}
	if hasCur {
		row.Lag = cur.Lag
		row.MaxPartitionLag = cur.MaxLag
	}
	switch {
	case row.LagPerSec > 0.5:
		row.Trend = "rising"
	case row.LagPerSec < -0.5:
		row.Trend = "falling"
	default:
		row.Trend = "flat"
	}

	// Per-partition view with skew.
	var maxBytes, sumBytes int64
	for _, l := range lives {
		if l.bytes > maxBytes {
			maxBytes = l.bytes
		}
		sumBytes += l.bytes
	}
	mean := float64(0)
	if len(lives) > 0 {
		mean = float64(sumBytes) / float64(len(lives))
	}
	if mean > 0 {
		row.SkewRatio = round2(float64(maxBytes) / mean)
	}

	for _, l := range lives {
		ph := partitionHealth{
			Partition: l.pid,
			Leader:    s.brokerID,
			LogEnd:    l.leo,
			Earliest:  l.earliest,
			Messages:  nonNegative(l.leo - l.earliest),
			Bytes:     int64Ptr(l.bytes),
			Broker: &partitionBrokerFacts{
				DurableOffset:      l.durable,
				OffsetsAtRisk:      l.atRiskOffsets,
				BytesAtRisk:        l.atRisk,
				RetentionDebtBytes: l.debt,
			},
		}
		if m, ok := lagByPart[l.pid]; ok {
			ph.Lag = m
		}
		if mean > 0 {
			ph.SkewRatio = round2(float64(l.bytes) / mean)
		}
		// Per-partition rates from the sample series.
		if pcur, pok := lastPartition(series, l.pid); pok && dt > 0 {
			if pbase, bok := partitionBaseline(series, l.pid, window, now); bok {
				ph.MessagesPerSec = rate(float64(pcur.Messages-pbase.Messages), dt)
				ph.BytesPerSec = f64Ptr(rate(float64(pcur.Bytes-pbase.Bytes), dt))
			}
		}
		row.PartitionsDetail = append(row.PartitionsDetail, ph)
	}

	// Retention projection: a group loses its unread offsets when the earliest
	// retained offset catches up with the offset it needs next. The earliest
	// offset advances only as retention deletes, so its observed rate is what
	// projects the moment.
	earliestNow := int64(-1)
	for _, l := range lives {
		if earliestNow < 0 || l.earliest < earliestNow {
			earliestNow = l.earliest
		}
	}
	if earliestNow >= 0 && len(groups) > 0 {
		var advance float64
		if hasCur && dt > 0 && base.Earliest >= 0 && cur.Earliest > base.Earliest {
			advance = rate(float64(cur.Earliest-base.Earliest), dt)
		}
		leoByPart := make(map[int32]int64, len(lives))
		for _, l := range lives {
			leoByPart[l.pid] = l.leo
		}
		for _, g := range groups {
			parts, ok := g.Offsets[name]
			if !ok || len(parts) == 0 {
				continue
			}
			// Only a group that is behind a partition needs data that retention
			// can still take: one that has caught up needs what comes next.
			next := int64(-1)
			for part, committed := range parts {
				leo, known := leoByPart[part]
				if !known || committed >= leo {
					continue
				}
				if next < 0 || committed+1 < next {
					next = committed + 1
				}
			}
			if next < 0 {
				continue
			}
			risk := retentionRisk{Group: g.Name, NextNeeded: next, Earliest: earliestNow, EarliestPerSec: round2(advance)}
			switch {
			case next < earliestNow:
				risk.AlreadyLost = true
			case advance > 0:
				secs := int64(float64(next-earliestNow) / advance)
				risk.SecondsUntilLoss = &secs
			default:
				continue // retention is not advancing: nothing to project
			}
			row.RetentionRisk = append(row.RetentionRisk, risk)
		}
		sort.Slice(row.RetentionRisk, func(i, j int) bool {
			return retentionUrgency(row.RetentionRisk[i]) < retentionUrgency(row.RetentionRisk[j])
		})
	}

	// Last observed append.
	if seen, ok := s.monitor.lastSeenAt(name); ok {
		ageMs := now.Sub(seen).Milliseconds()
		if ageMs < 0 {
			ageMs = 0
		}
		row.LastAppendAgeMs = &ageMs
	}

	// Consumer groups on this topic, including groups that stopped consuming but
	// still hold committed offsets. A group without offsets here is not a
	// consumer of this topic and must not appear.
	stalled := []string{}
	for _, g := range groups {
		parts, ok := g.Offsets[name]
		if !ok || len(parts) == 0 {
			continue
		}
		gl := groupLagRow{Group: g.Name, State: g.State, Members: g.Members}
		for part, committed := range parts {
			leo := int64(0)
			if t != nil {
				if p := t.Partitions[part]; p != nil {
					leo = p.LogEndOffset()
				}
			}
			gl.Lag += nonNegative(leo - committed)
		}
		// Per-group trend comes from the same sample series as the topic.
		if dt > 0 && hasCur {
			if curLag, ok := cur.GroupLag[g.Name]; ok {
				gl.LagPerSec = rate(float64(curLag-base.GroupLag[g.Name]), dt)
			}
		}
		if gl.Lag > 0 && g.Members == 0 {
			gl.Stalled = true
			stalled = append(stalled, g.Name)
		}
		row.Groups = append(row.Groups, gl)
	}

	// Sparkline: msg/s per sample in the window, oldest first.
	spark := make([]float64, 0, len(series))
	for i := 1; i < len(series); i++ {
		d := series[i].At.Sub(series[i-1].At)
		if d <= 0 {
			continue
		}
		if series[i].At.Before(now.Add(-window)) {
			continue
		}
		spark = append(spark, rate(float64(series[i].Messages-series[i-1].Messages), d))
	}
	if len(spark) > sparklinePoints {
		spark = spark[len(spark)-sparklinePoints:]
	}
	row.Sparkline = spark

	hasActivity := row.LastAppendAgeMs != nil
	var age time.Duration
	if hasActivity {
		age = time.Duration(*row.LastAppendAgeMs) * time.Millisecond
	}
	lossSeconds, lost := retentionSummary(row.RetentionRisk)
	row.Status, row.Reasons = evaluateTopic(th, topicMetrics{
		Partitions:           row.Partitions,
		Messages:             row.Messages,
		MessagesPerSec:       row.MessagesPerSec,
		Lag:                  row.Lag,
		LagPerSec:            row.LagPerSec,
		SkewRatio:            row.SkewRatio,
		LastAppendAge:        age,
		HasActivity:          hasActivity,
		StalledGroups:        stalled,
		RetentionLossSeconds: lossSeconds,
		RetentionLost:        lost,
	})
	// An empty topic with a consumer group that has nothing to read is still
	// idle, never a fault.
	return row
}

// HTTP

// handleHealthOverview serves the monitoring report rendered by the dashboard
// Health view.
func (s *Server) handleHealthOverview(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		writeErr(w, http.StatusServiceUnavailable, "storage unavailable")
		return
	}
	th := defaultThresholds()
	window := defaultWindow
	q := r.URL.Query()
	if v := q.Get("window"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 5 && n <= 3600 {
			window = time.Duration(n) * time.Second
		}
	}
	if v := q.Get("lag_warn"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			th.LagWarn = n
		}
	}
	if v := q.Get("lag_crit"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			th.LagCritical = n
		}
	}
	if v := q.Get("idle_s"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 10 {
			th.IdleAfter = time.Duration(n) * time.Second
		}
	}
	if v := q.Get("retention_warn_s"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			th.RetentionWarnSeconds = n
		}
	}
	if v := q.Get("retention_crit_s"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			th.RetentionCriticalSeconds = n
		}
	}
	if v := q.Get("unflushed_warn_bytes"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			th.UnflushedWarnBytes = n
		}
	}
	// A named cluster is served through the same response path as the local
	// broker: proxied from a peer over its API, or built here from a read-only
	// client session for a Kafka cluster we do not host.
	if name := q.Get("cluster"); name != "" && name != "local" {
		entry, ok := s.clusters.Get(name)
		if !ok {
			writeErr(w, http.StatusNotFound, "unknown cluster: "+name)
			return
		}
		if normalizeKind(entry.Kind) == clusterKindKafka {
			// The first request starts the sampler. A short grace lets a small
			// cluster answer with real data straight away; a large one answers
			// with "no sample yet" and fills in on the next poll.
			ec := s.externalFor(entry)
			ec.awaitFirstSample(r.Context(), 1500*time.Millisecond)
			report, err := s.externalOverview(name, window, time.Now())
			if err != nil {
				writeErr(w, http.StatusBadGateway, "cluster "+name+": "+err.Error())
				return
			}
			report.Sampling.WindowSeconds = int(window.Seconds())
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-PocketKafka-Cluster", name)
			w.Header().Set("X-PocketKafka-Source", sourceExternal)
			writeJSON(w, http.StatusOK, report)
			return
		}
		body, _, err := s.fetchClusterReport(r.Context(), entry, window, s.clusters.Token(name))
		if err != nil {
			writeErr(w, http.StatusBadGateway, "cluster "+name+": "+err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-PocketKafka-Cluster", name)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
		return
	}
	now := time.Now()
	s.monitor.Record(s.store, s.gm, now, false)
	writeJSON(w, http.StatusOK, s.buildOverview(th, window, now))
}

// StartSampler runs the background health sampler on this server's monitor, so
// the report has history even when nothing is polling the API.
func (s *Server) StartSampler(ctx context.Context, store *storage.Store, gm *coordinator.GroupManager, interval time.Duration) {
	if s.monitor == nil {
		return
	}
	s.monitor.StartSampler(ctx, store, gm, interval)
}

// Helpers

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

func rate(delta float64, dt time.Duration) float64 {
	if dt <= 0 {
		return 0
	}
	return round2(delta / dt.Seconds())
}

func round2(v float64) float64 {
	return float64(int64(v*100+sign(v)*0.5)) / 100
}

func sign(v float64) float64 {
	if v < 0 {
		return -1
	}
	return 1
}

func humanDuration(d time.Duration) string {
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	case d >= time.Minute:
		return fmt.Sprintf("%.0fm", d.Minutes())
	default:
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
}

// trimSamples drops samples older than retention, then caps the slice so a
// client polling faster than the sampler cannot grow it without bound.
func trimSamples[T interface{ sampleTime() time.Time }](s []T, now time.Time, retention time.Duration, max int) []T {
	cut := now.Add(-retention)
	first := 0
	for first < len(s) && s[first].sampleTime().Before(cut) {
		first++
	}
	if first > 0 {
		s = append([]T(nil), s[first:]...)
	}
	if len(s) > max {
		s = append([]T(nil), s[len(s)-max:]...)
	}
	return s
}

func sortedPartitionIDs(t *storage.Topic) []int32 {
	ids := make([]int32, 0, len(t.Partitions))
	for pid := range t.Partitions {
		ids = append(ids, pid)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// groupView is the merged view of one consumer group: liveness comes from the
// in-memory manager, backlog from the persistent offset store. Merging is what
// makes a stopped consumer visible.
type groupView struct {
	Name    string
	State   string
	Members int
	Offsets map[string]map[int32]int64
}

// mergedGroupViews unions the live groups with every group that holds a
// committed offset, so a consumer that stopped (no members, backlog left
// behind) still shows up in the dashboard.
func mergedGroupViews(gm *coordinator.GroupManager) []groupView {
	if gm == nil {
		return nil
	}
	out := []groupView{}
	seen := map[string]bool{}
	for _, g := range gm.ListGroups() {
		out = append(out, groupView{Name: g.Name, State: g.State, Members: len(g.Members), Offsets: g.Offsets})
		seen[g.Name] = true
	}
	for name, offsets := range gm.CommittedGroups() {
		if seen[name] {
			continue
		}
		out = append(out, groupView{Name: name, State: "Empty", Members: 0, Offsets: offsets})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// lagMaps derives in one pass: the worst per-partition lag per topic, and the
// per-group backlog per topic so a sample can carry both.
func lagMaps(views []groupView, store *storage.Store) (map[string]map[int32]int64, map[string]map[string]int64) {
	byTopicPart := map[string]map[int32]int64{}
	byTopicGroup := map[string]map[string]int64{}
	if store == nil {
		return byTopicPart, byTopicGroup
	}
	for _, g := range views {
		for topic, parts := range g.Offsets {
			var total int64
			for part, committed := range parts {
				leo := int64(0)
				if p := store.GetPartition(topic, part); p != nil {
					leo = p.LogEndOffset()
				}
				lag := nonNegative(leo - committed)
				total += lag
				m := byTopicPart[topic]
				if m == nil {
					m = map[int32]int64{}
					byTopicPart[topic] = m
				}
				if lag > m[part] {
					m[part] = lag
				}
			}
			gm := byTopicGroup[topic]
			if gm == nil {
				gm = map[string]int64{}
				byTopicGroup[topic] = gm
			}
			gm[g.Name] = total
		}
	}
	return byTopicPart, byTopicGroup
}

// groupSummariesOf merges the per-topic group rows into one cluster-level list.
func groupSummariesOf(topics []topicHealth, views []groupView) []groupSummary {
	idx := map[string]*groupSummary{}
	order := []string{}
	for _, t := range topics {
		for _, g := range t.Groups {
			s := idx[g.Group]
			if s == nil {
				s = &groupSummary{Name: g.Group, State: g.State, Members: g.Members}
				idx[g.Group] = s
				order = append(order, g.Group)
			}
			s.Topics = append(s.Topics, t.Name)
			s.Lag += g.Lag
			s.LagPerSec += g.LagPerSec
			s.Stalled = s.Stalled || g.Stalled
		}
	}
	// Live groups that have not committed anything yet also belong in the list.
	for _, v := range views {
		s := idx[v.Name]
		if s == nil {
			s = &groupSummary{Name: v.Name}
			idx[v.Name] = s
			order = append(order, v.Name)
		}
		s.State = v.State
		s.Members = v.Members
		if s.Topics == nil {
			s.Topics = []string{}
		}
	}
	out := make([]groupSummary, 0, len(order))
	for _, name := range order {
		out = append(out, *idx[name])
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Lag != out[j].Lag {
			return out[i].Lag > out[j].Lag
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// totalLagWithTopics sums the backlog of every topic row.
func totalLagWithTopics(topics []topicHealth) int64 {
	var total int64
	for _, t := range topics {
		total += t.Lag
	}
	return total
}

// lastTopic returns the newest sample of a topic series.
func lastTopic(series []topicSample) (topicSample, bool) {
	if len(series) == 0 {
		return topicSample{}, false
	}
	return series[len(series)-1], true
}

// topicBaseline returns the oldest sample inside the window (or the series
// head when the window covers everything) and the elapsed time to now.
func topicBaseline(series []topicSample, window time.Duration, now time.Time) (topicSample, time.Duration) {
	if len(series) < 2 {
		return topicSample{}, 0
	}
	cur := series[len(series)-1]
	idx := 0
	cutoff := now.Add(-window)
	for i := len(series) - 1; i >= 0; i-- {
		if series[i].At.Before(cutoff) {
			idx = i
			break
		}
		idx = i
	}
	base := series[idx]
	if !base.At.Before(cur.At) {
		return topicSample{}, 0
	}
	return base, cur.At.Sub(base.At)
}

func lastPartition(series []topicSample, pid int32) (partitionSample, bool) {
	if len(series) == 0 {
		return partitionSample{}, false
	}
	last := series[len(series)-1]
	for _, p := range last.Partitions {
		if p.Partition == pid {
			return p, true
		}
	}
	return partitionSample{}, false
}

func partitionBaseline(series []topicSample, pid int32, window time.Duration, now time.Time) (partitionSample, bool) {
	base, _ := topicBaseline(series, window, now)
	if base.At.IsZero() {
		return partitionSample{}, false
	}
	for _, p := range base.Partitions {
		if p.Partition == pid {
			return p, true
		}
	}
	return partitionSample{}, false
}

func lastCluster(series []clusterSample) (clusterSample, bool) {
	if len(series) == 0 {
		return clusterSample{}, false
	}
	return series[len(series)-1], true
}

func clusterBaseline(series []clusterSample, window time.Duration, now time.Time) (clusterSample, time.Duration) {
	if len(series) < 2 {
		return clusterSample{}, 0
	}
	cur := series[len(series)-1]
	cutoff := now.Add(-window)
	idx := 0
	for i := len(series) - 1; i >= 0; i-- {
		idx = i
		if series[i].At.Before(cutoff) {
			break
		}
	}
	base := series[idx]
	if !base.At.Before(cur.At) {
		return clusterSample{}, 0
	}
	return base, cur.At.Sub(base.At)
}

// snapshot copies the sample ring for lock-free rendering.
func (m *Monitor) snapshot() (map[string][]topicSample, []clusterSample, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	topics := make(map[string][]topicSample, len(m.topics))
	for name, series := range m.topics {
		topics[name] = append([]topicSample(nil), series...)
	}
	return topics, append([]clusterSample(nil), m.cluster...), m.lastAt
}

// limits returns the sampling interval and how far back samples are kept.
func (m *Monitor) limits() (time.Duration, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.interval, m.retention
}

func (m *Monitor) lastSeenAt(topic string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.lastSeen[topic]
	return t, ok
}
