package web

// Monitoring a Kafka cluster we do not host: the same report shape, filled from
// what a client can observe. Sampling runs on its own interval and the handlers
// serve the last snapshot, so a polling dashboard never multiplies load on
// somebody's production brokers, and the numbers stay consistent between the
// cluster row and the topic rows of one view.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/client"
)

const (
	// externalSampleInterval is how often a registered external cluster is
	// polled. It is deliberately slower than the local sampler: every pass costs
	// the remote cluster a metadata read plus one request per group.
	externalSampleInterval = 15 * time.Second
	// externalMaxTopics and externalMaxGroups bound one pass, so a cluster with
	// thousands of each still costs a predictable amount. What was skipped is
	// reported rather than silently dropped.
	externalMaxTopics = 300
	externalMaxGroups = 300
	// externalDialTimeout bounds connection and SASL setup per broker.
	externalDialTimeout = 8 * time.Second
	// externalLagWarn / externalLagCrit reuse the local verdict thresholds.
	externalUnavailableNote = "a client cannot observe disk usage, byte rates, or the broker's own durability, retention and compaction state"
)

// unavailableExternal lists the report fields an external cluster cannot fill.
var unavailableExternal = []string{"totalBytes", "bytes", "bytesPerSec", "diskUsagePct", "broker.durability", "broker.retentionDebt", "broker.compactionDebt"}

// externalTopicData is one pass's offsets for a topic.
type externalTopicData struct {
	leo      map[int32]int64
	earliest map[int32]int64
}

// externalGroupData is one pass's state and committed offsets for a group.
type externalGroupData struct {
	state   client.GroupState
	offsets map[string]map[int32]int64
}

// externalClusterClient is deliberately read-only; the same client backs the
// sampler and the registered target API adapter.
type externalClusterClient interface {
	Close() error
	Topics() []client.TopicInfo
	ListOffsetsBulk(int64) (map[string]map[int32]int64, error)
	ListEndOffsets(string) (map[int32]int64, error)
	ListStartOffsets(string) (map[int32]int64, error)
	ListGroups() ([]string, error)
	DescribeGroupsAll([]string) (map[string]client.GroupState, error)
	DescribeGroup(string) (client.GroupState, error)
	GroupOffsets(string) (map[string]map[int32]int64, error)
	FetchRecords(string, int32, int64, int) ([]client.FetchedRecord, int64, error)
	ClusterID() string
	Controller() int32
	Brokers() []client.Broker
}

var errExternalClientClosed = errors.New("external Kafka client is closed")

// listCacheTTL bounds how stale a cached topic or group list may be. The
// sampler's own snapshot runs on the same cadence, so a cached list is never
// older than the health data shown beside it.
const listCacheTTL = 15 * time.Second

// listCacheEntry is one cached list answer for an external target. refreshing
// marks a background re-read in flight so a poll storm starts only one.
type listCacheEntry struct {
	at         time.Time
	body       []byte
	refreshing bool
}

// externalCluster samples one registered "kafka" entry.
type externalCluster struct {
	name     string
	seeds    []string
	sasl     client.SASL
	clientID string
	// maxTopics and maxGroups bound one pass; zero means the package defaults.
	// The connectivity test keeps them small so a candidate answers in seconds.
	maxTopics int
	maxGroups int

	mu sync.Mutex
	// clientMu protects cc/closed and serializes creation, reads, failure, and close.
	// When both locks are needed, acquire clientMu before mu.
	clientMu sync.Mutex
	cc       externalClusterClient
	// readMu guards ic, the Data Browser's own connection. It is deliberately
	// separate from the sampler's: a pass over a large remote cluster holds
	// clientMu for seconds, and an interactive read queued behind it is a page
	// that looks hung. Never held together with clientMu.
	readMu sync.Mutex
	ic     *client.ClusterClient
	// listMu guards listCache, the short-lived copy of the topic and group list
	// answers. Never held with clientMu or readMu.
	listMu    sync.Mutex
	listCache map[string]listCacheEntry
	closed    bool
	series    []clusterSample
	topicRing map[string][]topicSample
	last      *overviewResponse
	lastErr   string
	sampledAt time.Time
	lastPass  time.Duration
	started   bool
	stop      chan struct{}
}

// externalFor returns the sampler for an entry, creating and starting it once.
func (s *Server) externalFor(e ClusterEntry) *externalCluster {
	s.externalMu.Lock()
	defer s.externalMu.Unlock()
	if s.external == nil {
		s.external = make(map[string]*externalCluster)
	}
	ec, ok := s.external[e.Name]
	if !ok {
		ec = &externalCluster{
			name:      e.Name,
			seeds:     append([]string(nil), e.Brokers...),
			sasl:      client.SASL{Mechanism: client.MechanismPlain, Username: e.SASLUser, Password: s.clusters.SASLPassword(e.Name)},
			clientID:  "pocketkafka-monitor",
			topicRing: make(map[string][]topicSample),
			stop:      make(chan struct{}),
		}
		s.external[e.Name] = ec
	}
	ec.mu.Lock()
	if !ec.started {
		ec.started = true
		go ec.run()
	}
	ec.mu.Unlock()
	return ec
}

// dropExternal stops sampling a cluster that was removed from the registry.
func (s *Server) dropExternal(name string) {
	s.externalMu.Lock()
	ec := s.external[name]
	delete(s.external, name)
	s.externalMu.Unlock()
	if ec != nil {
		ec.Close()
	}
}

// Close stops the sampler and its connections.
func (e *externalCluster) Close() {
	e.mu.Lock()
	if e.stop != nil {
		select {
		case <-e.stop:
		default:
			close(e.stop)
		}
	}
	e.mu.Unlock()

	e.clientMu.Lock()
	e.closed = true
	cc := e.cc
	e.cc = nil
	e.clientMu.Unlock()
	if cc != nil {
		cc.Close()
	}
	e.readMu.Lock()
	ic := e.ic
	e.ic = nil
	e.readMu.Unlock()
	if ic != nil {
		ic.Close()
	}
}

// run samples once immediately, then on the interval until stopped.
func (e *externalCluster) run() {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-timer.C:
		}
		e.sample()
		timer.Reset(externalSampleInterval)
	}
}

// sample performs one read-only pass over the remote cluster. Requests are
// batched per broker and per coordinator: a pass costs a handful of round trips
// per broker plus one per group, which is what keeps a 75-topic cluster cheap
// enough to sample on an interval.
func (e *externalCluster) sample() {
	started := time.Now()
	now := started
	err := e.withClient(func(cc externalClusterClient) error {
		return e.sampleWithClient(cc, started, now)
	})
	if err != nil {
		e.fail(err, now)
	}
}

func (e *externalCluster) sampleWithClient(cc externalClusterClient, started, now time.Time) error {
	topics := cc.Topics()
	maxTopics, maxGroups := e.maxTopics, e.maxGroups
	if maxTopics <= 0 {
		maxTopics = externalMaxTopics
	}
	if maxGroups <= 0 {
		maxGroups = externalMaxGroups
	}

	// Every partition's log end and earliest offset: two requests per broker.
	latest, err := cc.ListOffsetsBulk(-1)
	if err != nil {
		return fmt.Errorf("list end offsets: %w", err)
	}
	earliest, err := cc.ListOffsetsBulk(-2)
	if err != nil {
		return fmt.Errorf("list start offsets: %w", err)
	}

	topicData := make(map[string]externalTopicData, len(latest))
	skippedTopics := 0
	names := make([]string, 0, len(latest))
	for name := range latest {
		names = append(names, name)
	}
	sort.Strings(names)
	for i, name := range names {
		if i >= maxTopics {
			skippedTopics = len(names) - maxTopics
			break
		}
		start := earliest[name]
		if start == nil {
			start = map[int32]int64{}
		}
		topicData[name] = externalTopicData{leo: latest[name], earliest: start}
	}

	groups, err := cc.ListGroups()
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	skippedGroups := 0
	if len(groups) > maxGroups {
		skippedGroups = len(groups) - maxGroups
		groups = groups[:maxGroups]
	}
	states, err := cc.DescribeGroupsAll(groups)
	if err != nil {
		return fmt.Errorf("describe groups: %w", err)
	}
	groupData := make(map[string]externalGroupData, len(groups))
	for _, g := range groups {
		offs, err := cc.GroupOffsets(g)
		if err != nil {
			// A group that cannot be read is not a reason to lose the pass.
			continue
		}
		st, ok := states[g]
		if !ok {
			st = client.GroupState{Group: g, State: "Unknown"}
		}
		groupData[g] = externalGroupData{state: st, offsets: offs}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastErr = ""
	e.sampledAt = now
	e.lastPass = time.Since(started)

	// Ring sample: message totals and lag, so rates and history work exactly as
	// they do for the local broker.
	cs := clusterSample{At: now, Committed: make(map[string]int64, len(groupData))}
	ts := make(map[string]topicSample, len(topicData))
	var groupLagTotal int64
	for name, data := range topicData {
		var messages, lag int64
		sample := topicSample{At: now, Earliest: -1}
		for pid, end := range data.leo {
			start := data.earliest[pid]
			messages += nonNegative(end - start)
			if sample.Earliest < 0 || start < sample.Earliest {
				sample.Earliest = start
			}
			sample.Partitions = append(sample.Partitions, partitionSample{
				Partition: pid, LogEnd: end, Messages: nonNegative(end - start),
			})
		}
		sort.Slice(sample.Partitions, func(i, j int) bool { return sample.Partitions[i].Partition < sample.Partitions[j].Partition })
		sample.Messages = messages
		sample.GroupLag = make(map[string]int64)
		for gname, gd := range groupData {
			byPart, ok := gd.offsets[name]
			if !ok {
				continue
			}
			var gl int64
			for pid, committed := range byPart {
				if end, ok := data.leo[pid]; ok && end > committed {
					gl += end - committed
				}
			}
			if gl > 0 {
				sample.GroupLag[gname] = gl
				if gl > sample.MaxLag {
					sample.MaxLag = gl
				}
			}
			lag += gl
		}
		sample.Lag = lag
		groupLagTotal += lag
		cs.Messages += messages
		ts[name] = sample
	}
	cs.Lag = groupLagTotal
	e.series = trimSamples(append(e.series, cs), now, historyRetention, historyMaxSamples)
	for name, sample := range ts {
		e.topicRing[name] = trimSamples(append(e.topicRing[name], sample), now, historyRetention, historyMaxSamples)
	}
	for name := range e.topicRing {
		if _, ok := ts[name]; !ok {
			delete(e.topicRing, name)
		}
	}

	e.last = e.build(cc, topics, topicData, groupData, skippedTopics, skippedGroups, now)
	return nil
}

// fail records a pass that could not read the cluster, keeping the previous
// snapshot so the view degrades instead of going blank. The caller has released
// its client operation before failure invalidates and closes the connection.
func (e *externalCluster) fail(err error, now time.Time) {
	e.clientMu.Lock()
	cc := e.cc
	e.cc = nil
	e.mu.Lock()
	e.lastErr = err.Error()
	e.sampledAt = now
	e.mu.Unlock()
	e.clientMu.Unlock()
	if cc != nil {
		cc.Close()
	}
}

// withClient makes client creation single-flight and serializes the complete
// read operation against other reads, failure invalidation, and shutdown.
// Callbacks run while clientMu is held and may acquire mu, so code must not hold
// mu while trying to acquire clientMu.
func (e *externalCluster) withClient(read func(externalClusterClient) error) error {
	e.clientMu.Lock()
	defer e.clientMu.Unlock()
	if e.closed {
		return errExternalClientClosed
	}
	if e.cc == nil {
		cc, err := client.NewClusterClient(e.seeds, e.clientID, client.Options{SASL: e.sasl, ReadOnly: true})
		if err != nil {
			return err
		}
		e.cc = cc
	}
	return read(e.cc)
}

// cachedList returns the last stored answer for a target list endpoint. A copy
// older than listCacheTTL is still served — the caller gets an answer now — and
// a single background refresh is started through the refresher so the next
// caller gets a fresh one. Only a cold cache (nothing stored yet) reports false.
func (e *externalCluster) cachedList(path string, refresh func()) ([]byte, bool) {
	e.listMu.Lock()
	entry, ok := e.listCache[path]
	if !ok {
		e.listMu.Unlock()
		return nil, false
	}
	if time.Since(entry.at) > listCacheTTL && !entry.refreshing {
		entry.refreshing = true
		e.listCache[path] = entry
		e.listMu.Unlock()
		go func() {
			refresh()
			e.listMu.Lock()
			// Clear the flag even when the refresh failed, so the next caller
			// tries again instead of waiting for a refresh that never comes.
			if current, ok := e.listCache[path]; ok && current.refreshing {
				current.refreshing = false
				e.listCache[path] = current
			}
			e.listMu.Unlock()
		}()
		return entry.body, true
	}
	e.listMu.Unlock()
	return entry.body, true
}

// storeList keeps one list answer for the next poll or page reload.
func (e *externalCluster) storeList(path string, body []byte) {
	e.listMu.Lock()
	defer e.listMu.Unlock()
	if e.listCache == nil {
		e.listCache = make(map[string]listCacheEntry)
	}
	e.listCache[path] = listCacheEntry{at: time.Now(), body: append([]byte(nil), body...)}
}

// withReadClient runs one interactive read (Data Browser page, topic list,
// group list) on a connection of its own. The sampler holds clientMu for its
// whole pass, which on a large remote cluster is seconds; an interactive read
// waiting for it is a page that never loads. A failed read drops the connection
// so the next request dials fresh instead of reusing a broken one.
func (e *externalCluster) withReadClient(read func(externalClusterClient) error) error {
	e.readMu.Lock()
	defer e.readMu.Unlock()
	e.clientMu.Lock()
	closed := e.closed
	e.clientMu.Unlock()
	if closed {
		return errExternalClientClosed
	}
	if e.ic == nil {
		cc, err := client.NewClusterClient(e.seeds, e.clientID, client.Options{SASL: e.sasl, ReadOnly: true})
		if err != nil {
			return err
		}
		e.ic = cc
	}
	if err := read(e.ic); err != nil {
		e.ic.Close()
		e.ic = nil
		return err
	}
	return nil
}

// newReadClient dials an independent read-only client for a streaming session
// (live tail). It is deliberately separate from the sampler's shared client so a
// long-lived stream never holds clientMu and never starves sampling. The dial
// runs outside the lock for the same reason: an unreachable seed would block
// every sampler pass for the whole connect timeout.
func (e *externalCluster) newReadClient() (*client.ClusterClient, error) {
	e.clientMu.Lock()
	closed := e.closed
	e.clientMu.Unlock()
	if closed {
		return nil, errExternalClientClosed
	}
	return client.NewClusterClient(e.seeds, e.clientID, client.Options{SASL: e.sasl, ReadOnly: true})
}

// build turns one pass into a report. The caller holds e.mu.
func (e *externalCluster) build(
	cc externalClusterClient,
	topics []client.TopicInfo,
	topicData map[string]externalTopicData,
	groupData map[string]externalGroupData,
	skippedTopics, skippedGroups int,
	now time.Time,
) *overviewResponse {
	report := &overviewResponse{
		Cluster: clusterHealth{
			ClusterID:       cc.ClusterID(),
			BrokerID:        cc.Controller(),
			Status:          StatusHealthy,
			Source:          sourceExternal,
			Unavailable:     unavailableExternal,
			Topics:          len(topics),
			Groups:          len(groupData),
			TotalMessages:   lastSampleMessages(e.series),
			MessagesPerSec:  e.rate(func(c clusterSample) int64 { return c.Messages }),
			ConsumedPerSec:  e.consumed(),
			RateSpanSeconds: e.spanSeconds(now),
			Reasons:         []healthReason{},
			Listeners:       brokerAddrs(cc),
		},
		Topics:    make([]topicHealth, 0, len(topicData)),
		Groups:    make([]groupSummary, 0, len(groupData)),
		History:   e.history(),
		Attention: []topicHealth{},
	}
	report.Cluster.Lag = lastSampleLag(e.series)
	report.Cluster.MaxPartitionLag = lastSampleMaxPartitionLag(e.series)
	report.Cluster.DiskUsagePct = nil
	partitions := 0
	for _, t := range topics {
		if !t.Internal {
			partitions += len(t.Partitions)
		}
	}
	report.Cluster.Partitions = partitions

	// Topology facts a single-node broker cannot have.
	underReplicated, offline := 0, 0
	names := make([]string, 0, len(topicData))
	for name := range topicData {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, t := range topics {
		if t.Internal {
			continue
		}
		data, ok := topicData[t.Name]
		if !ok {
			continue
		}
		row := topicHealth{
			Name:              t.Name,
			Source:            sourceExternal,
			Unavailable:       unavailableExternal,
			Partitions:        len(t.Partitions),
			ReplicationFactor: replicationFactor(t.Partitions),
			PartitionsDetail:  make([]partitionHealth, 0, len(t.Partitions)),
			Groups:            []groupLagRow{},
			Reasons:           []healthReason{},
		}
		var messages, lag, maxPartLag int64
		for _, p := range t.Partitions {
			end := data.leo[p.ID]
			start := data.earliest[p.ID]
			ph := partitionHealth{
				Partition: p.ID,
				Leader:    p.Leader,
				LogEnd:    end,
				Earliest:  start,
				Messages:  nonNegative(end - start),
				Bytes:     nil,
				ISR:       p.ISR,
				Replicas:  p.Replicas,
			}
			if len(p.ISR) < len(p.Replicas) {
				underReplicated++
			}
			if len(p.Offline) > 0 {
				offline++
			}
			messages += ph.Messages
			ph.SkewRatio = float64(len(p.Replicas))
			row.PartitionsDetail = append(row.PartitionsDetail, ph)
		}
		for gname, gd := range groupData {
			byPart, ok := gd.offsets[t.Name]
			if !ok {
				continue
			}
			gl := groupLagRow{Group: gname, State: gd.state.State, Members: gd.state.Members}
			for pid, committed := range byPart {
				if end, ok := data.leo[pid]; ok {
					gl.Lag += nonNegative(end - committed)
				}
			}
			if gl.Lag > maxPartLag {
				maxPartLag = gl.Lag
			}
			lag += gl.Lag
			if gl.Lag > 0 && gd.state.Members == 0 {
				gl.Stalled = true
			}
			row.Groups = append(row.Groups, gl)
		}
		sort.Slice(row.Groups, func(i, j int) bool { return row.Groups[i].Lag > row.Groups[j].Lag })
		row.Messages = messages
		row.Lag = lag
		row.MaxPartitionLag = maxPartLag
		row.Bytes = nil
		row.BytesPerSec = nil
		row.MessagesPerSec = e.topicRate(t.Name, func(s topicSample) int64 { return s.Messages })
		row.RateSpanSeconds = e.topicSpanSeconds(t.Name)
		row.RateReliable = e.rateReliableFor(row.RateSpanSeconds, defaultWindow)
		row.Trend = trendOf(lag, e.topicLagRate(t.Name))
		row.LagPerSec = e.topicLagRate(t.Name)
		row.Broker = nil
		row.Status, row.Reasons = evaluateTopic(defaultThresholds(), topicMetrics{
			Partitions:     len(t.Partitions),
			Messages:       messages,
			MessagesPerSec: row.MessagesPerSec,
			Lag:            lag,
			LagPerSec:      row.LagPerSec,
			SkewRatio:      0, // no byte sizes to compare; message counts vary by design
			HasActivity:    row.MessagesPerSec > 0,
		})
		report.Topics = append(report.Topics, row)
	}

	// Group summaries across topics, so the group list answers "who is behind".
	groupTotals := make(map[string]*groupSummary, len(groupData))
	for name, gd := range groupData {
		held := make([]string, 0, len(gd.offsets))
		for topic := range gd.offsets {
			held = append(held, topic)
		}
		sort.Strings(held)
		groupTotals[name] = &groupSummary{Name: name, State: gd.state.State, Members: gd.state.Members, Topics: held}
	}
	for _, row := range report.Topics {
		for _, gl := range row.Groups {
			if gs, ok := groupTotals[gl.Group]; ok {
				gs.Lag += gl.Lag
				if gl.Stalled {
					gs.Stalled = true
				}
			}
		}
	}
	groupNames := make([]string, 0, len(groupTotals))
	for name := range groupTotals {
		groupNames = append(groupNames, name)
	}
	sort.Slice(groupNames, func(i, j int) bool {
		if groupTotals[groupNames[i]].Lag != groupTotals[groupNames[j]].Lag {
			return groupTotals[groupNames[i]].Lag > groupTotals[groupNames[j]].Lag
		}
		return groupNames[i] < groupNames[j]
	})
	for _, name := range groupNames {
		report.Groups = append(report.Groups, *groupTotals[name])
	}

	// Verdict from client-visible facts only.
	reasons := []healthReason{}
	status := StatusHealthy
	if underReplicated > 0 {
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code: "under_replicated", Severity: "warn",
			Message: fmt.Sprintf("%d partitions have fewer in-sync replicas than replicas", underReplicated),
		})
	}
	if offline > 0 {
		status = worseStatus(status, StatusCritical)
		reasons = append(reasons, healthReason{
			Code: "offline_partitions", Severity: "critical",
			Message: fmt.Sprintf("%d partitions report offline replicas", offline),
		})
	}
	stalledGroups := 0
	for _, gs := range report.Groups {
		if gs.Stalled {
			stalledGroups++
		}
	}
	if stalledGroups > 0 {
		status = worseStatus(status, StatusDegraded)
		reasons = append(reasons, healthReason{
			Code: "group_stalled", Severity: "warn",
			Message: fmt.Sprintf("%d groups hold a backlog with no active members", stalledGroups),
		})
	}
	if skippedTopics > 0 || skippedGroups > 0 {
		reasons = append(reasons, healthReason{
			Code: "sampling_capped", Severity: "info",
			Message: fmt.Sprintf("this pass skipped %d topics and %d groups (per-pass cap)", skippedTopics, skippedGroups),
		})
	}
	worst := StatusHealthy
	for _, row := range report.Topics {
		worst = worseStatus(worst, row.Status)
		if row.Status == StatusDegraded || row.Status == StatusCritical {
			report.Attention = append(report.Attention, row)
		}
	}
	status = worseStatus(status, worst)
	report.Cluster.Status = status
	report.Cluster.Reasons = reasons
	report.Cluster.Unavailable = unavailableExternal
	report.Sampling = samplingInfo{
		IntervalMs:       int64(externalSampleInterval / time.Millisecond),
		Samples:          len(e.series),
		WindowSeconds:    int(defaultWindow / time.Second),
		RetentionSeconds: int(historyRetention / time.Second),
		HistoryPoints:    len(report.History),
		RateReliable:     e.rateReliableFor(report.Cluster.RateSpanSeconds, defaultWindow),
		StartedAtMs:      now.UnixMilli(),
	}
	return report
}

func brokerAddrs(cc externalClusterClient) []string {
	out := []string{}
	for _, b := range cc.Brokers() {
		out = append(out, b.Addr())
	}
	sort.Strings(out)
	return out
}

func replicationFactor(parts []client.PartitionInfo) int {
	if len(parts) == 0 {
		return 0
	}
	return len(parts[0].Replicas)
}

func trendOf(lag int64, lagPerSec float64) string {
	switch {
	case lagPerSec > 0.5:
		return "rising"
	case lagPerSec < -0.5:
		return "falling"
	default:
		return "flat"
	}
}

// Report helpers: the caller holds e.mu.

func (e *externalCluster) rate(pick func(clusterSample) int64) float64 {
	if len(e.series) < 2 {
		return 0
	}
	prev, cur := e.series[len(e.series)-2], e.series[len(e.series)-1]
	d := cur.At.Sub(prev.At)
	if d <= 0 {
		return 0
	}
	return rate(float64(pick(cur)-pick(prev)), d)
}

func (e *externalCluster) consumed() float64 {
	if len(e.series) < 2 {
		return 0
	}
	prev, cur := e.series[len(e.series)-2], e.series[len(e.series)-1]
	d := cur.At.Sub(prev.At)
	if d <= 0 {
		return 0
	}
	return rate(float64(progressBetween(prev, cur)), d)
}

func (e *externalCluster) spanSeconds(now time.Time) float64 {
	if len(e.series) < 2 {
		return 0
	}
	return now.Sub(e.series[0].At).Seconds()
}

// rateReliableFor mirrors the local monitor's rule: a rate is only trustworthy
// when the measured span covers the requested window, with one sampling interval
// of slack for ring trimming. A shorter span means the baseline predates the
// window's edge, so the derived rate understates reality.
func (e *externalCluster) rateReliableFor(span float64, window time.Duration) bool {
	if span <= 0 {
		return false
	}
	return span >= window.Seconds()-externalSampleInterval.Seconds()
}

func (e *externalCluster) history() []historyPoint {
	out := make([]historyPoint, 0, len(e.series))
	for i := 1; i < len(e.series); i++ {
		prev, cur := e.series[i-1], e.series[i]
		d := cur.At.Sub(prev.At)
		if d <= 0 {
			continue
		}
		out = append(out, historyPoint{
			T:              cur.At.UnixMilli(),
			MessagesPerSec: rate(float64(cur.Messages-prev.Messages), d),
			ConsumedPerSec: rate(float64(progressBetween(prev, cur)), d),
			Lag:            cur.Lag,
		})
	}
	return thinPoints(out, historyPoints)
}

func (e *externalCluster) topicRate(topic string, pick func(topicSample) int64) float64 {
	series := e.topicRing[topic]
	if len(series) < 2 {
		return 0
	}
	prev, cur := series[len(series)-2], series[len(series)-1]
	d := cur.At.Sub(prev.At)
	if d <= 0 {
		return 0
	}
	return rate(float64(pick(cur)-pick(prev)), d)
}

func (e *externalCluster) topicLagRate(topic string) float64 {
	series := e.topicRing[topic]
	if len(series) < 2 {
		return 0
	}
	prev, cur := series[len(series)-2], series[len(series)-1]
	d := cur.At.Sub(prev.At)
	if d <= 0 {
		return 0
	}
	return rate(float64(cur.Lag-prev.Lag), d)
}

func (e *externalCluster) topicSpanSeconds(topic string) float64 {
	series := e.topicRing[topic]
	if len(series) < 2 {
		return 0
	}
	return series[len(series)-1].At.Sub(series[0].At).Seconds()
}

func lastSampleMessages(series []clusterSample) int64 {
	if len(series) == 0 {
		return 0
	}
	return series[len(series)-1].Messages
}

func lastSampleLag(series []clusterSample) int64 {
	if len(series) == 0 {
		return 0
	}
	return series[len(series)-1].Lag
}

func lastSampleMaxPartitionLag(series []clusterSample) int64 {
	var max int64
	for _, s := range series {
		if s.Lag > max {
			max = s.Lag
		}
	}
	return max
}

// awaitFirstSample waits for a first successful pass, so a view opened right
// after registration shows data instead of an error.
func (e *externalCluster) awaitFirstSample(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		e.mu.Lock()
		done := e.last != nil
		e.mu.Unlock()
		if done || time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// sampleOnce reads a candidate cluster exactly once without registering it, so
// a test cannot leave a sampler (and a goroutine holding credentials) behind.
// The password is passed in because a cluster entry never carries a secret.
func (s *Server) sampleOnce(ctx context.Context, e ClusterEntry, password string) (overviewResponse, error) {
	ec := &externalCluster{
		name:      e.Name,
		seeds:     append([]string(nil), e.Brokers...),
		sasl:      client.SASL{Mechanism: client.MechanismPlain, Username: e.SASLUser, Password: password},
		clientID:  "pocketkafka-monitor",
		topicRing: make(map[string][]topicSample),
		stop:      make(chan struct{}),
		// A test answers a connectivity question, so it reads a sample of the
		// cluster instead of all of it: SASL, topology, a few topics, a few
		// groups. The full pass runs when the entry is registered.
		maxTopics: 5,
		maxGroups: 5,
	}
	defer ec.Close()

	done := make(chan struct{})
	go func() {
		ec.sample()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return overviewResponse{}, ctx.Err()
	case <-time.After(externalDialTimeout + externalSampleInterval):
		return overviewResponse{}, fmt.Errorf("sampling timed out")
	}
	overview, err := ec.overview(defaultWindow, time.Now())
	if err != nil {
		return overviewResponse{}, err
	}
	return overview, nil
}

// overview serves the last snapshot. While the first pass is still running it
// answers with a shape-complete report saying so, instead of blocking the
// request or reporting an error for a cluster that is simply not read yet.
func (e *externalCluster) overview(window time.Duration, now time.Time) (overviewResponse, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.last == nil {
		if e.lastErr != "" {
			return overviewResponse{}, fmt.Errorf("external cluster %q: %s", e.name, e.lastErr)
		}
		return e.pendingReport(window, now), nil
	}
	out := *e.last
	// Deep-copy the slices a caller may keep, keeping an empty slice empty so a
	// healthy report serializes as [] rather than null.
	out.Topics = copySlice(e.last.Topics)
	out.Groups = copySlice(e.last.Groups)
	out.Cluster.Reasons = copySlice(e.last.Cluster.Reasons)
	out.Sampling.WindowSeconds = int(window.Seconds())
	out.Sampling.HistoryPoints = len(out.History)
	// The stored snapshot is built for the default window; a caller asking for a
	// different one must get reliability recomputed against ITS window, or a
	// wide window would be presented as trustworthy on a short span.
	out.Sampling.RateReliable = e.rateReliableFor(out.Cluster.RateSpanSeconds, window)
	for i := range out.Topics {
		out.Topics[i].RateReliable = e.rateReliableFor(e.topicSpanSeconds(out.Topics[i].Name), window)
	}
	return out, nil
}

// copySlice clones a slice without turning empty into nil.
func copySlice[T any](in []T) []T {
	out := make([]T, 0, len(in))
	return append(out, in...)
}

// pendingReport is the report for a cluster whose first pass has not finished.
// It keeps every field a renderer expects, and says plainly that nothing has
// been read yet rather than showing zeroes that look like measurements.
func (e *externalCluster) pendingReport(window time.Duration, now time.Time) overviewResponse {
	return overviewResponse{
		GeneratedAtMs: now.UnixMilli(),
		Cluster: clusterHealth{
			Status:      "unknown",
			Source:      sourceExternal,
			Unavailable: unavailableExternal,
			Reasons: []healthReason{{
				Code:     "no_sample_yet",
				Severity: "info",
				Message:  "the first read-only pass over this cluster has not finished",
			}},
		},
		Topics:    []topicHealth{},
		Groups:    []groupSummary{},
		Attention: []topicHealth{},
		History:   []historyPoint{},
		Sampling: samplingInfo{
			IntervalMs:       int64(externalSampleInterval / time.Millisecond),
			WindowSeconds:    int(window.Seconds()),
			RetentionSeconds: int(historyRetention / time.Second),
			Warmup:           true,
		},
	}
}

// externalOverview serves the snapshot of a registered external cluster.
func (s *Server) externalOverview(name string, window time.Duration, now time.Time) (overviewResponse, error) {
	s.externalMu.Lock()
	ec := s.external[name]
	s.externalMu.Unlock()
	if ec == nil {
		return overviewResponse{}, fmt.Errorf("external cluster %q is not started", name)
	}
	return ec.overview(window, now)
}
