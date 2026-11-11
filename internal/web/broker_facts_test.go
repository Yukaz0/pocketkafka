package web

import (
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func TestHistoryRingCoversEveryAcceptedWindow(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	if _, err := store.CreateTopic("hour", 1); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now().Add(-2 * time.Hour)
	for i := 0; i <= 120; i++ {
		s.monitor.Record(store, gm, t0.Add(time.Duration(i)*time.Minute), true)
	}
	now := t0.Add(120 * time.Minute)

	report := s.buildOverview(defaultThresholds(), time.Hour, now)
	if !report.Sampling.RateReliable {
		t.Fatal("a one-hour window must be reliable when an hour of history is retained")
	}
	if report.Sampling.RetentionSeconds != 3600 {
		t.Fatalf("retentionSeconds = %d, want 3600", report.Sampling.RetentionSeconds)
	}
	if len(report.History) == 0 {
		t.Fatal("no history returned for a one-hour window")
	}
	oldest := time.UnixMilli(report.History[0].T)
	if age := now.Sub(oldest); age > time.Hour+time.Minute {
		t.Fatalf("oldest history point is %v old, want at most an hour plus one sample", age)
	}

	for i := 1; i <= historyMaxSamples+400; i++ {
		s.monitor.Record(store, gm, now.Add(time.Duration(i)*time.Second), true)
	}
	_, series, _ := s.monitor.snapshot()
	if len(series) > historyMaxSamples {
		t.Fatalf("ring holds %d samples, cap is %d", len(series), historyMaxSamples)
	}
}

func TestRateReliableRejectsSpanShorterThanWindow(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	for i := 0; i <= 30; i++ {
		s.monitor.Record(store, gm, t0.Add(time.Duration(i)*10*time.Second), true)
	}
	now := t0.Add(300 * time.Second)

	covered := s.buildOverview(defaultThresholds(), 60*time.Second, now)
	if !covered.Sampling.RateReliable {
		t.Fatal("a 60s window must be reliable with 300s of history")
	}
	stretched := s.buildOverview(defaultThresholds(), time.Hour, now)
	if stretched.Sampling.RateReliable {
		t.Fatal("a one-hour window must not be reliable with only 300s of history")
	}
	if got := stretched.Cluster.RateSpanSeconds; got > 305 {
		t.Fatalf("rateSpanSeconds = %v, want the real span (~300), not the requested window", got)
	}
}

func TestConsumedRateCountsForwardProgressOnly(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("telemetry", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 200)

	t0 := time.Now()
	if err := gm.ResetOffsets("reader", "telemetry", 0, 10); err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, t0, true)

	if err := gm.ResetOffsets("reader", "telemetry", 0, 40); err != nil {
		t.Fatal(err)
	}
	if err := gm.ResetOffsets("latecomer", "telemetry", 0, 190); err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, t0.Add(10*time.Second), true)

	report := s.buildOverview(defaultThresholds(), time.Minute, t0.Add(10*time.Second))
	if got := report.Cluster.ConsumedPerSec; got != 3 {
		t.Fatalf("consumedPerSec = %v, want 3 (30 offsets over the 10s measured)", got)
	}
	if len(report.History) == 0 || report.History[len(report.History)-1].ConsumedPerSec != 3 {
		t.Fatalf("history must carry the consumed rate, got %+v", report.History)
	}

	if err := gm.ResetOffsets("reader", "telemetry", 0, 5); err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, t0.Add(20*time.Second), true)
	afterReset := s.buildOverview(defaultThresholds(), time.Minute, t0.Add(20*time.Second))
	if got := afterReset.Cluster.ConsumedPerSec; got != 1.5 {
		t.Fatalf("consumedPerSec after a reset = %v, want 1.5", got)
	}
	if last := afterReset.History[len(afterReset.History)-1]; last.ConsumedPerSec != 0 {
		t.Fatalf("the interval holding the reset = %v, want 0", last.ConsumedPerSec)
	}
}

func TestConsumedRateCountsAGroupThatAppearedMidWindow(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("late", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 200)

	t0 := time.Now()
	s.monitor.Record(store, gm, t0, true)
	if err := gm.ResetOffsets("newcomer", "late", 0, 0); err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, t0.Add(10*time.Second), true)
	if err := gm.ResetOffsets("newcomer", "late", 0, 100); err != nil {
		t.Fatal(err)
	}
	s.monitor.Record(store, gm, t0.Add(20*time.Second), true)

	report := s.buildOverview(defaultThresholds(), time.Minute, t0.Add(20*time.Second))
	if got := report.Cluster.ConsumedPerSec; got != 5 {
		t.Fatalf("consumedPerSec = %v, want 5 (100 offsets over the 20s measured)", got)
	}
}

func TestUnflushedBacklogWarnsOnlyAboveTheThreshold(t *testing.T) {
	s, store, gm := newHealthTestServer(t)
	topic, err := store.CreateTopic("tail", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 200)

	t0 := time.Now()
	s.monitor.Record(store, gm, t0, true)
	now := t0.Add(time.Second)

	report := s.buildOverview(defaultThresholds(), time.Minute, now)
	if report.Cluster.Broker.BytesAtRisk == 0 {
		t.Fatal("an un-synced append must be reported as bytes at risk")
	}
	if hasCode(report.Cluster.Reasons, "unflushed_backlog") {
		t.Fatal("a small un-synced tail must not raise a durability warning")
	}

	strict := defaultThresholds()
	strict.UnflushedWarnBytes = 1
	lowered := s.buildOverview(strict, time.Minute, now)
	if !hasCode(lowered.Cluster.Reasons, "unflushed_backlog") {
		t.Fatalf("reasons = %v, want unflushed_backlog once the tail exceeds the threshold", codesOf(lowered.Cluster.Reasons))
	}
	if lowered.Cluster.Status == StatusHealthy {
		t.Fatal("a cluster with more un-synced bytes than the threshold allows must not read healthy")
	}
}

func TestRetentionProjectionWarnsBeforeOffsetsAreLost(t *testing.T) {
	s, store, _ := newHealthTestServer(t)
	topic, err := store.CreateTopic("ret", 1)
	if err != nil {
		t.Fatal(err)
	}
	appendMessages(t, topic.Partitions[0], 1000)

	now := time.Now()
	series := []topicSample{
		{
			At:       now.Add(-time.Minute),
			Earliest: 100,
			Partitions: []partitionSample{
				{Partition: 0, LogEnd: 900, Bytes: 900, Messages: 900, Lag: 800},
			},
			Messages: 900, Bytes: 900, Lag: 800, MaxLag: 800,
		},
		{
			At:       now,
			Earliest: 160,
			Partitions: []partitionSample{
				{Partition: 0, LogEnd: 1000, Bytes: 1000, Messages: 1000, Lag: 800},
			},
			Messages: 1000, Bytes: 1000, Lag: 800, MaxLag: 800,
		},
	}
	row := s.buildTopicHealth(
		reportCtx{th: defaultThresholds(), window: time.Minute, now: now, interval: 2 * time.Second},
		"ret", topic, series, map[int32]int64{},
		[]groupView{{Name: "laggard", Offsets: map[string]map[int32]int64{"ret": {0: 200}}}},
	)

	if len(row.RetentionRisk) != 1 {
		t.Fatalf("retentionRisk = %+v, want one projected group", row.RetentionRisk)
	}
	risk := row.RetentionRisk[0]
	if risk.Group != "laggard" || risk.NextNeeded != 201 || risk.Earliest != 0 {
		t.Fatalf("risk = %+v, want laggard needing 201 with earliest 0", risk)
	}
	if risk.EarliestPerSec != 1 {
		t.Fatalf("earliestPerSec = %v, want 1", risk.EarliestPerSec)
	}
	if risk.SecondsUntilLoss == nil || *risk.SecondsUntilLoss != 201 {
		t.Fatalf("secondsUntilLoss = %v, want 201", risk.SecondsUntilLoss)
	}
	if row.Status != StatusCritical || !hasCode(row.Reasons, "retention_loss_imminent") {
		t.Fatalf("status = %q reasons = %v, want a critical retention warning", row.Status, codesOf(row.Reasons))
	}

	calm := s.buildTopicHealth(
		reportCtx{th: defaultThresholds(), window: time.Minute, now: now, interval: 2 * time.Second},
		"ret", topic, series, map[int32]int64{},
		[]groupView{{Name: "current", Offsets: map[string]map[int32]int64{"ret": {0: 1000}}}},
	)
	if len(calm.RetentionRisk) != 0 {
		t.Fatalf("a group at the log end was listed: %+v", calm.RetentionRisk)
	}
}

func TestRetentionProjectionFlagsOffsetsAlreadyDeleted(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 512, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	topic, err := store.CreateTopic("aged", 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		appendMessages(t, topic.Partitions[0], 1)
	}
	stop := store.StartRetention(storage.Retention{CheckInterval: 5 * time.Millisecond, RetentionBytes: 2048})
	t.Cleanup(stop)
	deadline := time.Now().Add(3 * time.Second)
	for topic.Partitions[0].EarliestOffset() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := topic.Partitions[0].EarliestOffset(); got == 0 {
		t.Fatal("retention never deleted a segment, so the projection cannot be exercised")
	}

	os, err := coordinator.NewOffsetStore("inmemory", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(os, 1, "localhost", 9092, 45000)
	s := New(store, gm, nil, 1, "test-cluster", "dev")
	now := time.Now()

	row := s.buildTopicHealth(
		reportCtx{th: defaultThresholds(), window: time.Minute, now: now, interval: 2 * time.Second},
		"aged", topic, nil, map[int32]int64{},
		[]groupView{{Name: "too-slow", Offsets: map[string]map[int32]int64{"aged": {0: 0}}}},
	)
	if len(row.RetentionRisk) != 1 || !row.RetentionRisk[0].AlreadyLost {
		t.Fatalf("retentionRisk = %+v, want one group flagged as already lost", row.RetentionRisk)
	}
	if row.Status != StatusCritical || !hasCode(row.Reasons, "retention_offset_lost") {
		t.Fatalf("status = %q reasons = %v, want retention_offset_lost", row.Status, codesOf(row.Reasons))
	}
	if row.Broker.RetentionDebtBytes != 0 || row.Broker.BytesAtRisk == 0 {
		t.Fatalf("debt = %d at risk = %d: a just-run pass owes nothing, and an un-synced append is at risk",
			row.Broker.RetentionDebtBytes, row.Broker.BytesAtRisk)
	}
}

func TestThinPointsKeepsBothEnds(t *testing.T) {
	points := make([]historyPoint, 1000)
	for i := range points {
		points[i] = historyPoint{T: int64(i)}
	}
	got := thinPoints(points, 240)
	if len(got) != 240 {
		t.Fatalf("thinned to %d points, want 240", len(got))
	}
	if got[0].T != 0 || got[len(got)-1].T != 999 {
		t.Fatalf("ends are %d..%d, want 0..999", got[0].T, got[len(got)-1].T)
	}
	for i := 1; i < len(got); i++ {
		if got[i].T <= got[i-1].T {
			t.Fatalf("thinned series is not increasing at %d", i)
		}
	}
	if short := thinPoints(points[:10], 240); len(short) != 10 {
		t.Fatalf("a short series was thinned to %d points", len(short))
	}
}
