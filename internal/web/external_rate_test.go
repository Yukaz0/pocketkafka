package web

import (
	"testing"
	"time"
)

func TestExternalRateReliableNeedsTheWholeWindow(t *testing.T) {
	ec := &externalCluster{}
	slack := externalSampleInterval.Seconds()
	cases := []struct {
		name   string
		span   float64
		window time.Duration
		want   bool
	}{
		{"no span at all", 0, 60 * time.Second, false},
		{"single interval of history", slack, 60 * time.Second, false},
		{"just short of the window", 60 - slack - 0.5, 60 * time.Second, false},
		{"exactly the window minus slack", 60 - slack, 60 * time.Second, true},
		{"well past the window", 120, 60 * time.Second, true},
		{"10 minute span asked for an hour", 600, time.Hour, false},
		{"hour minus slack asked for an hour", 3600 - slack, time.Hour, true},
	}
	for _, tc := range cases {
		if got := ec.rateReliableFor(tc.span, tc.window); got != tc.want {
			t.Errorf("%s: span=%.1fs window=%s -> %v, want %v", tc.name, tc.span, tc.window, got, tc.want)
		}
	}
}

func TestExternalOverviewRecomputesReliabilityForTheRequestedWindow(t *testing.T) {
	now := time.Now()
	ec := &externalCluster{topicRing: map[string][]topicSample{}}
	ec.series = []clusterSample{{At: now.Add(-20 * time.Second)}, {At: now}}
	ec.topicRing["orders"] = []topicSample{{At: now.Add(-20 * time.Second)}, {At: now}}
	stored := overviewResponse{
		Cluster: clusterHealth{RateSpanSeconds: ec.spanSeconds(now)},
		Topics:  []topicHealth{{Name: "orders"}},
		History: []historyPoint{},
	}
	ec.last = &stored

	narrow, err := ec.overview(15*time.Second, now)
	if err != nil {
		t.Fatalf("overview(15s): %v", err)
	}
	if !narrow.Sampling.RateReliable {
		t.Fatalf("20s span with 15s window: cluster rate should be reliable")
	}
	if !narrow.Topics[0].RateReliable {
		t.Fatalf("20s span with 15s window: topic rate should be reliable")
	}

	wide, err := ec.overview(15*time.Minute, now)
	if err != nil {
		t.Fatalf("overview(15m): %v", err)
	}
	if wide.Sampling.RateReliable {
		t.Fatalf("20s span must not be reliable for a 15m window")
	}
	if wide.Topics[0].RateReliable {
		t.Fatalf("20s topic span must not be reliable for a 15m window")
	}
	if stored.Sampling.RateReliable {
		t.Fatalf("serving a window must not mutate the stored snapshot")
	}
}

func TestExternalReliabilityIsFalseWithoutASpan(t *testing.T) {
	now := time.Now()
	ec := &externalCluster{topicRing: map[string][]topicSample{}}
	ec.series = []clusterSample{{At: now}}
	stored := overviewResponse{Cluster: clusterHealth{RateSpanSeconds: ec.spanSeconds(now)}, Topics: []topicHealth{}, History: []historyPoint{}}
	ec.last = &stored

	out, err := ec.overview(time.Second, now)
	if err != nil {
		t.Fatalf("overview: %v", err)
	}
	if out.Sampling.RateReliable {
		t.Fatalf("one sample is not a span: rate must not be reliable")
	}
}
