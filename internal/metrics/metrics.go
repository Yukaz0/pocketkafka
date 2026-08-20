// Package metrics implements a small Prometheus text-format exporter built on
// the storage engine. It adds monotonic counters and consumer-lag gauges on
// top of the existing ad-hoc metrics in the web UI.
package metrics

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

// Registry accumulates counters and renders Prometheus text format.
type Registry struct {
	mu        sync.Mutex
	startTime time.Time

	// Counters (monotonic).
	messagesIn map[string]int64 // topic -> count
	bytesIn    map[string]int64 // topic -> bytes

	// Request latency histogram buckets (seconds).
	latencyBuckets []float64
	latencyCounts  map[string][]uint64 // api -> bucket counts
	latencySum     map[string]float64
	latencyTotal   map[string]uint64
}

// NewRegistry builds an empty metrics registry.
func NewRegistry() *Registry {
	return &Registry{
		startTime:      time.Now(),
		messagesIn:     make(map[string]int64),
		bytesIn:        make(map[string]int64),
		latencyBuckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		latencyCounts:  make(map[string][]uint64),
		latencySum:     make(map[string]float64),
		latencyTotal:   make(map[string]uint64),
	}
}

// ObserveProduce records one produced batch for the metrics registry.
func (r *Registry) ObserveProduce(topic string, count, bytes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messagesIn[topic] += int64(count)
	r.bytesIn[topic] += int64(bytes)
}

// ObserveLatency records a request duration for an API key.
func (r *Registry) ObserveLatency(api string, seconds float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.latencyTotal[api]++
	r.latencySum[api] += seconds
	for i, b := range r.latencyBuckets {
		if seconds <= b {
			if r.latencyCounts[api] == nil {
				r.latencyCounts[api] = make([]uint64, len(r.latencyBuckets))
			}
			r.latencyCounts[api][i]++
		}
	}
}

// Render produces the Prometheus text exposition.
func (r *Registry) Render(store *storage.Store, gm *coordinator.GroupManager, clusterID string) string {
	r.mu.Lock()
	messages := make(map[string]int64, len(r.messagesIn))
	bytes := make(map[string]int64, len(r.bytesIn))
	for k, v := range r.messagesIn {
		messages[k] = v
	}
	for k, v := range r.bytesIn {
		bytes[k] = v
	}
	latCounts := make(map[string][]uint64, len(r.latencyCounts))
	for k, v := range r.latencyCounts {
		latCounts[k] = append([]uint64(nil), v...)
	}
	latSum := make(map[string]float64, len(r.latencySum))
	for k, v := range r.latencySum {
		latSum[k] = v
	}
	latTotal := make(map[string]uint64, len(r.latencyTotal))
	for k, v := range r.latencyTotal {
		latTotal[k] = v
	}
	r.mu.Unlock()

	var b strings.Builder

	// Counters.
	fmt.Fprintf(&b, "# HELP kafka_messages_in_total Total messages appended\n")
	fmt.Fprintf(&b, "# TYPE kafka_messages_in_total counter\n")
	for topic, n := range messages {
		fmt.Fprintf(&b, "kafka_messages_in_total{topic=%q} %d\n", topic, n)
	}
	fmt.Fprintf(&b, "# HELP kafka_bytes_in_total Total bytes received\n")
	fmt.Fprintf(&b, "# TYPE kafka_bytes_in_total counter\n")
	for topic, n := range bytes {
		fmt.Fprintf(&b, "kafka_bytes_in_total{topic=%q} %d\n", topic, n)
	}

	// Consumer lag gauge per group/topic/partition.
	fmt.Fprintf(&b, "# HELP kafka_consumer_lag Current partition lag\n")
	fmt.Fprintf(&b, "# TYPE kafka_consumer_lag gauge\n")
	for _, info := range gm.ListGroups() {
		for topic, parts := range info.Offsets {
			for part, committed := range parts {
				leo := int64(0)
				if p := store.GetPartition(topic, part); p != nil {
					leo = p.LogEndOffset()
				}
				lag := leo - committed
				if lag < 0 {
					lag = 0
				}
				fmt.Fprintf(&b, "kafka_consumer_lag{group=%q,topic=%q,partition=%q} %d\n",
					info.Name, topic, fmt.Sprintf("%d", part), lag)
			}
		}
	}

	// Request latency histogram.
	fmt.Fprintf(&b, "# HELP kafka_request_latency_seconds Request duration\n")
	fmt.Fprintf(&b, "# TYPE kafka_request_latency_seconds histogram\n")
	apis := make([]string, 0, len(latTotal))
	for api := range latTotal {
		apis = append(apis, api)
	}
	for _, api := range apis {
		var cum uint64
		for i, bkt := range r.latencyBuckets {
			cum += latCounts[api][i]
			fmt.Fprintf(&b, "kafka_request_latency_seconds_bucket{api=%q,le=%q} %d\n", api, fmt.Sprintf("%g", bkt), cum)
		}
		fmt.Fprintf(&b, "kafka_request_latency_seconds_bucket{api=%q,le=\"+Inf\"} %d\n", api, latTotal[api])
		fmt.Fprintf(&b, "kafka_request_latency_seconds_sum{api=%q} %g\n", api, latSum[api])
		fmt.Fprintf(&b, "kafka_request_latency_seconds_count{api=%q} %d\n", api, latTotal[api])
	}

	// Broker info + uptime.
	fmt.Fprintf(&b, "kafka_broker_info{cluster_id=%q} 1\n", clusterID)
	fmt.Fprintf(&b, "kafka_broker_uptime_seconds %g\n", time.Since(r.startTime).Seconds())

	// Storage gauges (computed live).
	topics := store.TopicsSnapshot()
	var partitions int
	var totalBytes, totalMsgs int64
	for _, t := range topics {
		for pid, p := range t.Partitions {
			partitions++
			leo := p.LogEndOffset()
			earliest := p.EarliestOffset()
			bytes := p.SizeBytes()
			totalBytes += bytes
			totalMsgs += leo - earliest
			fmt.Fprintf(&b, "pocketkafka_topic_log_end_offset{topic=%q,partition=%q} %d\n", t.Name, fmt.Sprintf("%d", pid), leo)
			fmt.Fprintf(&b, "pocketkafka_topic_bytes{topic=%q,partition=%q} %d\n", t.Name, fmt.Sprintf("%d", pid), bytes)
		}
	}
	fmt.Fprintf(&b, "pocketkafka_topics %d\n", len(topics))
	fmt.Fprintf(&b, "pocketkafka_partitions %d\n", partitions)
	fmt.Fprintf(&b, "pocketkafka_total_bytes %d\n", totalBytes)
	fmt.Fprintf(&b, "pocketkafka_total_messages %d\n", totalMsgs)

	return b.String()
}
