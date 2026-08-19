// Package client is a hand-written, zero external dependency Kafka client SDK
// that runs on top of pkg/protocol. It provides a connection pool, metadata
// cache, a batching producer, and a rebalancing consumer group.
package client

import "time"

// ProducerConfig configures the producer.
type ProducerConfig struct {
	Acks         int16         // 0: no ack, 1: leader ack, -1: all ISR
	Timeout      time.Duration // produce request timeout
	BatchSize    int           // max records per flushed batch
	LingerMs     time.Duration // linger before flushing a batch
	MaxRetries   int
	RetryBackoff time.Duration
}

// DefaultProducerConfig returns sensible defaults.
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Acks:         1,
		Timeout:      10 * time.Second,
		BatchSize:    1000,
		LingerMs:     10 * time.Millisecond,
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
	}
}

// ConsumerGroupConfig configures the high-level consumer group.
type ConsumerGroupConfig struct {
	GroupID            string
	SessionTimeout     time.Duration // default 30s
	HeartbeatInterval  time.Duration // default 3s
	AutoCommit         bool
	AutoCommitInterval time.Duration
	InitialOffset      int64 // -1 latest, -2 earliest
	FetchMaxBytes      int32
	FetchMaxWait       int32
}

// DefaultConsumerGroupConfig returns sensible defaults.
func DefaultConsumerGroupConfig() ConsumerGroupConfig {
	return ConsumerGroupConfig{
		SessionTimeout:     30 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		AutoCommit:         true,
		AutoCommitInterval: 5 * time.Second,
		InitialOffset:      -1,
		FetchMaxBytes:      1 << 20,
		FetchMaxWait:       500,
	}
}
