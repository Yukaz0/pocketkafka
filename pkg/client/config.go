package client

import "time"

type ProducerConfig struct {
	Acks         int16
	Timeout      time.Duration
	BatchSize    int
	LingerMs     time.Duration
	MaxRetries   int
	RetryBackoff time.Duration

	Idempotent      bool
	TransactionalID string
}

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

type ConsumerGroupConfig struct {
	GroupID            string
	SessionTimeout     time.Duration
	HeartbeatInterval  time.Duration
	AutoCommit         bool
	AutoCommitInterval time.Duration
	InitialOffset      int64
	FetchMaxBytes      int32
	FetchMaxWait       int32
}

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
