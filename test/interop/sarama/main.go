// Sarama interoperability check: metadata, create topic, produce, consumer
// group consume with offset commit, and a second group member resuming from
// the committed offset.
//
// Usage: sarama <host:port>
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic = "interop-sarama"
	group = "interop-sarama-group"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: sarama <host:port>")
		os.Exit(1)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("sarama OK")
}

func config() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = "interop-sarama"
	// Sarama does not negotiate via ApiVersions; V0_11 maps to Produce v3 / Fetch v4,
	// the lowest versions the broker advertises (Produce max is 3).
	cfg.Version = sarama.V0_11_0_0
	cfg.Producer.Return.Successes = true
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	return cfg
}

func run(addr string) error {
	cfg := config()
	brokers := []string{addr}

	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return fmt.Errorf("admin: %w", err)
	}
	defer admin.Close()
	brokersMD, _, err := admin.DescribeCluster()
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	fmt.Println("brokers:", len(brokersMD))
	if err := admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	prod, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	for _, v := range []string{"v1", "v2", "v3"} {
		if _, _, err := prod.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(v)}); err != nil {
			prod.Close()
			return fmt.Errorf("produce: %w", err)
		}
	}
	prod.Close()
	fmt.Println("produced 3 messages")

	// Member 1 reads and commits two records.
	got, err := consume(brokers, cfg, 2)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	if got[0] != "v1" || got[1] != "v2" {
		return fmt.Errorf("consume: expected v1,v2 got %v", got)
	}
	// Member 2 must resume after the committed offset and see only the third record.
	rest, err := consume(brokers, cfg, 1)
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if rest[0] != "v3" {
		return fmt.Errorf("resume: expected v3 after commit, got %q", rest[0])
	}
	return nil
}

// consume joins the group, reads n messages, commits them, and leaves.
func consume(brokers []string, cfg *sarama.Config, n int) ([]string, error) {
	cg, err := sarama.NewConsumerGroup(brokers, group, cfg)
	if err != nil {
		return nil, err
	}
	defer cg.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	h := &handler{n: n, done: make(chan struct{})}
	errc := make(chan error, 1)
	go func() { errc <- cg.Consume(ctx, []string{topic}, h) }()

	select {
	case <-h.done:
		cancel()
		<-errc
		return h.got, nil
	case err := <-errc:
		if err == nil {
			err = fmt.Errorf("session ended before %d messages", n)
		}
		return nil, err
	case <-ctx.Done():
		return nil, fmt.Errorf("timeout, got %v", h.got)
	}
}

type handler struct {
	n    int
	got  []string
	done chan struct{}
}

func (*handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for m := range c.Messages() {
		h.got = append(h.got, string(m.Value))
		s.MarkMessage(m, "")
		if len(h.got) == h.n {
			s.Commit()
			close(h.done)
			return nil
		}
	}
	return nil
}
