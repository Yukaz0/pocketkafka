// franz-go interoperability check: metadata, create topic, produce, consumer
// group consume with offset commit, and a second group member resuming from
// the committed offset.
//
// Usage: franzgo <host:port>
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic = "interop-franzgo"
	group = "interop-franzgo-group"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: franzgo <host:port>")
		os.Exit(1)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("franz-go OK")
}

func run(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("interop-franzgo"))
	if err != nil {
		return err
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	md, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return fmt.Errorf("metadata: %w", err)
	}
	fmt.Println("brokers:", len(md.Brokers))
	if _, err := adm.CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	recs := []*kgo.Record{
		{Topic: topic, Key: []byte("k1"), Value: []byte("v1")},
		{Topic: topic, Key: []byte("k2"), Value: []byte("v2")},
		{Topic: topic, Key: []byte("k3"), Value: []byte("v3")},
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	fmt.Println("produced", len(recs), "messages")

	newGroupClient := func() (*kgo.Client, error) {
		return kgo.NewClient(
			kgo.SeedBrokers(addr),
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
		)
	}

	// Member 1 reads two records and commits them.
	c1, err := newGroupClient()
	if err != nil {
		return err
	}
	got, err := poll(ctx, c1, 2)
	if err != nil {
		c1.Close()
		return fmt.Errorf("consume: %w", err)
	}
	if err := c1.CommitRecords(ctx, got...); err != nil {
		c1.Close()
		return fmt.Errorf("commit: %w", err)
	}
	c1.Close()

	// Member 2 must resume after the committed offset and see only the third record.
	c2, err := newGroupClient()
	if err != nil {
		return err
	}
	defer c2.Close()
	rest, err := poll(ctx, c2, 1)
	if err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	if string(rest[0].Value) != "v3" {
		return fmt.Errorf("resume: expected v3 after commit, got %q", rest[0].Value)
	}
	return nil
}

// poll fetches until n records arrive or ctx expires.
func poll(ctx context.Context, cl *kgo.Client, n int) ([]*kgo.Record, error) {
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			return nil, err
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out[:n], nil
}
