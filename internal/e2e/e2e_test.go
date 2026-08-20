// Package e2e boots the broker in-process and exercises the full stack through
// the bundled client SDK (produce, consume group, admin).
package e2e

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/server"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func TestEndToEnd(t *testing.T) {
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	adv := fmt.Sprintf("localhost:%d", port)

	cfg := config.Default()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Listeners = map[string]string{"test": addr}
	cfg.AdvertisedListeners = map[string]string{"test": adv}

	store, err := storage.NewStore(cfg.Storage.DataDir, cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	offsetStore, _ := coordinator.NewOffsetStore("inmemory", cfg.Storage.DataDir)
	gm := coordinator.NewGroupManager(offsetStore, 0, "localhost", int32(port), int32(cfg.Coordinator.SessionTimeoutMs))
	h := handler.New(store, gm, &cfg, 0, "localhost", int32(port))
	srv := server.New(&cfg, h)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	kc, err := client.NewClient([]string{adv}, "e2e")
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()

	// Produce 5 messages; offsets must be contiguous from 0.
	prod := client.NewProducer(kc, client.DefaultProducerConfig())
	for i := 0; i < 5; i++ {
		off, err := prod.SendSync(context.Background(), &client.Message{Topic: "t", Value: []byte(fmt.Sprintf("v%d", i))})
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("produce offset %d want %d", off, i)
		}
	}

	// Admin: create then delete a topic.
	if err := kc.CreateTopic("admin", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := kc.DeleteTopic("admin"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	if off, _ := kc.QueryOffset("t", 0, -2); off != 0 {
		t.Fatalf("earliest offset want 0 got %d", off)
	}

	// Consume the 5 messages from the beginning via a consumer group.
	got := make(chan string, 5)
	gcfg := client.DefaultConsumerGroupConfig()
	gcfg.InitialOffset = -2
	gcfg.AutoCommit = false
	cg := client.NewConsumerGroup(kc, gcfg, func(m *client.ConsumedMessage) error {
		got <- string(m.Value)
		return nil
	})
	cg.SetGroupID("g1")
	cg.SetTopics([]string{"t"})
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go cg.Start(ctx)

	for i := 0; i < 5; i++ {
		select {
		case v := <-got:
			if v != fmt.Sprintf("v%d", i) {
				t.Fatalf("got %q want v%d", v, i)
			}
		case <-ctx.Done():
			t.Fatalf("timeout waiting for message %d", i)
		}
	}
}
