package handler

import (
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func newFetchHandler(t *testing.T) (*Handler, *storage.Store) {
	t.Helper()
	cfg := config.Default()
	store, err := storage.NewStore(t.TempDir(), cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	offsets, err := coordinator.NewOffsetStore(coordinator.BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(offsets, 0, "localhost", 9092, int32(cfg.Coordinator.SessionTimeoutMs))
	return New(store, gm, &cfg, 0, "localhost", 9092), store
}

func fetchAll(topic string, partitions int, fetchOffset int64, minBytes, maxWait int32) []byte {
	parts := make([]protocol.FetchRequestPartition, 0, partitions)
	for i := range partitions {
		parts = append(parts, protocol.FetchRequestPartition{
			Partition:         int32(i),
			FetchOffset:       fetchOffset,
			PartitionMaxBytes: 1024,
		})
	}
	req := &protocol.FetchRequest{
		Version:       5,
		MaxWaitMillis: maxWait,
		MinBytes:      minBytes,
		MaxBytes:      1024 * 1024,
		Topics:        []protocol.FetchRequestTopic{{Topic: topic, Partitions: parts}},
	}
	body, _ := protocol.EncodeFetchRequest(req)
	return body
}

func appendBatch(t *testing.T, part *storage.Partition, value string) {
	t.Helper()
	batch, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: 0, MaxTimestamp: 0,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte(value)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Append(batch); err != nil {
		t.Fatal(err)
	}
}

func TestFetchManyIdlePartitionsWaitsOnce(t *testing.T) {
	h, store := newFetchHandler(t)
	if _, _, err := store.EnsureTopic("idle", 3); err != nil {
		t.Fatal(err)
	}
	body := fetchAll("idle", 3, 0, 1, 150)

	start := time.Now()
	respBody, err := h.Handle(protocol.APKFetch, 5, body, RequestContext{Principal: "alice"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed >= 2*150*time.Millisecond {
		t.Fatalf("fetch to 3 idle partitions took %v, want < %v (one shared wait, not N x maxWait)",
			elapsed, 2*150*time.Millisecond)
	}
	resp, err := protocol.DecodeFetchResponse(5, respBody)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 3 {
		t.Fatalf("unexpected response shape: %+v", resp.Topics)
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != protocol.ErrNone {
			t.Fatalf("partition %d error = %d, want ErrNone", p.Partition, p.ErrorCode)
		}
		if len(p.Records) != 0 {
			t.Fatalf("partition %d unexpectedly returned %d bytes", p.Partition, len(p.Records))
		}
	}
}

func TestFetchMinBytesGreaterThanOneStillBounded(t *testing.T) {
	h, store := newFetchHandler(t)
	if _, _, err := store.EnsureTopic("idle2", 3); err != nil {
		t.Fatal(err)
	}
	body := fetchAll("idle2", 3, 0, 1024, 150)

	start := time.Now()
	if _, err := h.Handle(protocol.APKFetch, 5, body, RequestContext{Principal: "alice"}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed >= 2*150*time.Millisecond {
		t.Fatalf("MinBytes>1 fetch to 3 idle partitions took %v, want < %v", elapsed, 2*150*time.Millisecond)
	}
}

func TestFetchWithDataReturnsImmediately(t *testing.T) {
	h, store := newFetchHandler(t)
	if _, _, err := store.EnsureTopic("ready", 1); err != nil {
		t.Fatal(err)
	}
	appendBatch(t, store.GetPartition("ready", 0), "v")
	body := fetchAll("ready", 1, 0, 1, 5000)

	start := time.Now()
	respBody, err := h.Handle(protocol.APKFetch, 5, body, RequestContext{Principal: "alice"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed >= 500*time.Millisecond {
		t.Fatalf("fetch with available data took %v, want immediate return", elapsed)
	}
	resp, err := protocol.DecodeFetchResponse(5, respBody)
	if err != nil {
		t.Fatal(err)
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != protocol.ErrNone {
		t.Fatalf("error = %d, want ErrNone", p.ErrorCode)
	}
	if len(p.Records) == 0 {
		t.Fatal("expected records for a partition with data")
	}
}

func TestFetchWakesOnAppend(t *testing.T) {
	h, store := newFetchHandler(t)
	if _, _, err := store.EnsureTopic("wake", 1); err != nil {
		t.Fatal(err)
	}
	body := fetchAll("wake", 1, 0, 1, 2000)

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		if _, err := h.Handle(protocol.APKFetch, 5, body, RequestContext{Principal: "alice"}); err != nil {
			done <- -1
			return
		}
		done <- time.Since(start)
	}()

	time.Sleep(30 * time.Millisecond)
	appendBatch(t, store.GetPartition("wake", 0), "v")

	select {
	case elapsed := <-done:
		if elapsed < 0 {
			t.Fatal("concurrent fetch failed")
		}
		if elapsed >= time.Second {
			t.Fatalf("fetch took %v after append, want a timely wake", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fetch did not wake on append")
	}
}
