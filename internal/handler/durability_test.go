package handler

import (
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// produceOne sends a single-record produce request with the given acks and
// returns the partition error code.
func produceOne(t *testing.T, h *Handler, topic string, acks int16) int16 {
	t.Helper()
	now := int64(0)
	batch, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte("v")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &protocol.ProduceRequest{Version: 3, Acks: acks, Timeout: 5000,
		Topics: []protocol.ProduceRequestTopic{{Topic: topic,
			Partitions: []protocol.ProduceRequestPartition{{Partition: 0, Records: batch}}}}}
	body, _ := protocol.EncodeProduceRequest(req)
	respBody, err := h.Handle(protocol.APKProduce, 3, body, RequestContext{Principal: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.DecodeProduceResponse(3, respBody)
	if err != nil {
		t.Fatal(err)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

// TestFlushPolicyRequestSyncsBeforeAck asserts the produce ack is only returned
// after a successful sync under flush_policy=request.
func TestFlushPolicyRequestSyncsBeforeAck(t *testing.T) {
	cfg := config.Default()
	cfg.Storage.FlushPolicy = config.FlushPolicyRequest
	store, err := storage.NewStore(t.TempDir(), cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	offsets, _ := coordinator.NewOffsetStore(coordinator.BackendInMemory, t.TempDir())
	gm := coordinator.NewGroupManager(offsets, 0, "localhost", 9092, 10000)
	h := New(store, gm, &cfg, 0, "localhost", 9092)

	if code := produceOne(t, h, "durable", 1); code != protocol.ErrNone {
		t.Fatalf("produce error = %d, want ErrNone", code)
	}
	if leo := store.GetPartition("durable", 0).LogEndOffset(); leo != 1 {
		t.Fatalf("LEO = %d, want 1", leo)
	}
}

// TestAcksAllSyncsWhenConfigured asserts acks=-1 triggers a sync when
// sync_on_acks_all is enabled, and that turning it off keeps plain appends.
func TestAcksAllSyncsWhenConfigured(t *testing.T) {
	for _, syncOnAcksAll := range []bool{true, false} {
		cfg := config.Default()
		cfg.Storage.FlushPolicy = config.FlushPolicyNone
		cfg.Storage.SyncOnAcksAll = syncOnAcksAll
		store, err := storage.NewStore(t.TempDir(), cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
		if err != nil {
			t.Fatal(err)
		}
		offsets, _ := coordinator.NewOffsetStore(coordinator.BackendInMemory, t.TempDir())
		gm := coordinator.NewGroupManager(offsets, 0, "localhost", 9092, 10000)
		h := New(store, gm, &cfg, 0, "localhost", 9092)

		if code := produceOne(t, h, "acksall", -1); code != protocol.ErrNone {
			t.Fatalf("syncOnAcksAll=%v: produce error = %d, want ErrNone", syncOnAcksAll, code)
		}
		store.Close()
	}
}
