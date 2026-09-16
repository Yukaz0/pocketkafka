package handler

import (
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// newDenyAllHandler builds a handler whose authorizer denies everything, plus
// the underlying store so tests can assert nothing was mutated.
func newDenyAllHandler(t *testing.T) (*Handler, *storage.Store) {
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
	h := New(store, gm, &cfg, 0, "localhost", 9092)
	h.WithAuthorizer(authz.NewInMemory()) // empty store => default-deny
	return h, store
}

func TestProduceDeniedLeavesLogUntouched(t *testing.T) {
	h, store := newDenyAllHandler(t)
	store.EnsureTopic("orders", 1)

	now := int64(0)
	batch, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte("v")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &protocol.ProduceRequest{Version: 3, Acks: 1, Timeout: 5000,
		Topics: []protocol.ProduceRequestTopic{{Topic: "orders",
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
	if got := resp.Topics[0].Partitions[0].ErrorCode; got != protocol.ErrTopicAuthorizationFailed {
		t.Fatalf("produce error = %d, want ErrTopicAuthorizationFailed", got)
	}
	if leo := store.GetPartition("orders", 0).LogEndOffset(); leo != 0 {
		t.Fatalf("log end offset = %d, want 0 (denied produce must not write)", leo)
	}
}

func TestFetchDenied(t *testing.T) {
	h, store := newDenyAllHandler(t)
	store.EnsureTopic("orders", 1)

	req := &protocol.FetchRequest{Version: 5, MaxWaitMillis: 0, MinBytes: 0, MaxBytes: 1024,
		Topics: []protocol.FetchRequestTopic{{Topic: "orders",
			Partitions: []protocol.FetchRequestPartition{{Partition: 0, FetchOffset: 0, PartitionMaxBytes: 1024}}}}}
	body, _ := protocol.EncodeFetchRequest(req)

	respBody, err := h.Handle(protocol.APKFetch, 5, body, RequestContext{Principal: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.DecodeFetchResponse(5, respBody)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Topics[0].Partitions[0].ErrorCode; got != protocol.ErrTopicAuthorizationFailed {
		t.Fatalf("fetch error = %d, want ErrTopicAuthorizationFailed", got)
	}
}

func TestCreateTopicDenied(t *testing.T) {
	h, store := newDenyAllHandler(t)

	req := &protocol.CreateTopicsRequest{Version: 4,
		Topics: []protocol.CreateTopicsRequestTopic{{Topic: "new-topic", NumPartitions: 1, ReplicationFactor: 1}}}
	body, _ := protocol.EncodeCreateTopicsRequest(req)

	respBody, err := h.Handle(protocol.APKCreateTopics, 4, body, RequestContext{Principal: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.DecodeCreateTopicsResponse(4, respBody)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Topics[0].ErrorCode; got != protocol.ErrTopicAuthorizationFailed {
		t.Fatalf("create topic error = %d, want ErrTopicAuthorizationFailed", got)
	}
	if store.GetTopic("new-topic") != nil {
		t.Fatal("denied create-topics mutated the store")
	}
}

func TestJoinGroupDenied(t *testing.T) {
	h, _ := newDenyAllHandler(t)

	req := &protocol.JoinGroupRequest{Version: 5, Group: "g1", SessionTimeoutMs: 10000,
		RebalanceTimeoutMs: 30000, ProtocolType: "consumer"}
	body, _ := protocol.EncodeJoinGroupRequest(req)

	respBody, err := h.Handle(protocol.APKJoinGroup, 5, body, RequestContext{Principal: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.DecodeJoinGroupResponse(5, respBody)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != protocol.ErrGroupAuthorizationFailed {
		t.Fatalf("join group error = %d, want ErrGroupAuthorizationFailed", resp.ErrorCode)
	}
}

// TestAllowAllWhenSecurityDisabled confirms the historical allow-all behaviour
// is preserved for the default handler.
func TestAllowAllWhenSecurityDisabled(t *testing.T) {
	cfg := config.Default()
	store, err := storage.NewStore(t.TempDir(), cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	offsets, _ := coordinator.NewOffsetStore(coordinator.BackendInMemory, t.TempDir())
	gm := coordinator.NewGroupManager(offsets, 0, "localhost", 9092, int32(cfg.Coordinator.SessionTimeoutMs))
	h := New(store, gm, &cfg, 0, "localhost", 9092)

	now := int64(0)
	batch, _ := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte("v")}},
	})
	req := &protocol.ProduceRequest{Version: 3, Acks: 1, Timeout: 5000,
		Topics: []protocol.ProduceRequestTopic{{Topic: "auto", Partitions: []protocol.ProduceRequestPartition{{Partition: 0, Records: batch}}}}}
	body, _ := protocol.EncodeProduceRequest(req)

	respBody, err := h.Handle(protocol.APKProduce, 3, body, RequestContext{})
	if err != nil {
		t.Fatal(err)
	}
	resp, _ := protocol.DecodeProduceResponse(3, respBody)
	if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		t.Fatalf("allow-all produce denied: %d", resp.Topics[0].Partitions[0].ErrorCode)
	}
}
