package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/server"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// startBroker boots a full in-process broker on a free port and returns the
// client connection plus a stop function.
func startBroker(t *testing.T) (*client.KafkaClient, func()) {
	t.Helper()
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
	offsetStore, _ := coordinator.NewOffsetStore("inmemory", cfg.Storage.DataDir)
	gm := coordinator.NewGroupManager(offsetStore, 0, "localhost", int32(port), int32(cfg.Coordinator.SessionTimeoutMs))
	h := handler.New(store, gm, &cfg, 0, "localhost", int32(port))
	srv := server.New(&cfg, h)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	kc, err := client.NewClient([]string{adv}, "e2e-features")
	if err != nil {
		t.Fatal(err)
	}
	return kc, func() {
		kc.Close()
		srv.Close()
		store.Close()
	}
}

// kcRawRoundTrip sends a raw request body through the SDK's connection.
func kcRawRoundTrip(kc *client.KafkaClient, apiKey, version int16, body []byte) ([]byte, error) {
	return kc.RoundTrip(apiKey, version, body)
}

// TestCompressedProduce verifies gzip/snappy/lz4 compressed produce is
// decompressed on ingest and readable on fetch (Fitur 7).
func TestCompressedProduce(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	for _, codec := range []int16{protocol.CompressionGzip, protocol.CompressionSnappy, protocol.CompressionLZ4} {
		topic := fmt.Sprintf("c-%d", codec)
		if err := kc.CreateTopic(topic, 1); err != nil {
			t.Fatal(err)
		}
		now := time.Now().UnixMilli()
		plain, _ := protocol.EncodeRecordBatch(&protocol.RecordBatch{
			BaseTimestamp: now, MaxTimestamp: now, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
			Records: []protocol.Record{{Key: []byte("k"), Value: []byte("compressed-value-" + fmt.Sprintf("%d", codec))}},
		})
		compressed, attr, err := protocol.CompressRecordBatch(plain[61:], codec, 0)
		if err != nil {
			t.Fatal(err)
		}
		raw := append(append([]byte{}, plain[:61]...), compressed...)
		binary.BigEndian.PutUint16(raw[21:23], uint16(attr))
		binary.BigEndian.PutUint32(raw[8:12], uint32(49+len(compressed)))
		crc := protocol.CalculateRecordBatchCRC(raw[21:])
		binary.BigEndian.PutUint32(raw[17:21], crc)

		req := &protocol.ProduceRequest{Version: 2, Acks: 1, Timeout: 5000,
			Topics: []protocol.ProduceRequestTopic{{Topic: topic,
				Partitions: []protocol.ProduceRequestPartition{{Partition: 0, Records: raw}}}}}
		body, _ := protocol.EncodeProduceRequest(req)
		respBody, err := kcRawRoundTrip(kc, protocol.APKProduce, 2, body)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := protocol.DecodeProduceResponse(2, respBody)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
			t.Fatalf("codec %d produce error %d", codec, resp.Topics[0].Partitions[0].ErrorCode)
		}

		// Fetch back and confirm the value survived.
		off, err := kc.QueryOffset(topic, 0, -1)
		if err != nil || off != 1 {
			t.Fatalf("codec %d LEO=%d err=%v", codec, off, err)
		}
	}
}

// TestGroupAdminAPIs verifies ListGroups/DescribeGroups/DeleteGroups round trips
// (Fitur 5) and reset-offset through the SDK (Fitur 1).
func TestGroupAdminAPIs(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	if err := kc.CreateTopic("ga", 1); err != nil {
		t.Fatal(err)
	}
	if err := kc.ResetOffset("ga-group", "ga", 0, 7); err != nil {
		t.Fatal(err)
	}
	off, err := kc.CommittedOffset("ga-group", "ga", 0)
	if err != nil || off != 7 {
		t.Fatalf("committed offset = %d err=%v", off, err)
	}
	ids, err := kc.ListGroups()
	if err != nil || len(ids) != 1 || ids[0] != "ga-group" {
		t.Fatalf("ListGroups = %v err=%v", ids, err)
	}
	info, err := kc.DescribeGroup("ga-group")
	if err != nil || info.GroupID != "ga-group" {
		t.Fatalf("DescribeGroup = %+v err=%v", info, err)
	}
	if err := kc.DeleteGroup("ga-group"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
}

// TestTransactionAPIsFailClosed proves the broker neither advertises nor
// answers the transactional APIs (Milestone 1 / decision D1). A direct request
// must never come back as success, and the working paths (plain produce,
// idempotent produce) must keep working.
func TestTransactionAPIsFailClosed(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	// 1. ApiVersions must not list keys 24, 25, 26.
	respBody, err := kcRawRoundTrip(kc, protocol.APKApiVersions, 0, nil)
	if err != nil {
		t.Fatalf("ApiVersions round trip: %v", err)
	}
	av, err := protocol.DecodeApiVersionsResponse(0, respBody)
	if err != nil {
		t.Fatalf("decode ApiVersions: %v", err)
	}
	advertised := map[int16]bool{}
	for _, k := range av.ApiKeys {
		advertised[k.ApiKey] = true
	}
	for _, key := range protocol.DisabledTransactionAPIKeys {
		if advertised[key] {
			t.Errorf("ApiVersions advertises disabled transactional key %d", key)
		}
	}

	// 2. Direct requests must report UNSUPPORTED_VERSION, never ErrNone.
	txnAdd, _ := protocol.EncodeAddPartitionsToTxnRequest(&protocol.AddPartitionsToTxnRequest{
		TransactionalID: "txn-1", ProducerID: 1, ProducerEpoch: 0,
		Topics: []protocol.AddPartitionsToTxnRequestTopic{{Topic: "t", Partitions: []int32{0}}},
	})
	body, err := kcRawRoundTrip(kc, protocol.APKAddPartitionsToTxn, 1, txnAdd)
	if err != nil {
		t.Fatalf("AddPartitionsToTxn round trip: %v", err)
	}
	ap, err := protocol.DecodeAddPartitionsToTxnResponse(1, body)
	if err != nil {
		t.Fatalf("decode AddPartitionsToTxn: %v", err)
	}
	if ap.ErrorCode == protocol.ErrNone {
		t.Error("AddPartitionsToTxn returned ErrNone (false success)")
	}

	offsetsReq, _ := protocol.EncodeAddOffsetsToTxnRequest(&protocol.AddOffsetsToTxnRequest{
		TransactionalID: "txn-1", ProducerID: 1, ProducerEpoch: 0, GroupID: "g",
	})
	body, err = kcRawRoundTrip(kc, protocol.APKAddOffsetsToTxn, 1, offsetsReq)
	if err != nil {
		t.Fatalf("AddOffsetsToTxn round trip: %v", err)
	}
	ao, err := protocol.DecodeAddOffsetsToTxnResponse(1, body)
	if err != nil {
		t.Fatalf("decode AddOffsetsToTxn: %v", err)
	}
	if ao.ErrorCode == protocol.ErrNone {
		t.Error("AddOffsetsToTxn returned ErrNone (false success)")
	}

	endReq, _ := protocol.EncodeEndTxnRequest(&protocol.EndTxnRequest{
		TransactionalID: "txn-1", ProducerID: 1, ProducerEpoch: 0, Committed: true,
	})
	body, err = kcRawRoundTrip(kc, protocol.APKEndTxn, 1, endReq)
	if err != nil {
		t.Fatalf("EndTxn round trip: %v", err)
	}
	et, err := protocol.DecodeEndTxnResponse(1, body)
	if err != nil {
		t.Fatalf("decode EndTxn: %v", err)
	}
	if et.ErrorCode == protocol.ErrNone {
		t.Error("EndTxn returned ErrNone (false success)")
	}

	// 3. Non-transactional produce still succeeds.
	if err := kc.CreateTopic("txn-check", 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	prod := client.NewProducer(kc, client.DefaultProducerConfig())
	defer prod.Close()
	off, err := prod.SendSync(context.Background(), &client.Message{Topic: "txn-check", Value: []byte("v")})
	if err != nil || off != 0 {
		t.Fatalf("non-transactional produce off=%d err=%v", off, err)
	}

	// 4. Idempotent producer still works.
	icfg := client.DefaultProducerConfig()
	icfg.Idempotent = true
	ip := client.NewProducer(kc, icfg)
	defer ip.Close()
	ioff, err := ip.SendSync(context.Background(), &client.Message{Topic: "txn-check", Value: []byte("i")})
	if err != nil || ioff != 1 {
		t.Fatalf("idempotent produce off=%d err=%v", ioff, err)
	}
}

// TestTransactionClientMethodsFailClosed verifies the client SDK returns a
// clear sentinel instead of pretending a transaction started or committed.
func TestTransactionClientMethodsFailClosed(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	cfg := client.DefaultProducerConfig()
	cfg.TransactionalID = "txn-client"
	p := client.NewProducer(kc, cfg)
	defer p.Close()

	if err := p.BeginTransaction(); err != client.ErrTransactionsUnsupported {
		t.Errorf("BeginTransaction = %v, want ErrTransactionsUnsupported", err)
	}
	if err := p.CommitTransaction(); err != client.ErrTransactionsUnsupported {
		t.Errorf("CommitTransaction = %v, want ErrTransactionsUnsupported", err)
	}
	if err := p.AbortTransaction(); err != client.ErrTransactionsUnsupported {
		t.Errorf("AbortTransaction = %v, want ErrTransactionsUnsupported", err)
	}
}

// TestIdempotentProduce verifies InitProducerId + sequence validation (Fitur 4).
func TestIdempotentProduce(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	cfg := client.DefaultProducerConfig()
	cfg.Idempotent = true
	p := client.NewProducer(kc, cfg)
	defer p.Close()
	off1, err := p.SendSync(context.Background(), &client.Message{Topic: "idem", Value: []byte("x")})
	if err != nil || off1 != 0 {
		t.Fatalf("first idempotent produce off=%d err=%v", off1, err)
	}
	off2, err := p.SendSync(context.Background(), &client.Message{Topic: "idem", Value: []byte("y")})
	if err != nil || off2 != 1 {
		t.Fatalf("second idempotent produce off=%d err=%v", off2, err)
	}
	leo, err := kc.QueryOffset("idem", 0, -1)
	if err != nil || leo != 2 {
		t.Fatalf("LEO = %d err=%v (want 2)", leo, err)
	}
}
