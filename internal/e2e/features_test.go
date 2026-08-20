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
