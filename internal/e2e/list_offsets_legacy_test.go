package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// TestListOffsetsLegacyVersions: klien legacy (librdkafka
// broker.version.fallback=0.9.0) mengirim ListOffsets v0 dan membaca hasil
// lewat OldStyleOffsets. Bug lama: handler selalu mengisi Offset (field v1+)
// dan membiarkan OldStyleOffsets = array null (-1), sehingga konsumen legacy
// menyimpulkan offset END dan tidak pernah fetch — consumer dev yang terhubung
// ke broker ini tampak diam tanpa error.
func TestListOffsetsLegacyVersions(t *testing.T) {
	kc, stop := startBroker(t)
	defer stop()

	topic := fmt.Sprintf("legacy-%d", time.Now().UnixNano())
	if err := kc.CreateTopic(topic, 1); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UnixMilli()
	batch, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now, ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte("m1")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	preq := &protocol.ProduceRequest{Version: 2, Acks: 1, Timeout: 5000,
		Topics: []protocol.ProduceRequestTopic{{Topic: topic,
			Partitions: []protocol.ProduceRequestPartition{{Partition: 0, Records: batch}}}}}
	pbody, err := protocol.EncodeProduceRequest(preq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kcRawRoundTrip(kc, protocol.APKProduce, 2, pbody); err != nil {
		t.Fatal(err)
	}

	for _, v := range []int16{0, 1, 2, 4, 5} {
		req := &protocol.ListOffsetsRequest{
			Version:        v,
			ReplicaID:      -1,
			IsolationLevel: 0,
			Topics: []protocol.ListOffsetsRequestTopic{{
				Topic: topic,
				Partitions: []protocol.ListOffsetsRequestPartition{{
					Partition: 0, Timestamp: -1, MaxNumOffsets: 1,
				}},
			}},
		}
		body, err := protocol.EncodeListOffsetsRequest(req)
		if err != nil {
			t.Fatalf("encode v%d: %v", v, err)
		}
		raw, err := kcRawRoundTrip(kc, protocol.APKListOffsets, v, body)
		if err != nil {
			t.Fatalf("roundtrip v%d: %v", v, err)
		}
		resp, err := protocol.DecodeListOffsetsResponse(v, raw)
		if err != nil {
			t.Fatalf("decode v%d: %v", v, err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("v%d: bentuk respons salah: %+v", v, resp)
		}
		p := resp.Topics[0].Partitions[0]
		if p.ErrorCode != protocol.ErrNone {
			t.Fatalf("v%d: error code %d", v, p.ErrorCode)
		}
		if v == 0 {
			if len(p.OldStyleOffsets) != 1 {
				t.Fatalf("v0: OldStyleOffsets kosong (null) — klien legacy tidak bisa fetch; got %+v", p)
			}
			if p.OldStyleOffsets[0] != 1 {
				t.Fatalf("v0: OldStyleOffsets[0] = %d, want 1", p.OldStyleOffsets[0])
			}
		} else if p.Offset != 1 {
			t.Fatalf("v%d: Offset = %d, want 1", v, p.Offset)
		}
	}
}
