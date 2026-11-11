package protocol

import "testing"

// librdkafka rejects a -1 length for the non-nullable replica arrays, so a
// partition with nil slices must round-trip as empty arrays.
func TestMetadataResponseNilReplicaArraysAreEmpty(t *testing.T) {
	resp := &MetadataResponse{
		Version: 8,
		Brokers: []MetadataBroker{{NodeID: 0, Host: "127.0.0.1", Port: 9092}},
		Topics: []MetadataTopic{{
			Name:       "t",
			Partitions: []MetadataPartition{{Partition: 0, Leader: 0}},
		}},
	}
	body, err := EncodeMetadataResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeMetadataResponse(8, body)
	if err != nil {
		t.Fatal(err)
	}
	p := got.Topics[0].Partitions[0]
	if p.Replicas == nil || p.ISR == nil || p.OfflineReplicas == nil {
		t.Fatalf("nil array decoded: %+v", p)
	}
	if len(p.Replicas)+len(p.ISR)+len(p.OfflineReplicas) != 0 {
		t.Fatalf("want empty arrays, got %+v", p)
	}
}
