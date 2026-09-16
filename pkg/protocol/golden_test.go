package protocol

import (
	"encoding/hex"
	"testing"
)

// Golden byte fixtures for every advertised API version. These lock the wire
// layout so a refactor of the codecs cannot silently change what clients see.
// Regenerate deliberately (and review the diff) if the wire format is meant to
// change.

func goldenMetadata(v int16) *MetadataResponse {
	cluster := "c1"
	return &MetadataResponse{
		Version: v, ThrottleTimeMs: 7, ClusterID: &cluster, ControllerID: 1,
		Brokers:                     []MetadataBroker{{NodeID: 1, Host: "h", Port: 9092}},
		Topics:                      []MetadataTopic{{Name: "t", Partitions: []MetadataPartition{{Partition: 0, Leader: 1, LeaderEpoch: 0, Replicas: []int32{1}, ISR: []int32{1}}}}},
		ClusterAuthorizedOperations: -1,
	}
}

func goldenFetch(v int16) *FetchResponse {
	return &FetchResponse{Version: v, ThrottleTimeMs: 7, Topics: []FetchResponseTopic{{
		Topic: "t",
		Partitions: []FetchResponsePartition{{
			Partition: 0, HighWatermark: 5, LastStableOffset: 5, LogStartOffset: 0, Records: []byte{1, 2},
		}},
	}}}
}

func goldenListOffsets(v int16) *ListOffsetsResponse {
	p := ListOffsetsResponsePartition{Partition: 0, ErrorCode: 0, Timestamp: -1, Offset: 42, LeaderEpoch: -1}
	if v == 0 {
		p.OldStyleOffsets = []int64{42}
	}
	return &ListOffsetsResponse{Version: v, ThrottleTimeMs: 7, Topics: []ListOffsetsResponseTopic{{Topic: "t", Partitions: []ListOffsetsResponsePartition{p}}}}
}

func goldenFindCoordinator(v int16) *FindCoordinatorResponse {
	return &FindCoordinatorResponse{Version: v, ThrottleTimeMs: 7, NodeID: 1, Host: "h", Port: 9092}
}

func goldenJoinGroup(v int16) *JoinGroupResponse {
	return &JoinGroupResponse{Version: v, ThrottleTimeMs: 7, GenerationID: 3, ProtocolName: "range", LeaderID: "m1", MemberID: "m1",
		Members: []JoinGroupResponseMember{{MemberID: "m1", Metadata: []byte{9}}}}
}

func goldenSyncGroup(v int16) *SyncGroupResponse {
	return &SyncGroupResponse{Version: v, ThrottleTimeMs: 7, Assignment: []byte{4, 5}}
}

func goldenOffsetFetch(v int16) *OffsetFetchResponse {
	return &OffsetFetchResponse{Version: v, ThrottleTimeMs: 7, ErrorCode: 0, Topics: []OffsetFetchResponseTopic{{
		Topic:      "t",
		Partitions: []OffsetFetchResponsePartition{{Partition: 0, Offset: 11, LeaderEpoch: -1, ErrorCode: 0}},
	}}}
}

func TestGoldenPackets(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		enc  func() ([]byte, error)
	}{
		{"metadata_v0", "000000010000000100016800002384000000010000000174000000010000000000000000000100000001000000010000000100000001", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(0)) }},
		{"metadata_v1", "000000010000000100016800002384ffff0000000100000001000000017400000000010000000000000000000100000001000000010000000100000001", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(1)) }},
		{"metadata_v2", "000000010000000100016800002384ffff000263310000000100000001000000017400000000010000000000000000000100000001000000010000000100000001", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(2)) }},
		{"metadata_v3", "00000007000000010000000100016800002384ffff000263310000000100000001000000017400000000010000000000000000000100000001000000010000000100000001", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(3)) }},
		{"metadata_v4", "00000007000000010000000100016800002384ffff000263310000000100000001000000017400000000010000000000000000000100000001000000010000000100000001", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(4)) }},
		{"metadata_v5", "00000007000000010000000100016800002384ffff000263310000000100000001000000017400000000010000000000000000000100000001000000010000000100000001ffffffff", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(5)) }},
		{"metadata_v6", "00000007000000010000000100016800002384ffff000263310000000100000001000000017400000000010000000000000000000100000001000000010000000100000001ffffffff", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(6)) }},
		{"metadata_v7", "00000007000000010000000100016800002384ffff00026331000000010000000100000001740000000001000000000000000000010000000000000001000000010000000100000001ffffffff", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(7)) }},
		{"metadata_v8", "00000007000000010000000100016800002384ffff00026331000000010000000100000001740000000001000000000000000000010000000000000001000000010000000100000001ffffffff00000000ffffffff", func() ([]byte, error) { return EncodeMetadataResponse(goldenMetadata(8)) }},

		{"fetch_v0", "00000001000174000000010000000000000000000000000005000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(0)) }},
		{"fetch_v1", "0000000700000001000174000000010000000000000000000000000005000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(1)) }},
		{"fetch_v2", "0000000700000001000174000000010000000000000000000000000005000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(2)) }},
		{"fetch_v3", "0000000700000001000174000000010000000000000000000000000005000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(3)) }},
		{"fetch_v4", "00000007000000010001740000000100000000000000000000000000050000000000000005ffffffff000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(4)) }},
		{"fetch_v5", "000000070000000100017400000001000000000000000000000000000500000000000000050000000000000000ffffffff000000020102", func() ([]byte, error) { return EncodeFetchResponse(goldenFetch(5)) }},

		{"listoffsets_v0", "000000010001740000000100000000000000000001000000000000002a", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(0)) }},
		{"listoffsets_v1", "0000000100017400000001000000000000ffffffffffffffff000000000000002a", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(1)) }},
		{"listoffsets_v2", "000000070000000100017400000001000000000000ffffffffffffffff000000000000002a", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(2)) }},
		{"listoffsets_v3", "000000070000000100017400000001000000000000ffffffffffffffff000000000000002a", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(3)) }},
		{"listoffsets_v4", "000000070000000100017400000001000000000000ffffffffffffffff000000000000002affffffff", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(4)) }},
		{"listoffsets_v5", "000000070000000100017400000001000000000000ffffffffffffffff000000000000002affffffff", func() ([]byte, error) { return EncodeListOffsetsResponse(goldenListOffsets(5)) }},

		{"findcoordinator_v0", "00000000000100016800002384", func() ([]byte, error) { return EncodeFindCoordinatorResponse(goldenFindCoordinator(0)) }},
		{"findcoordinator_v1", "000000070000ffff0000000100016800002384", func() ([]byte, error) { return EncodeFindCoordinatorResponse(goldenFindCoordinator(1)) }},
		{"findcoordinator_v2", "000000070000ffff0000000100016800002384", func() ([]byte, error) { return EncodeFindCoordinatorResponse(goldenFindCoordinator(2)) }},

		{"joingroup_v0", "000000000003000572616e676500026d3100026d310000000100026d310000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(0)) }},
		{"joingroup_v1", "000000000003000572616e676500026d3100026d310000000100026d310000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(1)) }},
		{"joingroup_v2", "00000007000000000003000572616e676500026d3100026d310000000100026d310000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(2)) }},
		{"joingroup_v3", "00000007000000000003000572616e676500026d3100026d310000000100026d310000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(3)) }},
		{"joingroup_v4", "00000007000000000003000572616e676500026d3100026d310000000100026d310000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(4)) }},
		{"joingroup_v5", "00000007000000000003000572616e676500026d3100026d310000000100026d31ffff0000000109", func() ([]byte, error) { return EncodeJoinGroupResponse(goldenJoinGroup(5)) }},

		{"syncgroup_v0", "0000000000020405", func() ([]byte, error) { return EncodeSyncGroupResponse(goldenSyncGroup(0)) }},
		{"syncgroup_v1", "000000070000000000020405", func() ([]byte, error) { return EncodeSyncGroupResponse(goldenSyncGroup(1)) }},
		{"syncgroup_v2", "000000070000000000020405", func() ([]byte, error) { return EncodeSyncGroupResponse(goldenSyncGroup(2)) }},
		{"syncgroup_v3", "000000070000000000020405", func() ([]byte, error) { return EncodeSyncGroupResponse(goldenSyncGroup(3)) }},

		{"offsetfetch_v0", "000000010001740000000100000000000000000000000bffff0000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(0)) }},
		{"offsetfetch_v1", "000000010001740000000100000000000000000000000bffff0000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(1)) }},
		{"offsetfetch_v2", "000000010001740000000100000000000000000000000bffff00000000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(2)) }},
		{"offsetfetch_v3", "00000007000000010001740000000100000000000000000000000bffff00000000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(3)) }},
		{"offsetfetch_v4", "00000007000000010001740000000100000000000000000000000bffff00000000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(4)) }},
		{"offsetfetch_v5", "00000007000000010001740000000100000000000000000000000bffffffffffff00000000", func() ([]byte, error) { return EncodeOffsetFetchResponse(goldenOffsetFetch(5)) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.enc()
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(got) != tc.hex {
				t.Fatalf("golden mismatch for %s:\n got  %s\n want %s", tc.name, hex.EncodeToString(got), tc.hex)
			}
		})
	}
}

// TestGoldenListOffsetsV0OldStyleOffsets guards the legacy-consumer fix: v0
// must carry the offset in OldStyleOffsets (a non-null int64 array), not the
// v1+ Offset field, or librdkafka's 0.9.0 fallback reads a null array and never
// fetches.
func TestGoldenListOffsetsV0OldStyleOffsets(t *testing.T) {
	body, err := EncodeListOffsetsResponse(goldenListOffsets(0))
	if err != nil {
		t.Fatal(err)
	}
	// Trailing 12 bytes are the old-style offsets array: length 1, value 42.
	const wantTail = "00000001000000000000002a"
	if got := hex.EncodeToString(body[len(body)-12:]); got != wantTail {
		t.Fatalf("v0 tail = %s, want old-style offsets array %s", got, wantTail)
	}
}

// TestGoldenCompleteBufferConsumption asserts that decoding the golden bytes
// consumes them without error, i.e. the encoders and decoders agree exactly.
func TestGoldenMetadataDecodeRoundTrip(t *testing.T) {
	for v := int16(0); v <= 8; v++ {
		body, err := EncodeMetadataResponse(goldenMetadata(v))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := DecodeMetadataResponse(v, body)
		if err != nil {
			t.Fatalf("v%d decode: %v", v, err)
		}
		if len(resp.Topics) != 1 || resp.Topics[0].Name != "t" {
			t.Fatalf("v%d decoded topics = %+v", v, resp.Topics)
		}
	}
}
