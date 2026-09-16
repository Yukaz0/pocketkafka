package protocol

import "testing"

// FuzzDecodeProduceRequest exercises the produce request decoder (which reads
// nested topic/partition arrays and an opaque record blob) with hostile input.
func FuzzDecodeProduceRequest(f *testing.F) {
	req := &ProduceRequest{Version: 3, Acks: 1, Timeout: 1000,
		Topics: []ProduceRequestTopic{{Topic: "t", Partitions: []ProduceRequestPartition{{Partition: 0, Records: []byte{1, 2, 3}}}}}}
	if body, err := EncodeProduceRequest(req); err == nil {
		f.Add(int16(3), body)
	}
	f.Add(int16(0), []byte{})
	f.Fuzz(func(t *testing.T, version int16, body []byte) {
		if version < 0 || version > 9 {
			return
		}
		_, _ = DecodeProduceRequest(version, body)
	})
}

// FuzzDecodeMetadataRequest covers the metadata decoder across versions.
func FuzzDecodeMetadataRequest(f *testing.F) {
	if body, err := EncodeMetadataRequest(&MetadataRequest{Version: 8}); err == nil {
		f.Add(int16(8), body)
	}
	f.Add(int16(0), []byte{})
	f.Fuzz(func(t *testing.T, version int16, body []byte) {
		if version < 0 || version > 9 {
			return
		}
		_, _ = DecodeMetadataRequest(version, body)
	})
}

// FuzzDecodeGroupRequests walks the group-coordinator request decoders, which
// share a lot of array/string parsing logic.
func FuzzDecodeGroupRequests(f *testing.F) {
	f.Add(int16(5), []byte{0x00, 0x01, 'g'})
	f.Add(int16(0), []byte{})
	f.Fuzz(func(t *testing.T, version int16, body []byte) {
		if version < 0 || version > 9 {
			return
		}
		_, _ = DecodeJoinGroupRequest(version, body)
		_, _ = DecodeSyncGroupRequest(version, body)
		_, _ = DecodeHeartbeatRequest(version, body)
		_, _ = DecodeLeaveGroupRequest(version, body)
		_, _ = DecodeOffsetCommitRequest(version, body)
		_, _ = DecodeOffsetFetchRequest(version, body)
	})
}

// FuzzDecodeFetchAndListOffsets covers the read-path decoders.
func FuzzDecodeFetchAndListOffsets(f *testing.F) {
	f.Add(int16(5), []byte{})
	f.Fuzz(func(t *testing.T, version int16, body []byte) {
		if version < 0 || version > 9 {
			return
		}
		_, _ = DecodeFetchRequest(version, body)
		_, _ = DecodeListOffsetsRequest(version, body)
	})
}

// FuzzDecodeTransactionRequests covers the transactional decoders even though
// the broker rejects those APIs, because they can still be reached directly.
func FuzzDecodeTransactionRequests(f *testing.F) {
	f.Add(int16(1), []byte{})
	f.Fuzz(func(t *testing.T, version int16, body []byte) {
		if version < 0 || version > 4 {
			return
		}
		_, _ = DecodeAddPartitionsToTxnRequest(version, body)
		_, _ = DecodeAddOffsetsToTxnRequest(version, body)
		_, _ = DecodeEndTxnRequest(version, body)
	})
}
