package storage

import (
	"encoding/binary"
	"fmt"
)

// RecordBatchHeader holds the fields of a Kafka RecordBatch v2 header that the
// storage engine needs to assign offsets and advance the Log End Offset.
type RecordBatchHeader struct {
	BaseOffset      int64
	Length          int32 // number of bytes after the Length field
	LastOffsetDelta int32
	Count           int32
}

const (
	batchHeaderSize = 61
)

// ParseRecordBatchHeader decodes the leading fixed portion of a RecordBatch
// (Record format v2, magic == 2). It returns the parsed header together with
// the total size in bytes of the batch (including the 12-byte prefix that holds
// BaseOffset and Length).
//
// Layout of the raw batch bytes:
//
//	[ 0: 8] BaseOffset          int64
//	[ 8:12] Length              int32
//	[12:16] PartitionLeaderEpoch int32
//	[16:17] Magic               int8
//	[17:21] CRC32C              int32
//	[21:23] Attributes          int16
//	[23:27] LastOffsetDelta     int32
//	[27:35] BaseTimestamp       int64
//	[35:43] MaxTimestamp        int64
//	[43:51] ProducerID          int64
//	[51:53] ProducerEpoch       int16
//	[53:57] BaseSequence        int32
//	[57:61] RecordsCount        int32
func ParseRecordBatchHeader(raw []byte) (RecordBatchHeader, error) {
	if len(raw) < batchHeaderSize {
		return RecordBatchHeader{}, fmt.Errorf("record batch too short: %d bytes", len(raw))
	}
	h := RecordBatchHeader{
		BaseOffset: int64(binary.BigEndian.Uint64(raw[0:8])),
		Length:     int32(binary.BigEndian.Uint32(raw[8:12])),
	}
	magic := int8(raw[16])
	if magic != 2 {
		return h, fmt.Errorf("unsupported record batch magic %d (expected 2)", magic)
	}
	h.LastOffsetDelta = int32(binary.BigEndian.Uint32(raw[23:27]))
	h.Count = int32(binary.BigEndian.Uint32(raw[57:61]))
	return h, nil
}

// BatchTotalSize returns the total number of bytes a batch occupies in the log
// given its parsed header. It is the 12 byte prefix plus the Length field.
func BatchTotalSize(h RecordBatchHeader) int64 {
	return 12 + int64(h.Length)
}
