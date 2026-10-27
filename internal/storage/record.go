package storage

import (
	"encoding/binary"
	"fmt"
)

type RecordBatchHeader struct {
	BaseOffset      int64
	Length          int32
	LastOffsetDelta int32
	Count           int32
}

const (
	batchHeaderSize = 61
)

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

func BatchTotalSize(h RecordBatchHeader) int64 {
	return 12 + int64(h.Length)
}
