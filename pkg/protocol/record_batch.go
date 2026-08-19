package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// crc32cTable is the Castagnoli CRC-32C table used by Kafka's record format v2.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// CalculateRecordBatchCRC computes the Castagnoli CRC over the batch bytes
// that follow the CRC field (i.e. from Attributes to the end of the batch).
func CalculateRecordBatchCRC(batchDataAfterCRC []byte) uint32 {
	return crc32.Checksum(batchDataAfterCRC, crc32cTable)
}

// Record is a single Kafka record inside a RecordBatch.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int32
	Key            []byte
	Value          []byte
	Headers        []RecordHeader
}

// RecordHeader is a key/value header attached to a record.
type RecordHeader struct {
	Key   string
	Value []byte
}

// RecordBatch is a serialized Kafka record batch in record format v2.
type RecordBatch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}

// EncodeRecord appends a single record's wire form to dst and returns the
// updated slice.
func EncodeRecord(dst []byte, rec Record) []byte {
	body := make([]byte, 0, 16+len(rec.Key)+len(rec.Value))
	body = append(body, byte(rec.Attributes))
	body = appendVarint(body, rec.TimestampDelta)
	body = appendVarint(body, int64(rec.OffsetDelta))
	body = appendVarint(body, int64(len(rec.Key))) // key length, -1 if null
	if rec.Key != nil {
		body = append(body, rec.Key...)
	}
	if rec.Value == nil {
		body = appendVarint(body, -1)
	} else {
		body = appendVarint(body, int64(len(rec.Value)))
		body = append(body, rec.Value...)
	}
	body = appendUvarint(body, uint64(len(rec.Headers)))
	for _, h := range rec.Headers {
		body = appendVarint(body, int64(len(h.Key)))
		body = append(body, h.Key...)
		if h.Value == nil {
			body = appendVarint(body, -1)
		} else {
			body = appendVarint(body, int64(len(h.Value)))
			body = append(body, h.Value...)
		}
	}
	dst = appendVarint(dst, int64(len(body)))
	return append(dst, body...)
}

// EncodeRecordBatch serializes a RecordBatch to its full wire form, computing
// the CRC32C checksum and populating LastOffsetDelta.
func EncodeRecordBatch(b *RecordBatch) ([]byte, error) {
	b.Magic = RecordMagic
	if b.LastOffsetDelta == 0 && len(b.Records) > 0 {
		b.LastOffsetDelta = int32(len(b.Records) - 1)
	}
	records := make([]byte, 0, 64)
	for _, rec := range b.Records {
		records = EncodeRecord(records, rec)
	}

	// Total batch length covers everything from PartitionLeaderEpoch onward.
	length := int32(4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4) // 49
	length += int32(len(records))

	out := make([]byte, 0, 12+int(length))
	out = binary.BigEndian.AppendUint64(out, uint64(b.BaseOffset))
	out = binary.BigEndian.AppendUint32(out, uint32(length))
	out = binary.BigEndian.AppendUint32(out, uint32(b.PartitionLeaderEpoch))
	out = append(out, byte(b.Magic))
	out = binary.BigEndian.AppendUint32(out, 0) // CRC placeholder
	out = binary.BigEndian.AppendUint16(out, uint16(b.Attributes))
	out = binary.BigEndian.AppendUint32(out, uint32(b.LastOffsetDelta))
	out = binary.BigEndian.AppendUint64(out, uint64(b.BaseTimestamp))
	out = binary.BigEndian.AppendUint64(out, uint64(b.MaxTimestamp))
	out = binary.BigEndian.AppendUint64(out, uint64(b.ProducerID))
	out = binary.BigEndian.AppendUint16(out, uint16(b.ProducerEpoch))
	out = binary.BigEndian.AppendUint32(out, uint32(b.BaseSequence))
	out = binary.BigEndian.AppendUint32(out, uint32(len(b.Records)))
	out = append(out, records...)

	crc := CalculateRecordBatchCRC(out[21:])
	binary.BigEndian.PutUint32(out[17:21], crc)
	b.CRC = crc
	return out, nil
}

// DecodeRecordBatch parses a raw RecordBatch v2 byte slice into a struct.
func DecodeRecordBatch(raw []byte) (*RecordBatch, error) {
	if len(raw) < 61 {
		return nil, fmt.Errorf("record batch too short: %d bytes", len(raw))
	}
	b := &RecordBatch{
		BaseOffset:           int64(binary.BigEndian.Uint64(raw[0:8])),
		PartitionLeaderEpoch: int32(binary.BigEndian.Uint32(raw[12:16])),
		Magic:                int8(raw[16]),
		CRC:                  binary.BigEndian.Uint32(raw[17:21]),
		Attributes:           int16(binary.BigEndian.Uint16(raw[21:23])),
		LastOffsetDelta:      int32(binary.BigEndian.Uint32(raw[23:27])),
		BaseTimestamp:        int64(binary.BigEndian.Uint64(raw[27:35])),
		MaxTimestamp:         int64(binary.BigEndian.Uint64(raw[35:43])),
		ProducerID:           int64(binary.BigEndian.Uint64(raw[43:51])),
		ProducerEpoch:        int16(binary.BigEndian.Uint16(raw[51:53])),
		BaseSequence:         int32(binary.BigEndian.Uint32(raw[53:57])),
	}
	if b.Magic != RecordMagic {
		return nil, fmt.Errorf("unsupported record batch magic %d", b.Magic)
	}
	count := int32(binary.BigEndian.Uint32(raw[57:61]))
	if count == 0 {
		count = b.LastOffsetDelta + 1
	}
	off := 61
	for i := int32(0); i < count && off < len(raw); i++ {
		rec, n, err := decodeRecord(raw[off:])
		if err != nil {
			return nil, err
		}
		b.Records = append(b.Records, rec)
		off += n
	}
	return b, nil
}

func decodeRecord(src []byte) (Record, int, error) {
	length, n := binary.Varint(src)
	if n <= 0 {
		return Record{}, 0, fmt.Errorf("invalid record length varint")
	}
	if int(length) < 0 || n+int(length) > len(src) {
		return Record{}, 0, fmt.Errorf("invalid record length %d", length)
	}
	body := src[n : n+int(length)]
	r := NewReader(body)
	var rec Record
	var err error
	if rec.Attributes, err = r.ReadInt8(); err != nil {
		return rec, 0, err
	}
	if rec.TimestampDelta, err = r.ReadVarint(); err != nil {
		return rec, 0, err
	}
	var od int64
	if od, err = r.ReadVarint(); err != nil {
		return rec, 0, err
	}
	rec.OffsetDelta = int32(od)
	if rec.Key, err = r.ReadBytesVarint(); err != nil {
		return rec, 0, err
	}
	if rec.Value, err = r.ReadBytesVarint(); err != nil {
		return rec, 0, err
	}
	hc, err := r.ReadUVarint()
	if err != nil {
		return rec, 0, err
	}
	for i := uint64(0); i < hc; i++ {
		var h RecordHeader
		if h.Key, err = r.ReadVarintString(); err != nil {
			return rec, 0, err
		}
		if h.Value, err = r.ReadBytesVarint(); err != nil {
			return rec, 0, err
		}
		rec.Headers = append(rec.Headers, h)
	}
	return rec, n + int(length), nil
}

// ReadBytesVarint reads a length-prefixed byte array where the length is a
// signed varint (as used inside records).
func (r *Reader) ReadBytesVarint() ([]byte, error) {
	l, err := r.ReadVarint()
	if err != nil {
		return nil, err
	}
	if l < 0 {
		return nil, nil
	}
	if r.off+int(l) > len(r.buf) {
		return nil, fmt.Errorf("bytes varint overrun")
	}
	out := make([]byte, l)
	copy(out, r.buf[r.off:r.off+int(l)])
	r.off += int(l)
	return out, nil
}

// ReadVarintString reads a varint-length-prefixed string.
func (r *Reader) ReadVarintString() (string, error) {
	b, err := r.ReadBytesVarint()
	if err != nil {
		return "", err
	}
	return string(b), nil
}
