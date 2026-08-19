package protocol

import (
	"bytes"
	"testing"
)

func TestRecordBatchEncodeDecodeRoundTrip(t *testing.T) {
	b := &RecordBatch{
		BaseTimestamp: 1700000000000,
		MaxTimestamp:  1700000001000,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records: []Record{
			{OffsetDelta: 0, Key: []byte("k1"), Value: []byte("value one"), Headers: []RecordHeader{{Key: "h1", Value: []byte("hv1")}}},
			{OffsetDelta: 1, Key: nil, Value: []byte("value two")},
			{OffsetDelta: 2, Key: []byte(""), Value: nil},
		},
	}

	raw, err := EncodeRecordBatch(b)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Verify CRC is valid and covers everything after the CRC field.
	crc := CalculateRecordBatchCRC(raw[21:])
	if crc != b.CRC {
		t.Fatalf("crc mismatch: got %d want %d", crc, b.CRC)
	}

	dec, err := DecodeRecordBatch(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.Magic != RecordMagic {
		t.Fatalf("magic: got %d", dec.Magic)
	}
	if dec.LastOffsetDelta != 2 {
		t.Fatalf("lastOffsetDelta: got %d", dec.LastOffsetDelta)
	}
	if len(dec.Records) != 3 {
		t.Fatalf("records: got %d want 3", len(dec.Records))
	}
	if !bytes.Equal(dec.Records[0].Key, []byte("k1")) || !bytes.Equal(dec.Records[0].Value, []byte("value one")) {
		t.Fatalf("record 0 mismatch: %+v", dec.Records[0])
	}
	if len(dec.Records[0].Headers) != 1 || dec.Records[0].Headers[0].Key != "h1" {
		t.Fatalf("headers mismatch: %+v", dec.Records[0].Headers)
	}
	if len(dec.Records[1].Key) != 0 {
		t.Fatalf("record 1 key should be empty")
	}
	if len(dec.Records[2].Value) != 0 {
		t.Fatalf("record 2 value should be empty")
	}
}

func TestEncodeRecordBatchSetsOffsets(t *testing.T) {
	b := &RecordBatch{
		BaseOffset: 100,
		Records:    []Record{{Key: []byte("a"), Value: []byte("b")}},
	}
	raw, err := EncodeRecordBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := int64(bigEndianUint64(raw[0:8])); got != 100 {
		t.Fatalf("base offset: got %d want 100", got)
	}
}

func TestReaderWriterVarints(t *testing.T) {
	w := NewWriter(0)
	w.WriteVarint(-1)
	w.WriteVarint(0)
	w.WriteVarint(64)
	w.WriteUVarint(300)
	r := NewReader(w.Bytes())
	v1, _ := r.ReadVarint()
	v2, _ := r.ReadVarint()
	v3, _ := r.ReadVarint()
	v4, _ := r.ReadUVarint()
	if v1 != -1 || v2 != 0 || v3 != 64 || v4 != 300 {
		t.Fatalf("varints mismatch: %d %d %d %d", v1, v2, v3, v4)
	}
}

func bigEndianUint64(b []byte) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}
