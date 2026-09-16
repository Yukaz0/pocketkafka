package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func benchBatch(b *testing.B, records int) []byte {
	b.Helper()
	now := int64(0)
	rb := &RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
	}
	for i := 0; i < records; i++ {
		rb.Records = append(rb.Records, Record{
			OffsetDelta: int32(i),
			Key:         []byte("key"),
			Value:       []byte("value-payload"),
		})
	}
	raw, err := EncodeRecordBatch(rb)
	if err != nil {
		b.Fatal(err)
	}
	return raw
}

// BenchmarkEncodeRecordBatch measures decoding then re-encoding a 100-record batch.
func BenchmarkEncodeRecordBatch(b *testing.B) {
	raw := benchBatch(b, 100)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb, err := DecodeRecordBatch(raw)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := EncodeRecordBatch(rb); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeRecordBatch measures decoding a 100-record batch.
func BenchmarkDecodeRecordBatch(b *testing.B) {
	raw := benchBatch(b, 100)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeRecordBatch(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReadRequestFrame measures parsing a small request frame.
func BenchmarkReadRequestFrame(b *testing.B) {
	body := make([]byte, 0, 16)
	body = binary.BigEndian.AppendUint16(body, uint16(APKApiVersions))
	body = binary.BigEndian.AppendUint16(body, 0)
	body = binary.BigEndian.AppendUint32(body, 1)
	body = binary.BigEndian.AppendUint16(body, 4)
	body = append(body, "test"...)

	frame := make([]byte, 0, len(body)+4)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(body)))
	frame = append(frame, body...)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ReadRequestFrame(bytes.NewReader(frame), 1<<20); err != nil {
			b.Fatal(err)
		}
	}
}
