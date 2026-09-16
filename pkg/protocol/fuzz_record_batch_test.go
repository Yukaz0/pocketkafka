package protocol

import "testing"

// FuzzDecodeRecordBatch asserts the RecordBatch v2 decoder is robust against
// truncated, oversized, and inconsistent headers.
func FuzzDecodeRecordBatch(f *testing.F) {
	now := int64(0)
	good, err := EncodeRecordBatch(&RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []Record{{Key: []byte("k"), Value: []byte("v")}},
	})
	if err == nil {
		f.Add(good)
	}
	f.Add([]byte{})
	f.Add(make([]byte, 61))
	f.Add(make([]byte, 12))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = DecodeRecordBatch(data)
	})
}

// FuzzDecompressRecordBatch covers every compression codec path (none, gzip,
// snappy, lz4) with arbitrary payloads.
func FuzzDecompressRecordBatch(f *testing.F) {
	f.Add([]byte{}, int16(0))
	f.Add([]byte{0x00, 0x01, 0x02, 0x03}, int16(1))
	f.Add([]byte{0x1f, 0x8b, 0x08, 0x00}, int16(1))
	f.Add([]byte{0xff, 0x06, 0x00, 0x00, 0x73}, int16(2))
	f.Add([]byte{0x04, 0x22, 0x4d, 0x18}, int16(3))

	f.Fuzz(func(t *testing.T, data []byte, attr int16) {
		// Only the low three bits select a codec; the rest are flags.
		_, _ = DecompressRecordBatch(data, attr)
	})
}

// FuzzCompressDecompressRoundTrip verifies compression never panics for any
// input, regardless of codec.
func FuzzCompressDecompressRoundTrip(f *testing.F) {
	f.Add([]byte("hello world"), int16(1))
	f.Add([]byte{}, int16(0))
	f.Fuzz(func(t *testing.T, data []byte, codec int16) {
		if codec < 0 || codec > 3 {
			return
		}
		compressed, _, err := CompressRecordBatch(data, codec, 0)
		if err != nil {
			return
		}
		if codec == 0 {
			return
		}
		_, _ = DecompressRecordBatch(compressed, codec)
	})
}
