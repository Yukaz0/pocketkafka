package protocol

import (
	"bytes"
	"math/rand"
	"testing"
)

// compressRoundTrip compresses records, decompresses them, and asserts equality.
func compressRoundTrip(t *testing.T, codec int16, data []byte) {
	t.Helper()
	compressed, attr, err := CompressRecordBatch(data, codec, 0)
	if err != nil {
		t.Fatalf("codec %d compress: %v", codec, err)
	}
	decompressed, err := DecompressRecordBatch(compressed, attr)
	if err != nil {
		t.Fatalf("codec %d decompress: %v", codec, err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatalf("codec %d round-trip mismatch: got %d bytes want %d", codec, len(decompressed), len(data))
	}
}

func TestCompressionRoundTrip(t *testing.T) {
	// Small (literal-only) payload.
	small := []byte("hello kafka compression")
	// Larger payload that exercises matches.
	big := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 200)
	// Random payload (mostly literals).
	rnd := make([]byte, 8192)
	rand.New(rand.NewSource(42)).Read(rnd)

	for _, codec := range []int16{CompressionGzip, CompressionSnappy, CompressionLZ4} {
		compressRoundTrip(t, codec, small)
		compressRoundTrip(t, codec, big)
		compressRoundTrip(t, codec, rnd)
	}
}

func TestCompressionNone(t *testing.T) {
	data := []byte("plain")
	out, err := DecompressRecordBatch(data, 0)
	if err != nil || !bytes.Equal(out, data) {
		t.Fatalf("codec 0 should pass through: %v", err)
	}
}

func TestCompressionZSTDUnsupported(t *testing.T) {
	_, err := DecompressRecordBatch([]byte("x"), CompressionZSTD)
	if err == nil {
		t.Fatal("expected unsupported zstd error")
	}
}

func TestCompressionCodecExtract(t *testing.T) {
	if got := CompressionCodec(0x00); got != 0 {
		t.Fatalf("expected 0 got %d", got)
	}
	if got := CompressionCodec(0x02); got != CompressionSnappy {
		t.Fatalf("expected snappy got %d", got)
	}
	// Attribute bits 3+ are reserved and must not leak into the codec.
	if got := CompressionCodec(0x08); got != 0 {
		t.Fatalf("expected 0 for reserved bits got %d", got)
	}
}
