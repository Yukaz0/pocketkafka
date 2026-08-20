package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func makeBatch(values ...string) []byte {
	b := &protocol.RecordBatch{
		BaseTimestamp: 1700000000000,
		MaxTimestamp:  1700000000000,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
	}
	for i, v := range values {
		b.Records = append(b.Records, protocol.Record{OffsetDelta: int32(i), Value: []byte(v)})
	}
	raw, err := protocol.EncodeRecordBatch(b)
	if err != nil {
		panic(err)
	}
	return raw
}

// countBatches iterates over the raw concatenated batches in data.
func countBatches(t *testing.T, data []byte) int {
	n := 0
	pos := 0
	for pos < len(data) {
		h, err := ParseRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("parse batch at %d: %v", pos, err)
		}
		n++
		pos += int(BatchTotalSize(h))
	}
	return n
}

func TestPartitionAppendReadRecover(t *testing.T) {
	dir := t.TempDir()
	partDir := filepath.Join(dir, "test-topic", "0")

	p, err := OpenPartition(partDir, "test-topic", 0, 1024*1024, 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Append 3 batches, each with 2 records => offsets 0..5.
	for i := 0; i < 3; i++ {
		off, err := p.Append(makeBatch("a", "b"))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if off != int64(i*2) {
			t.Fatalf("append offset: got %d want %d", off, i*2)
		}
	}

	if p.LogEndOffset() != 6 || p.HighWatermark() != 6 {
		t.Fatalf("LEO/HWM: got %d/%d want 6/6", p.LogEndOffset(), p.HighWatermark())
	}
	if p.EarliestOffset() != 0 {
		t.Fatalf("earliest: got %d", p.EarliestOffset())
	}
	if off, _ := p.GetOffset(-1); off != 6 {
		t.Fatalf("latest offset: got %d", off)
	}
	if off, _ := p.GetOffset(-2); off != 0 {
		t.Fatalf("earliest offset: got %d", off)
	}

	// Read from 0: all 3 batches.
	data, hwm, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if hwm != 6 {
		t.Fatalf("read hwm: got %d", hwm)
	}
	if n := countBatches(t, data); n != 3 {
		t.Fatalf("read from 0: got %d batches want 3", n)
	}

	// Read from offset 3: batches 1 and 2 only.
	data2, _, err := p.Read(3, 1<<20)
	if err != nil {
		t.Fatalf("read offset 3: %v", err)
	}
	if n := countBatches(t, data2); n != 2 {
		t.Fatalf("read from 3: got %d batches want 2", n)
	}

	// Read beyond LEO returns empty.
	data3, hwm3, _ := p.Read(100, 1<<20)
	if len(data3) != 0 || hwm3 != 6 {
		t.Fatalf("read beyond LEO: len=%d hwm=%d", len(data3), hwm3)
	}

	p.Close()

	// Reopen and verify LEO/HWM recovered.
	p2, err := OpenPartition(partDir, "test-topic", 0, 1024*1024, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer p2.Close()
	if p2.LogEndOffset() != 6 {
		t.Fatalf("recovered LEO: got %d want 6", p2.LogEndOffset())
	}
	rd, _, _ := p2.Read(0, 1<<20)
	if n := countBatches(t, rd); n != 3 {
		t.Fatalf("recovered read: got %d batches", n)
	}
}

func TestPartitionSegmentRolling(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(filepath.Join(dir, "t", "0"), "t", 0, 512, 1) // tiny segments
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Write enough batches to force rolling into multiple segments.
	for i := 0; i < 50; i++ {
		if _, err := p.Append(makeBatch("hello")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	// All data must still be readable across segments.
	data, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n := countBatches(t, data); n != 50 {
		t.Fatalf("rolling read: got %d batches want 50", n)
	}

	entries, _ := os.ReadDir(filepath.Join(dir, "t", "0"))
	logs := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logs++
		}
	}
	if logs < 2 {
		t.Fatalf("expected multiple segments, found %d .log files", logs)
	}
}
