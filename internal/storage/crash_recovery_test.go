package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func crashBatch(t *testing.T, value string) []byte {
	t.Helper()
	now := time.Now().UnixMilli()
	raw, err := protocol.EncodeRecordBatch(&protocol.RecordBatch{
		BaseTimestamp: now, MaxTimestamp: now,
		ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1,
		Records: []protocol.Record{{Value: []byte(value)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// logPath returns the single .log file inside a partition dir.
func logPath(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			return filepath.Join(dir, e.Name())
		}
	}
	t.Fatalf("no .log file in %s", dir)
	return ""
}

// TestCrashPartialRecordBatch truncates the last batch mid-write and verifies
// recovery drops only the partial tail and keeps earlier offsets.
func TestCrashPartialRecordBatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "0")
	p, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := p.Append(crashBatch(t, fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	lp := logPath(t, dir)
	st, _ := os.Stat(lp)
	if err := os.Truncate(lp, st.Size()-5); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer reopened.Close()
	if leo := reopened.LogEndOffset(); leo != 2 {
		t.Fatalf("LEO after partial-batch crash = %d, want 2", leo)
	}
}

// TestCrashMissingIndex rebuilds a deleted index from the log on restart.
func TestCrashMissingIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "0")
	p, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := p.Append(crashBatch(t, "v")); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	lp := logPath(t, dir)
	if err := os.Remove(lp[:len(lp)-4] + ".index"); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer reopened.Close()
	if leo := reopened.LogEndOffset(); leo != 5 {
		t.Fatalf("LEO after missing index = %d, want 5", leo)
	}
}

// TestCrashLogAheadOfIndex recreates a stale index (older than the log) and
// verifies recovery still yields the correct LEO.
func TestCrashLogAheadOfIndex(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "0")
	p, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, err := p.Append(crashBatch(t, "v")); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	// Truncate the index to zero: the log is now "ahead" of the index.
	lp := logPath(t, dir)
	ip := lp[:len(lp)-4] + ".index"
	if err := os.Truncate(ip, 0); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenPartition(dir, "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer reopened.Close()
	if leo := reopened.LogEndOffset(); leo != 4 {
		t.Fatalf("LEO after stale index = %d, want 4", leo)
	}
}

// TestCrashDuringSegmentRoll leaves multiple segments behind (as a roll would)
// and verifies the whole range is readable after restart.
func TestCrashDuringSegmentRoll(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "0")
	// A tiny segment size forces a roll every batch or two.
	p, err := OpenPartition(dir, "t", 0, 120, 32)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	for i := 0; i < n; i++ {
		if _, err := p.Append(crashBatch(t, fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()

	reopened, err := OpenPartition(dir, "t", 0, 120, 32)
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer reopened.Close()
	if leo := reopened.LogEndOffset(); leo != n {
		t.Fatalf("LEO after segment roll = %d, want %d", leo, n)
	}
	data, hwm, err := reopened.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if hwm != n {
		t.Fatalf("HWM = %d, want %d", hwm, n)
	}
	count := 0
	pos := 0
	for pos < len(data) {
		h, err := ParseRecordBatchHeader(data[pos:])
		if err != nil {
			t.Fatalf("parse batch at %d: %v", pos, err)
		}
		total := int(BatchTotalSize(h))
		b, err := protocol.DecodeRecordBatch(data[pos : pos+total])
		if err != nil {
			t.Fatal(err)
		}
		count += len(b.Records)
		pos += total
	}
	if count != n {
		t.Fatalf("read back %d records, want %d", count, n)
	}
}
