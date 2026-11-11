package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurabilityReportsUnsyncedTail(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "facts", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if durable, risk, _ := p.Durability(); durable != 0 || risk != 0 {
		t.Fatalf("fresh partition durable=%d risk=%d, want 0/0", durable, risk)
	}
	if _, err := p.Append(makeBatch("hello world")); err != nil {
		t.Fatal(err)
	}
	durable, risk, syncMs := p.Durability()
	if durable != 0 {
		t.Fatalf("durable offset = %d before any sync, want 0", durable)
	}
	if risk <= 0 {
		t.Fatalf("bytes at risk = %d after an un-synced append, want > 0", risk)
	}
	if syncMs != 0 {
		t.Fatalf("last sync = %d before any sync, want 0", syncMs)
	}

	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	durable, risk, syncMs = p.Durability()
	if risk != 0 {
		t.Fatalf("bytes at risk = %d after sync, want 0", risk)
	}
	if durable != p.LogEndOffset() {
		t.Fatalf("durable offset = %d, want log end %d", durable, p.LogEndOffset())
	}
	if syncMs == 0 {
		t.Fatal("sync timestamp was not recorded")
	}
}

func TestRetentionDebtMatchesTheNextPass(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "debt", 0, 256, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	payload := makeBatch(strings.Repeat("x", 100))
	for i := 0; i < 40; i++ {
		if _, err := p.Append(payload); err != nil {
			t.Fatal(err)
		}
	}

	before := p.SizeBytes()
	ret := Retention{RetentionBytes: before / 2}
	debt := p.RetentionDebt(ret)
	if debt <= 0 {
		t.Fatalf("retention debt = %d with a half-size cap over %d bytes on disk", debt, before)
	}
	p.enforceRetention(ret)
	if got := before - p.SizeBytes(); got != debt {
		t.Fatalf("retention deleted %d bytes but the reported debt was %d", got, debt)
	}
	if len(p.SegmentInfos()) == 0 {
		t.Fatal("retention deleted every segment")
	}
	if again := p.RetentionDebt(ret); again != 0 {
		t.Fatalf("debt after a pass = %d, want 0", again)
	}
}

func TestRetentionDebtCountsAgedSegments(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(dir, "aged", 0, 256, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	payload := makeBatch(strings.Repeat("y", 100))
	for i := 0; i < 40; i++ {
		if _, err := p.Append(payload); err != nil {
			t.Fatal(err)
		}
	}

	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(logs) < 3 {
		t.Fatalf("segments on disk = %v, want several", logs)
	}
	old := time.Now().Add(-2 * time.Hour)
	closed := logs[:len(logs)-1]
	for _, f := range closed {
		if err := os.Chtimes(f, old, old); err != nil {
			t.Fatal(err)
		}
	}

	prev := Retention{RetentionTime: time.Hour}
	debt := p.RetentionDebt(prev)

	infos := p.SegmentInfos()
	var wantDebt int64
	for _, info := range infos[:len(infos)-1] {
		wantDebt += info.SizeBytes
	}
	if debt != wantDebt {
		t.Fatalf("debt = %d, want the closed segments' %d bytes", debt, wantDebt)
	}
	before := p.SizeBytes()
	p.enforceRetention(prev)
	if got := before - p.SizeBytes(); got != debt {
		t.Fatalf("age policy deleted %d bytes but the reported debt was %d", got, debt)
	}

	if again := p.RetentionDebt(prev); again != 0 {
		t.Fatalf("debt after the age pass = %d, want 0", again)
	}
}

func TestCompactionDebtTracksBytesSinceTheLastCompaction(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "compacted", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	for i := 0; i < 20; i++ {
		if _, err := p.Append(makeBatch("value")); err != nil {
			t.Fatal(err)
		}
	}
	debt, lastMs := p.CompactionDebt()
	if debt != p.SizeBytes() || lastMs != 0 {
		t.Fatalf("debt = %d lastCompact = %d, want %d and 0 before any compaction", debt, lastMs, p.SizeBytes())
	}

	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	if debt, lastMs = p.CompactionDebt(); debt != 0 || lastMs == 0 {
		t.Fatalf("debt = %d lastCompact = %d, want 0 and a timestamp right after compacting", debt, lastMs)
	}
	for i := 0; i < 5; i++ {
		if _, err := p.Append(makeBatch("value")); err != nil {
			t.Fatal(err)
		}
	}
	after := p.SizeBytes()
	if debt, _ = p.CompactionDebt(); debt <= 0 || debt > after {
		t.Fatalf("debt = %d after 5 appends over %d bytes, want (0, %d]", debt, after, after)
	}
}
