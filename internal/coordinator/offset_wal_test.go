package coordinator

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestWALReplayRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	meta := "m"
	recs := []walRecord{
		{Type: walRecordCommit, Group: "g1", Topic: "t", Partition: 0, Offset: 5, LeaderEpoch: 1, Metadata: &meta, Timestamp: 100},
		{Type: walRecordCommit, Group: "g1", Topic: "t", Partition: 1, Offset: 9, Timestamp: 200},
	}
	for _, r := range recs {
		if err := w.append(r); err != nil {
			t.Fatal(err)
		}
	}

	var got []walRecord
	if err := w.replay(func(r walRecord) { got = append(got, r) }); err != nil {
		t.Fatalf("replay: %v", err)
	}
	w.close()

	if len(got) != 2 {
		t.Fatalf("replayed %d records, want 2", len(got))
	}
	if got[0].Group != "g1" || got[0].Offset != 5 || got[0].Metadata == nil || *got[0].Metadata != "m" {
		t.Fatalf("record 0 = %+v", got[0])
	}
	if got[1].Partition != 1 || got[1].Offset != 9 || got[1].Metadata != nil {
		t.Fatalf("record 1 = %+v", got[1])
	}
}

func TestWALTruncatedTailIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(walRecord{Type: walRecordCommit, Group: "g", Topic: "t", Partition: 0, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.append(walRecord{Type: walRecordCommit, Group: "g", Topic: "t", Partition: 1, Offset: 2}); err != nil {
		t.Fatal(err)
	}
	w.close()

	// Simulate a crash mid-append: chop the last record in half.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-3], 0o600); err != nil {
		t.Fatal(err)
	}

	w2, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.close()
	var got []walRecord
	if err := w2.replay(func(r walRecord) { got = append(got, r) }); err != nil {
		t.Fatalf("replay of truncated tail returned error: %v", err)
	}
	if len(got) != 1 || got[0].Partition != 0 {
		t.Fatalf("replayed = %+v, want only the first intact record", got)
	}
}

func TestWALChecksumMismatchIsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offsets.wal")
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(walRecord{Type: walRecordCommit, Group: "g", Topic: "t", Partition: 0, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	w.close()

	// Flip a payload byte in place (leave the length and checksum intact).
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[8] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	w2, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.close()
	if err := w2.replay(func(walRecord) {}); err == nil {
		t.Fatal("corrupt checksum did not produce an error")
	}
}

func TestOffsetStoreCompactionPreservesLatest(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	// Commit more than the compaction threshold so a snapshot is unavoidable.
	for i := 0; i < walCompactThreshold+10; i++ {
		if err := s.Commit("g", "t", 0, &CommittedOffset{Offset: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	off, ok := reopened.Fetch("g", "t", 0)
	if !ok || off.Offset != int64(walCompactThreshold+9) {
		t.Fatalf("offset after compaction = %+v ok=%v, want %d", off, ok, walCompactThreshold+9)
	}
}

func TestOffsetStoreWALReplayAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewOffsetStore(BackendFile, dir)
	if err := s.Commit("g", "t", 0, &CommittedOffset{Offset: 42}); err != nil {
		t.Fatal(err)
	}
	// Do NOT compact/close cleanly: reopen must replay the WAL.
	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if off, ok := reopened.Fetch("g", "t", 0); !ok || off.Offset != 42 {
		t.Fatalf("WAL replay lost the commit: %+v ok=%v", off, ok)
	}
}

func TestOffsetStoreConcurrentCommits(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			group := "g" + strconv.Itoa(i%3)
			for j := 0; j < 40; j++ {
				_ = s.Commit(group, "t", int32(j), &CommittedOffset{Offset: int64(j)})
				s.Fetch(group, "t", int32(j))
			}
		}(i)
	}
	wg.Wait()
}

func TestOffsetStoreRetention(t *testing.T) {
	now := time.Now()
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// One fresh offset and one old offset.
	if err := s.Commit("fresh", "t", 0, &CommittedOffset{Offset: 1, UpdatedAt: now.UnixNano()}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("stale", "t", 0, &CommittedOffset{Offset: 2, UpdatedAt: now.Add(-48 * time.Hour).UnixNano()}); err != nil {
		t.Fatal(err)
	}

	removed := s.PruneOlderThan(now.Add(-24 * time.Hour))
	if removed != 1 {
		t.Fatalf("pruned %d offsets, want 1", removed)
	}
	if _, ok := s.Fetch("stale", "t", 0); ok {
		t.Fatal("stale offset survived retention")
	}
	if off, ok := s.Fetch("fresh", "t", 0); !ok || off.Offset != 1 {
		t.Fatal("fresh offset was pruned")
	}

	// Retention result must persist across a restart.
	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok := reopened.Fetch("stale", "t", 0); ok {
		t.Fatal("pruned offset came back after restart")
	}
	if _, ok := reopened.Fetch("fresh", "t", 0); !ok {
		t.Fatal("fresh offset lost after restart")
	}
}

func TestPruneOffsetsZeroTimeIsNoop(t *testing.T) {
	s, _ := NewOffsetStore(BackendInMemory, t.TempDir())
	_ = s.Commit("g", "t", 0, &CommittedOffset{Offset: 1})
	if n := s.PruneOlderThan(time.Time{}); n != 0 {
		t.Fatalf("zero cutoff pruned %d, want 0", n)
	}
	if _, ok := s.Fetch("g", "t", 0); !ok {
		t.Fatal("zero cutoff removed an offset")
	}
}

func TestGroupManagerPruneOffsets(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewOffsetStore(BackendFile, dir)
	defer s.Close()
	gm := NewGroupManager(s, 0, "h", 1, 1000)

	now := time.Now()
	_ = s.Commit("old", "t", 0, &CommittedOffset{Offset: 1, UpdatedAt: now.Add(-time.Hour).UnixNano()})
	_ = s.Commit("new", "t", 0, &CommittedOffset{Offset: 2, UpdatedAt: now.UnixNano()})

	if n := gm.PruneOffsets(now, 10*time.Minute); n != 1 {
		t.Fatalf("PruneOffsets removed %d, want 1", n)
	}
	if n := gm.PruneOffsets(now, 0); n != 0 {
		t.Fatalf("zero retention pruned %d, want 0", n)
	}
}
