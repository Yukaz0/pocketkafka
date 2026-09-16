package coordinator

import (
	"strconv"
	"sync"
	"testing"
)

func TestOffsetStoreInMemory(t *testing.T) {
	s, err := NewOffsetStore(BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	if err := s.Commit("g", "t", 0, &CommittedOffset{Offset: 42}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	off, ok := s.Fetch("g", "t", 0)
	if !ok || off.Offset != 42 {
		t.Fatalf("Fetch = %+v ok=%v, want offset 42", off, ok)
	}

	// A second store over the same directory must not see in-memory data.
	s2, err := NewOffsetStore(BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Fetch("g", "t", 0); ok {
		t.Fatal("in-memory backend persisted data across instances")
	}
}

func TestOffsetStoreFilePersists(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatalf("NewOffsetStore: %v", err)
	}
	meta := "m"
	if err := s.Commit("g", "t", 3, &CommittedOffset{Offset: 7, Metadata: &meta, LeaderEpoch: 1}); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	off, ok := reopened.Fetch("g", "t", 3)
	if !ok {
		t.Fatal("offset missing after reopen")
	}
	if off.Offset != 7 || off.LeaderEpoch != 1 || off.Metadata == nil || *off.Metadata != "m" {
		t.Fatalf("offset = %+v, want offset 7 epoch 1 meta m", off)
	}
}

func TestOffsetStoreUnknownBackend(t *testing.T) {
	if _, err := NewOffsetStore("rocksdb", t.TempDir()); err == nil {
		t.Fatal("NewOffsetStore(rocksdb) = nil error, want failure")
	}
}

// TestOffsetStoreConcurrent commits from many goroutines to exercise the
// locking under the race detector.
func TestOffsetStoreConcurrent(t *testing.T) {
	s, err := NewOffsetStore(BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			group := "g" + strconv.Itoa(i%4)
			for j := 0; j < 32; j++ {
				if err := s.Commit(group, "t", int32(j), &CommittedOffset{Offset: int64(j)}); err != nil {
					t.Errorf("Commit: %v", err)
					return
				}
				s.Fetch(group, "t", int32(j))
				s.GroupOffsets(group)
			}
		}(i)
	}
	wg.Wait()
}
