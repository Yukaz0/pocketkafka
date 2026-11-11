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

func TestOffsetStoreCompactionDoesNotLoseCommits(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	const goroutines, per = 6, 400
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range per {
				if err := s.Commit("g", "t", int32(g*1000+i), &CommittedOffset{Offset: int64(i)}); err != nil {
					t.Errorf("Commit: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	s.compactWG.Wait() // in-flight async compaction would race the reopen

	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	missing := 0
	for g := range goroutines {
		for i := range per {
			if _, ok := reopened.Fetch("g", "t", int32(g*1000+i)); !ok {
				missing++
			}
		}
	}
	if missing != 0 {
		t.Fatalf("%d of %d commits lost across compaction+crash", missing, goroutines*per)
	}
}

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

func TestOffsetStoreWALOrderMatchesMemory(t *testing.T) {
	dir := t.TempDir()
	s, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := range 8 { // 8*100 < walCompactThreshold: replay is WAL-only
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 100 {
				_ = s.Commit("g", "t", 0, &CommittedOffset{Offset: int64(g*100 + i)})
			}
		}(g)
	}
	wg.Wait()
	want, _ := s.Fetch("g", "t", 0)
	reopened, err := NewOffsetStore(BackendFile, dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reopened.Fetch("g", "t", 0)
	if got.Offset != want.Offset {
		t.Fatalf("replayed offset %d, memory %d", got.Offset, want.Offset)
	}
}
