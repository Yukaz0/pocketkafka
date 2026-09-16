package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

func testBatch(t *testing.T, value string) []byte {
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

func TestAppendSyncedWritesToDisk(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(filepath.Join(dir, "0"), "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	off, err := p.AppendSynced(testBatch(t, "durable"), true)
	if err != nil {
		t.Fatalf("AppendSynced: %v", err)
	}
	if off != 0 {
		t.Fatalf("offset = %d, want 0", off)
	}

	// The bytes must already be visible on disk after a synced append.
	entries, err := os.ReadDir(filepath.Join(dir, "0"))
	if err != nil {
		t.Fatal(err)
	}
	var dataLen int64
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".log" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		dataLen += info.Size()
	}
	if dataLen == 0 {
		t.Fatal("no log bytes written after synced append")
	}
}

func TestAppendSyncedSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenPartition(filepath.Join(dir, "0"), "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.AppendSynced(testBatch(t, "v1"), true); err != nil {
		t.Fatal(err)
	}
	if _, err := p.AppendSynced(testBatch(t, "v2"), true); err != nil {
		t.Fatal(err)
	}
	p.Close()

	reopened, err := OpenPartition(filepath.Join(dir, "0"), "t", 0, 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if leo := reopened.LogEndOffset(); leo != 2 {
		t.Fatalf("recovered LEO = %d, want 2", leo)
	}
}

func TestSyncAllAndStartFlush(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("t", 0)
	if _, err := p.Append(testBatch(t, "x")); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncAll(); err != nil {
		t.Fatalf("SyncAll: %v", err)
	}

	// The interval flusher must start and stop without leaking or blocking.
	stop := store.StartFlush(5 * time.Millisecond)
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("StartFlush stop did not return")
	}
}

// TestStartFlushDisabled checks a non-positive interval yields a usable no-op stop.
func TestStartFlushDisabled(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	stop := store.StartFlush(0)
	stop() // must not panic
	stop() // idempotent
}
