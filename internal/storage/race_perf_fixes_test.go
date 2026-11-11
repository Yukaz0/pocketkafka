package storage

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/tier"
)

func TestConcurrentReadRestoresRemoteSegment(t *testing.T) {
	srv, _ := newMockS3()
	defer srv.Close()

	dir := t.TempDir()
	store, err := NewStore(dir, 512, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("t", 0)
	for range 40 {
		if _, err := p.Append(makeBatch("val")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	store.EnableTiering(tier.NewS3Client(srv.URL, "test-bucket", "ak", "sk", "us-east-1", ""), 0, time.Hour)
	store.offloadAll()

	remote := 0
	for _, info := range p.SegmentInfos() {
		if info.Remote {
			remote++
		}
	}
	if remote == 0 {
		t.Fatal("offload left no remote segment to restore")
	}

	orig := p.remoteFetch
	p.remoteFetch = func(key string) ([]byte, error) {
		time.Sleep(50 * time.Millisecond)
		return orig(key)
	}

	var wg sync.WaitGroup
	results := make([][]byte, 2)
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _, errs[i] = p.Read(0, 1<<20)
		}(i)
	}
	wg.Wait()

	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("read %d: %v", i, errs[i])
		}
		if n := countBatches(t, results[i]); n != 40 {
			t.Fatalf("read %d returned %d batches, want 40", i, n)
		}
	}
}

func TestEnforceRetentionRemovesRemoteSegment(t *testing.T) {
	srv, snapshot := newMockS3()
	defer srv.Close()

	dir := t.TempDir()
	store, err := NewStore(dir, 512, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("t", 0)
	for range 40 {
		if _, err := p.Append(makeBatch("val")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	store.EnableTiering(tier.NewS3Client(srv.URL, "test-bucket", "ak", "sk", "us-east-1", ""), 0, time.Hour)
	store.offloadAll()

	stubs, err := filepath.Glob(filepath.Join(p.Dir(), "*.remote"))
	if err != nil || len(stubs) == 0 {
		t.Fatalf("remote stubs = %v, want some", stubs)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, f := range stubs {
		if err := os.Chtimes(f, old, old); err != nil {
			t.Fatal(err)
		}
	}
	before := snapshot()
	if len(before) == 0 {
		t.Fatal("no objects uploaded to mock S3")
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("enforceRetention panicked on a remote segment: %v", r)
			}
		}()
		p.enforceRetention(Retention{RetentionTime: time.Hour})
	}()

	for _, f := range stubs {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Fatalf("remote stub %s still present after retention", f)
		}
	}
	if after := snapshot(); len(after) >= len(before) {
		t.Fatalf("S3 objects not deleted: before=%d after=%d", len(before), len(after))
	}
}

func TestOffloadDoesNotBlockAppend(t *testing.T) {
	release := make(chan struct{})
	putStarted := make(chan struct{}, 1)
	var mu sync.Mutex
	objects := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
			select {
			case putStarted <- struct{}{}:
			default:
			}
			<-release
			mu.Lock()
			objects[key] = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			data, ok := objects[key]
			mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write(data)
		case http.MethodDelete:
			mu.Lock()
			delete(objects, key)
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	store, err := NewStore(t.TempDir(), 512, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("t", 0)
	for range 40 {
		if _, err := p.Append(makeBatch("val")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	store.EnableTiering(tier.NewS3Client(srv.URL, "test-bucket", "ak", "sk", "us-east-1", ""), 0, time.Hour)

	offloadDone := make(chan struct{})
	go func() {
		store.offloadAll()
		close(offloadDone)
	}()

	select {
	case <-putStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("offload never began uploading")
	}

	appended := make(chan error, 1)
	go func() {
		_, err := p.Append(makeBatch("while-offloading"))
		appended <- err
	}()
	select {
	case err := <-appended:
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Append blocked behind a slow S3 upload")
	}

	close(release)
	select {
	case <-offloadDone:
	case <-time.After(5 * time.Second):
		t.Fatal("offload did not finish after the upload was released")
	}
}

func TestAppendIgnoresIndexFailure(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "t", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if _, err := p.Append(makeBatch("one")); err != nil {
		t.Fatal(err)
	}

	if err := p.activeSegment.index.file.Close(); err != nil {
		t.Fatal(err)
	}
	off, err := p.Append(makeBatch("two"))
	if err != nil {
		t.Fatalf("append after index failure: %v", err)
	}
	if off != 1 {
		t.Fatalf("assigned offset = %d, want 1", off)
	}
	raw, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n := countBatches(t, raw); n != 2 {
		t.Fatalf("read %d batches, want 2", n)
	}
}

func TestDataWaiterSnapshotsChannelBeforeHWM(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "t", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	type result struct {
		hwm int64
	}
	got := make(chan result, 1)

	p.appendMu.Lock()
	go func() {
		_, hwm := p.DataWaiter()
		got <- result{hwm: hwm}
	}()
	time.Sleep(50 * time.Millisecond)

	p.mu.Lock()
	p.nextOffset = 1
	p.highWatermark = 1
	p.mu.Unlock()
	close(p.appendCh)
	p.appendCh = make(chan struct{})
	p.appendMu.Unlock()

	select {
	case r := <-got:
		if r.hwm != 1 {
			t.Fatalf("DataWaiter returned stale HWM %d; the wake would be lost", r.hwm)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DataWaiter blocked")
	}
}

func TestSyncSkipsIdlePartition(t *testing.T) {
	p, err := OpenPartition(t.TempDir(), "t", 0, 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, _, ms := p.Durability(); ms != 0 {
		t.Fatalf("idle sync recorded a timestamp %d", ms)
	}

	if _, err := p.Append(makeBatch("x")); err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	_, _, ms1 := p.Durability()
	if ms1 == 0 {
		t.Fatal("sync after an append did not run")
	}
	time.Sleep(5 * time.Millisecond)
	if err := p.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, _, ms2 := p.Durability(); ms2 != ms1 {
		t.Fatalf("idle partition was fsynced again: %d -> %d", ms1, ms2)
	}
}

func TestSegmentReadSkipsBeforeOffset(t *testing.T) {

	p, err := OpenPartition(t.TempDir(), "t", 0, 1<<20, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for range 5 {
		if _, err := p.Append(makeBatch("v")); err != nil {
			t.Fatal(err)
		}
	}

	raw, _, err := p.Read(3, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if n := countBatches(t, raw); n != 2 {
		t.Fatalf("read from offset 3 returned %d batches, want 2", n)
	}
	head, err := ParseRecordBatchHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if head.BaseOffset != 3 {
		t.Fatalf("first batch base offset = %d, want 3", head.BaseOffset)
	}

	raw1, _, err := p.Read(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if n := countBatches(t, raw1); n != 1 {
		t.Fatalf("read with maxBytes=1 returned %d batches, want 1", n)
	}
}

func TestDeleteTopicKeepsStoreResponsive(t *testing.T) {
	store, err := NewStore(t.TempDir(), 1<<20, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.CreateTopic("t", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetPartition("t", 0).Append(makeBatch("x")); err != nil {
		t.Fatal(err)
	}

	deleted := make(chan error, 1)
	go func() { deleted <- store.DeleteTopic("t") }()

	for range 200 {
		_ = store.TopicNames()
	}
	if err := <-deleted; err != nil {
		t.Fatalf("delete: %v", err)
	}
	if store.GetTopic("t") != nil {
		t.Fatal("topic still present after delete")
	}
	if _, err := os.Stat(store.partitionDir("t", 0)); !os.IsNotExist(err) {
		t.Fatalf("partition dir still present: %v", err)
	}
}
