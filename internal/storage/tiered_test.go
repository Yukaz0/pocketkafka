package storage

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/tier"
)

// newMockS3 returns an in-memory S3-compatible server storing PUT/GET objects.
func newMockS3() (*httptest.Server, func() map[string][]byte) {
	var mu sync.Mutex
	objects := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		switch r.Method {
		case http.MethodPut:
			body, _ := io.ReadAll(r.Body)
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
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	snapshot := func() map[string][]byte {
		mu.Lock()
		defer mu.Unlock()
		out := make(map[string][]byte, len(objects))
		for k, v := range objects {
			out[k] = v
		}
		return out
	}
	return srv, snapshot
}

func TestTieredOffloadAndRestore(t *testing.T) {
	srv, snapshot := newMockS3()
	defer srv.Close()

	dir := t.TempDir()
	store, err := NewStore(dir, 512, 1) // tiny segments to force rolling
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateTopic("t", 1); err != nil {
		t.Fatal(err)
	}
	p := store.GetPartition("t", 0)
	for i := 0; i < 40; i++ {
		if _, err := p.Append(makeBatch("val")); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	client := tier.NewS3Client(srv.URL, "test-bucket", "ak", "sk", "us-east-1", "")
	store.EnableTiering(client, 0, time.Hour)
	store.offloadAll()

	// Verify remote stubs exist and .log files were removed for offloaded segs.
	entries, _ := os.ReadDir(p.Dir())
	remote, logs := 0, 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".remote") {
			remote++
		}
		if strings.HasSuffix(e.Name(), ".log") {
			logs++
		}
	}
	if remote == 0 {
		t.Fatal("no remote stubs written")
	}
	if logs == 0 {
		t.Fatal("active segment .log should remain")
	}
	if n := len(snapshot()); n == 0 {
		t.Fatal("no objects uploaded to mock S3")
	}

	// Simulate a restart: close and reopen, re-enable tiering.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store2, err := NewStore(dir, 512, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	store2.EnableTiering(tier.NewS3Client(srv.URL, "test-bucket", "ak", "sk", "us-east-1", ""), 0, time.Hour)

	p2 := store2.GetPartition("t", 0)
	if p2.LogEndOffset() != 40 {
		t.Fatalf("LEO after restart: got %d want 40", p2.LogEndOffset())
	}
	raw, _, err := p2.Read(0, 1<<20)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n := countBatches(t, raw); n != 40 {
		t.Fatalf("read after restore: got %d batches want 40", n)
	}
}
