package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/schemaregistry"
	"github.com/Yukaz0/pocketkafka/internal/server"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

// bootAt starts a broker over an existing data dir and returns a client plus a
// stop function. Unlike startBroker it reuses the caller's directory, so a
// second call simulates a process restart.
func bootAt(t *testing.T, dataDir string, port int) (*client.KafkaClient, func()) {
	t.Helper()
	adv := fmt.Sprintf("localhost:%d", port)

	cfg := config.Default()
	cfg.Storage.DataDir = dataDir
	cfg.Listeners = map[string]string{"test": fmt.Sprintf("127.0.0.1:%d", port)}
	cfg.AdvertisedListeners = map[string]string{"test": adv}

	store, err := storage.NewStore(dataDir, cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	offsetStore, err := coordinator.NewOffsetStore(coordinator.BackendFile, filepath.Join(dataDir, "__coordinator"))
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(offsetStore, 0, "localhost", int32(port), int32(cfg.Coordinator.SessionTimeoutMs))
	h := handler.New(store, gm, &cfg, 0, "localhost", int32(port))
	srv := server.New(&cfg, h)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	kc, err := client.NewClient([]string{adv}, "e2e-crash")
	if err != nil {
		t.Fatal(err)
	}
	return kc, func() {
		kc.Close()
		srv.Close()
		offsetStore.Close()
		store.Close()
	}
}

// TestBrokerRestartPreservesLogAndOffsets boots a broker, produces records and
// commits a consumer offset, restarts on the same data dir, and verifies both
// the log and the committed offset survived.
func TestBrokerRestartPreservesLogAndOffsets(t *testing.T) {
	dataDir := t.TempDir()

	kc, stop := bootAt(t, dataDir, freePort(t))
	if err := kc.CreateTopic("restart", 1); err != nil {
		t.Fatal(err)
	}
	prod := client.NewProducer(kc, client.DefaultProducerConfig())
	const n = 5
	for i := 0; i < n; i++ {
		if _, err := prod.SendSync(context.Background(), &client.Message{Topic: "restart", Value: []byte("v")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := kc.ResetOffset("g-restart", "restart", 0, 3); err != nil {
		t.Fatal(err)
	}
	stop()

	kc2, stop2 := bootAt(t, dataDir, freePort(t))
	defer stop2()

	if leo, err := kc2.QueryOffset("restart", 0, -1); err != nil || leo != n {
		t.Fatalf("LEO after restart = %d err=%v, want %d", leo, err, n)
	}
	if off, err := kc2.CommittedOffset("g-restart", "restart", 0); err != nil || off != 3 {
		t.Fatalf("committed offset after restart = %d err=%v, want 3", off, err)
	}
	if off, err := kc2.QueryOffset("restart", 0, -2); err != nil || off != 0 {
		t.Fatalf("earliest offset after restart = %d err=%v, want 0", off, err)
	}
}

// TestStartupIgnoresLeftoverTempFiles ensures the atomic-write temp files left
// behind by a crash during an ACL/schema rename are ignored, not parsed.
func TestStartupIgnoresLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"__acls.json.tmp-123", "__schemas.json.tmp-456", "offsets.gob.tmp-789"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{ not valid"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := authz.NewStore(filepath.Join(dir, "__acls.json")); err != nil {
		t.Fatalf("ACL store rejected leftover temp files: %v", err)
	}
	if _, err := schemaregistry.Open(filepath.Join(dir, "__schemas.json")); err != nil {
		t.Fatalf("schema registry rejected leftover temp files: %v", err)
	}
	if _, err := coordinator.NewOffsetStore(coordinator.BackendFile, dir); err != nil {
		t.Fatalf("offset store rejected leftover temp files: %v", err)
	}
}
