package server

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func newTestServer(t *testing.T, mutate func(*config.Config)) (*Server, string) {
	t.Helper()
	addr := freePort(t)
	cfg := config.Default()
	cfg.Listeners = map[string]string{"test": addr}
	cfg.AdvertisedListeners = map[string]string{"test": addr}
	if mutate != nil {
		mutate(&cfg)
	}

	store, err := storage.NewStore(t.TempDir(), cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	offsets, err := coordinator.NewOffsetStore(coordinator.BackendInMemory, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gm := coordinator.NewGroupManager(offsets, 0, "localhost", 9092, int32(cfg.Coordinator.SessionTimeoutMs))
	h := handler.New(store, gm, &cfg, 0, "localhost", 9092)

	s := New(&cfg, h)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	return s, addr
}

// waitForConns polls until the server has the expected number of live
// connections, so the tests do not depend on accept scheduling.
func waitForConns(t *testing.T, s *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.connMu.Lock()
		n := len(s.conns)
		s.connMu.Unlock()
		if n == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.connMu.Lock()
	n := len(s.conns)
	s.connMu.Unlock()
	t.Fatalf("live connections = %d, want %d", n, want)
}

func TestConnectionLimitRejectsAndFrees(t *testing.T) {
	s, addr := newTestServer(t, func(c *config.Config) { c.Network.MaxConnections = 1 })
	defer s.Close()

	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	waitForConns(t, s, 1)

	// The second connection must be refused (closed) rather than served.
	c2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c2.Read(make([]byte, 1)); err == nil {
		t.Fatal("second connection was served despite max_connections=1")
	}
	if got := s.RejectedConnections(); got == 0 {
		t.Fatal("rejected connection was not counted")
	}

	// Closing the first connection frees the slot for a new one.
	c1.Close()
	waitForConns(t, s, 0)

	c3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	waitForConns(t, s, 1)
}

func TestCloseDoesNotDeadlockWithFullSlots(t *testing.T) {
	s, addr := newTestServer(t, func(c *config.Config) { c.Network.MaxConnections = 1 })

	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	waitForConns(t, s, 1)

	done := make(chan struct{})
	go func() {
		s.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close deadlocked while connection slots were full")
	}
}

func TestIncompleteFrameDisconnects(t *testing.T) {
	s, addr := newTestServer(t, func(c *config.Config) {
		c.Network.IdleTimeoutMs = 50
		c.Network.ReadTimeoutMs = 50
	})
	defer s.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Announce a 100-byte body, then send nothing: the read deadline must
	// disconnect us.
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], 100)
	if _, err := conn.Write(prefix[:]); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("incomplete frame was not disconnected")
	}
}

func TestIdleConnectionDisconnects(t *testing.T) {
	s, addr := newTestServer(t, func(c *config.Config) {
		c.Network.IdleTimeoutMs = 50
	})
	defer s.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Never send anything; the idle deadline must close the connection.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle connection was not disconnected")
	}
}
