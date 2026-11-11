package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	brokerserver "github.com/Yukaz0/pocketkafka/internal/server"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/client"
)

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// testBroker spins an ephemeral broker, returns addr for client dial.
func testBroker(t *testing.T) string {
	t.Helper()
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cfg := config.Default()
	cfg.Storage.DataDir = t.TempDir()
	cfg.Listeners = map[string]string{"test": addr}
	cfg.AdvertisedListeners = map[string]string{"test": addr}

	store, err := storage.NewStore(cfg.Storage.DataDir, cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	offsetStore, _ := coordinator.NewOffsetStore("inmemory", cfg.Storage.DataDir)
	gm := coordinator.NewGroupManager(offsetStore, 0, "localhost", int32(port), int32(cfg.Coordinator.SessionTimeoutMs))
	h := handler.New(store, gm, &cfg, 0, "localhost", int32(port))
	srv := brokerserver.New(&cfg, h)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	return addr
}

func testHTTPClient() *http.Client {
	return &http.Client{Timeout: time.Second}
}

func testServer(t *testing.T) *server {
	t.Helper()
	kc, err := client.NewClient([]string{testBroker(t)}, "mcp-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { kc.Close() })
	return &server{kc: kc, registry: "http://127.0.0.1:1", http: testHTTPClient()}
}

func rpc(t *testing.T, s *server, method string, params interface{}) rpcResp {
	t.Helper()
	var raw json.RawMessage
	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			t.Fatal(err)
		}
		raw = b
	}
	line, _ := json.Marshal(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "method": method, "params": raw})
	return s.dispatch(line)
}

func TestHandshake(t *testing.T) {
	s := &server{}
	r := rpc(t, s, "initialize", nil)
	m, ok := r.Result.(map[string]any)
	if !ok || m["protocolVersion"] != protoVersion {
		t.Fatalf("bad initialize: %+v", r)
	}
	if r := rpc(t, s, "ping", nil); r.Error != nil {
		t.Fatalf("ping: %+v", r.Error)
	}
	r = rpc(t, s, "tools/list", nil)
	m, ok = r.Result.(map[string]any)
	if !ok {
		t.Fatalf("bad tools/list: %+v", r)
	}
	tools, ok := m["tools"].([]tool)
	if !ok || len(tools) != 27 {
		t.Fatalf("want 27 tools, got %d", len(tools))
	}
}

func TestErrors(t *testing.T) {
	s := &server{}
	if r := rpc(t, s, "nope", nil); r.Error == nil || r.Error.Code != -32601 {
		t.Fatalf("want -32601, got %+v", r)
	}
	if r := s.dispatch([]byte("{bad")); r.Error == nil || r.Error.Code != -32700 {
		t.Fatalf("want -32700, got %+v", r)
	}
	r := rpc(t, s, "tools/call", map[string]any{"name": "nope", "arguments": map[string]any{}})
	if r.Error == nil || r.Error.Code != -32601 {
		t.Fatalf("want -32601, got %+v", r)
	}
	r = rpc(t, s, "tools/call", map[string]any{"name": "fetch_messages", "arguments": map[string]any{}})
	if r.Error == nil || r.Error.Code != -32602 {
		t.Fatalf("want -32602, got %+v", r)
	}
}

func TestListDescribeEmpty(t *testing.T) {
	s := testServer(t)
	r := rpc(t, s, "tools/call", map[string]any{"name": "list_topics", "arguments": map[string]any{}})
	if r.Error != nil {
		t.Fatalf("list_topics: %+v", r.Error)
	}
	r = rpc(t, s, "tools/call", map[string]any{
		"name": "describe_topic", "arguments": map[string]any{"topic": "missing"},
	})
	if r.Error == nil || r.Error.Code != -32000 {
		t.Fatalf("want -32000, got %+v", r)
	}
}

func TestProduceFetchRoundtrip(t *testing.T) {
	s := testServer(t)
	prod := client.NewProducer(s.kc, client.DefaultProducerConfig())
	defer prod.Close()
	for i := 0; i < 3; i++ {
		if _, err := prod.SendSync(context.Background(),
			&client.Message{Topic: "t", Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	// big value → truncated
	big := strings.Repeat("x", maxValueLen+10)
	if _, err := prod.SendSync(context.Background(),
		&client.Message{Topic: "t", Value: []byte(big)}); err != nil {
		t.Fatal(err)
	}

	out, cerr := s.fetchMessages(map[string]any{"topic": "t", "limit": float64(10)})
	if cerr != nil {
		t.Fatal(cerr)
	}
	msgs := out.([]fetchedMsg)
	if len(msgs) != 4 {
		t.Fatalf("want 4 msgs, got %d", len(msgs))
	}
	if msgs[0].Value != "v0" || msgs[0].Offset != 0 {
		t.Fatalf("bad first msg: %+v", msgs[0])
	}
	if !msgs[3].Truncated || len(msgs[3].Value) != maxValueLen {
		t.Fatalf("want truncated 4KB value, got %+v", msgs[3])
	}

	// offset explicit
	out, cerr = s.fetchMessages(map[string]any{"topic": "t", "offset": float64(2), "limit": float64(10)})
	if cerr != nil {
		t.Fatal(cerr)
	}
	if msgs := out.([]fetchedMsg); len(msgs) != 2 || msgs[0].Value != "v2" {
		t.Fatalf("bad offset fetch: %+v", msgs)
	}

	// health counts 4 messages
	h, cerr := s.health()
	if cerr != nil {
		t.Fatal(cerr)
	}
	if h.(map[string]any)["messages"] != int64(4) {
		t.Fatalf("bad health: %+v", h)
	}
}

func TestWriteGated(t *testing.T) {
	s := testServer(t) // allowWrite false
	r := rpc(t, s, "tools/call", map[string]any{
		"name": "produce_message", "arguments": map[string]any{"topic": "t", "value": "x"},
	})
	if r.Error == nil || r.Error.Code != -32002 {
		t.Fatalf("want -32002, got %+v", r)
	}
	r = rpc(t, s, "tools/call", map[string]any{
		"name":      "reset_consumer_offset",
		"arguments": map[string]any{"group_id": "g", "topic": "t"},
	})
	if r.Error == nil || r.Error.Code != -32002 {
		t.Fatalf("want -32002 (not -32602), got %+v", r)
	}
	// write on → produce + blind reset ditolak param
	s.allowWrite = true
	r = rpc(t, s, "tools/call", map[string]any{
		"name": "produce_message", "arguments": map[string]any{"topic": "wt", "value": "hello"},
	})
	if r.Error != nil {
		t.Fatalf("produce: %+v", r.Error)
	}
	r = rpc(t, s, "tools/call", map[string]any{
		"name":      "reset_consumer_offset",
		"arguments": map[string]any{"group_id": "g", "topic": "wt"},
	})
	if r.Error == nil || r.Error.Code != -32602 {
		t.Fatalf("want -32602 for blind reset, got %+v", r)
	}
}

func TestAuth(t *testing.T) {
	s := &server{authToken: "secret"}
	r := rpc(t, s, "initialize", map[string]any{})
	if r.Error == nil || r.Error.Code != -32001 {
		t.Fatalf("want -32001, got %+v", r)
	}
	r = rpc(t, s, "initialize", map[string]any{"token": "secret"})
	if r.Error != nil {
		t.Fatalf("authed initialize: %+v", r.Error)
	}
}

func TestFleetMqttNoWeb(t *testing.T) {
	s := testServer(t)
	s.web = ""
	r := rpc(t, s, "tools/call", map[string]any{"name": "fleet_status", "arguments": map[string]any{}})
	if r.Error == nil || r.Error.Code != -32000 {
		t.Fatalf("want -32000, got %+v", r)
	}
	r = rpc(t, s, "tools/call", map[string]any{
		"name": "fleet_container_stop", "arguments": map[string]any{"container_id": "x"},
	})
	if r.Error == nil || r.Error.Code != -32002 {
		t.Fatalf("write gate first: want -32002, got %+v", r)
	}
}

func TestRegistryDown(t *testing.T) {
	s := testServer(t)
	if _, cerr := s.getSchema(map[string]any{"subject": "x"}); cerr == nil || cerr.code != -32000 {
		t.Fatalf("want -32000, got %+v", cerr)
	}
}

func TestLoopStdio(t *testing.T) {
	s := testServer(t)
	in := `{"jsonrpc":"2.0","id":1,"method":"initialize"}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_topics","arguments":{}}}` + "\n"
	var buf bytes.Buffer
	s.loop(strings.NewReader(in), &buf)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 responses, got %q", buf.String())
	}
	var r rpcResp
	if err := json.Unmarshal([]byte(lines[1]), &r); err != nil || r.Error != nil {
		t.Fatalf("bad second response: %s", lines[1])
	}
}
