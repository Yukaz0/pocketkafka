package gateway

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/storage"
)

func appendMQTTString(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

// buildConnect builds an MQTT 3.1.1 CONNECT packet, optionally with credentials.
func buildConnect(clientID, username, password string, withCreds bool) []byte {
	payload := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02} // protocol, level, clean-session
	payload = append(payload, 0x00, 0x00)                         // keepalive
	payload = appendMQTTString(payload, clientID)
	if withCreds {
		payload[7] |= 0x80 // username flag
		payload[7] |= 0x40 // password flag
		payload = appendMQTTString(payload, username)
		payload = appendMQTTString(payload, password)
	}
	return mqttPacket(mqttConnect, payload)
}

func TestParseConnectCredentials(t *testing.T) {
	payload := buildConnect("c1", "alice", "s3cret", true)
	// Strip the MQTT fixed header (1 type byte + 1 length byte for small sizes).
	info, err := parseConnect(payload[2:])
	if err != nil {
		t.Fatalf("parseConnect: %v", err)
	}
	if info.ClientID != "c1" || info.Username != "alice" || info.Password != "s3cret" {
		t.Fatalf("connect info = %+v", info)
	}

	noCreds := buildConnect("c2", "", "", false)
	info2, err := parseConnect(noCreds[2:])
	if err != nil {
		t.Fatalf("parseConnect(no creds): %v", err)
	}
	if info2.HasUsername || info2.HasPassword {
		t.Fatalf("unexpected credentials: %+v", info2)
	}
}

func startSecureBridge(t *testing.T, a authz.Authorizer) (*storage.Store, string) {
	t.Helper()
	store, err := storage.NewStore(t.TempDir(), 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	users := []config.SecurityUser{{Username: "alice", Password: "s3cret"}}
	bridge := NewMQTTBridge(store).WithSecurity(true, users, a)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	if err := bridge.Start(addr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bridge.Close(); store.Close() })
	return store, addr
}

func TestMQTTSecureRejectsBadCredentials(t *testing.T) {
	_, addr := startSecureBridge(t, authz.NewInMemory())
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write(buildConnect("c1", "", "", false))
	ptype, payload, err := readPacket(bufio.NewReader(conn))
	if err != nil || ptype != mqttConnack {
		t.Fatalf("expected CONNACK, got type=%d err=%v", ptype, err)
	}
	if len(payload) < 2 || payload[1] != 0x05 {
		t.Fatalf("CONNACK return code = %v, want 0x05 (not authorized)", payload)
	}
}

func TestMQTTSecurePublishDeniedLeavesNoRecords(t *testing.T) {
	store, addr := startSecureBridge(t, authz.NewInMemory()) // default-deny
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(buildConnect("c1", "alice", "s3cret", true))
	if ptype, payload, err := readPacket(br); err != nil || ptype != mqttConnack || payload[1] != 0x00 {
		t.Fatalf("expected successful CONNACK, got type=%d payload=%v err=%v", ptype, payload, err)
	}

	conn.Write(buildPublish("sensors/device-1/temperature", []byte("25.5")))
	time.Sleep(200 * time.Millisecond)

	if recs := kafkaRecords(t, store, "mqtt-sensors"); len(recs) != 0 {
		t.Fatalf("denied publish was stored: %v", recs)
	}
}

func TestMQTTSecurePublishAllowed(t *testing.T) {
	acl := authz.NewInMemory()
	if err := acl.Upsert(authz.Rule{
		Principal: "alice", ResourceType: authz.ResourceTopic,
		ResourceName: "mqtt-sensors", Operations: []string{"Write", "Read"},
	}); err != nil {
		t.Fatal(err)
	}
	store, addr := startSecureBridge(t, acl)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(buildConnect("c1", "alice", "s3cret", true))
	if ptype, payload, err := readPacket(br); err != nil || ptype != mqttConnack || payload[1] != 0x00 {
		t.Fatalf("expected successful CONNACK, got type=%d payload=%v err=%v", ptype, payload, err)
	}

	conn.Write(buildPublish("sensors/device-1/temperature", []byte("25.5")))
	time.Sleep(200 * time.Millisecond)

	recs := kafkaRecords(t, store, "mqtt-sensors")
	if len(recs) != 1 || recs[0][1] != "25.5" {
		t.Fatalf("allowed publish not stored: %v", recs)
	}
}

func TestMQTTSecureSubscribeDenied(t *testing.T) {
	acl := authz.NewInMemory()
	// alice may write but not read.
	if err := acl.Upsert(authz.Rule{
		Principal: "alice", ResourceType: authz.ResourceTopic,
		ResourceName: "mqtt-sensors", Operations: []string{"Write"},
	}); err != nil {
		t.Fatal(err)
	}
	_, addr := startSecureBridge(t, acl)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(buildConnect("c1", "alice", "s3cret", true))
	if ptype, payload, err := readPacket(br); err != nil || ptype != mqttConnack || payload[1] != 0x00 {
		t.Fatalf("expected successful CONNACK, got type=%d payload=%v err=%v", ptype, payload, err)
	}

	conn.Write(buildSubscribe(1, "sensors/#"))
	ptype, payload, err := readPacket(br)
	if err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}
	// payload = packet id (2 bytes) then one return code.
	if len(payload) < 3 || payload[2] != 0x80 {
		t.Fatalf("SUBACK codes = %v, want 0x80 (failure)", payload)
	}
}
