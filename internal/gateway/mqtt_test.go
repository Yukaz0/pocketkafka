package gateway

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"

	"github.com/neu/go-kafka-neu/internal/storage"
	"github.com/neu/go-kafka-neu/pkg/protocol"
)

func mqttPacket(ptype byte, payload []byte) []byte {
	buf := []byte{ptype << 4}
	for {
		digit := byte(len(payload) % 128)
		lenRest := len(payload) / 128
		if lenRest > 0 {
			digit |= 0x80
		}
		buf = append(buf, digit)
		if lenRest == 0 {
			break
		}
	}
	return append(buf, payload...)
}

func buildPublish(topic string, data []byte) []byte {
	payload := make([]byte, 0, 2+len(topic)+len(data))
	payload = append(payload, byte(len(topic)>>8), byte(len(topic)))
	payload = append(payload, topic...)
	payload = append(payload, data...)
	return mqttPacket(mqttPublish, payload)
}

func buildSubscribe(packetID uint16, filter string) []byte {
	payload := []byte{byte(packetID >> 8), byte(packetID)}
	payload = append(payload, byte(len(filter)>>8), byte(len(filter)))
	payload = append(payload, filter...)
	payload = append(payload, 0x00) // requested QoS
	return mqttPacket(mqttSubscribe, payload)
}

func readPacket(br *bufio.Reader) (byte, []byte, error) {
	first, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	length, err := readRemainingLength(br)
	if err != nil {
		return 0, nil, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	return first >> 4, payload, nil
}

func kafkaRecords(t *testing.T, store *storage.Store, topic string) [][2]string {
	t.Helper()
	p := store.GetPartition(topic, 0)
	if p == nil {
		return nil
	}
	raw, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var out [][2]string
	pos := 0
	for pos < len(raw) {
		h, err := storage.ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(storage.BatchTotalSize(h))
		b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err == nil {
			for _, r := range b.Records {
				out = append(out, [2]string{string(r.Key), string(r.Value)})
			}
		}
		pos += total
	}
	return out
}

func TestMQTTBridgePublish(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	bridge := NewMQTTBridge(store)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// Rebind: Start binds its own listener; use the free port.
	addr := ln.Addr().String()
	ln.Close()
	if err := bridge.Start(addr); err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	// CONNECT -> CONNACK
	if _, err := conn.Write(mqttPacket(mqttConnect, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02})); err != nil {
		t.Fatal(err)
	}
	ptype, _, err := readPacket(br)
	if err != nil || ptype != mqttConnack {
		t.Fatalf("expected CONNACK, got type=%d err=%v", ptype, err)
	}

	// PUBLISH sensors/device-1/temperature = 25.5
	conn.Write(buildPublish("sensors/device-1/temperature", []byte("25.5")))
	conn.Write(buildPublish("sensors/device-2/humidity", []byte("60")))
	time.Sleep(200 * time.Millisecond)

	recs := kafkaRecords(t, store, "mqtt-sensors")
	if len(recs) != 2 {
		t.Fatalf("expected 2 records in mqtt-sensors, got %v", recs)
	}
	if recs[0][0] != "device-1" || recs[0][1] != "25.5" {
		t.Fatalf("record 0 mismatch: %v", recs[0])
	}
	if recs[1][0] != "device-2" || recs[1][1] != "60" {
		t.Fatalf("record 1 mismatch: %v", recs[1])
	}
}

func TestMQTTBridgeSubscribeForward(t *testing.T) {
	store, err := storage.NewStore(t.TempDir(), 1024*1024, 4096)
	if err != nil {
		t.Fatal(err)
	}
	bridge := NewMQTTBridge(store)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	if err := bridge.Start(addr); err != nil {
		t.Fatal(err)
	}
	defer bridge.Close()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(mqttPacket(mqttConnect, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02}))
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttConnack {
		t.Fatalf("no connack: %d %v", ptype, err)
	}

	// SUBSCRIBE sensors/# -> SUBACK
	conn.Write(buildSubscribe(1, "sensors/#"))
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("no suback: %d %v", ptype, err)
	}

	// Publish a new sensor reading; the bridge should forward it to the subscriber.
	time.Sleep(200 * time.Millisecond)
	conn.Write(buildPublish("sensors/device-9/temp", []byte("42")))
	deadline := time.Now().Add(3 * time.Second)
	got := false
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		ptype, payload, err := readPacket(br)
		if err != nil {
			continue
		}
		if ptype == mqttPublish {
			topic, data, err := parsePublish(payload)
			if err == nil && topic != "" && string(data) == "42" {
				got = true
				break
			}
		}
	}
	if !got {
		t.Fatal("subscriber did not receive forwarded publish")
	}
}
