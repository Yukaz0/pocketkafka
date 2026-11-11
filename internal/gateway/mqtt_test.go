package gateway

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
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
	payload = append(payload, 0x00)
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

	if _, err := conn.Write(mqttPacket(mqttConnect, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02})); err != nil {
		t.Fatal(err)
	}
	ptype, _, err := readPacket(br)
	if err != nil || ptype != mqttConnack {
		t.Fatalf("expected CONNACK, got type=%d err=%v", ptype, err)
	}

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

	conn.Write(buildSubscribe(1, "sensors/#"))
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("no suback: %d %v", ptype, err)
	}

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
			_, data, _, _, err := parsePublish(0, payload)
			if err == nil && string(data) == "42" {
				got = true
				break
			}
		}
	}
	if !got {
		t.Fatal("subscriber did not receive forwarded publish")
	}
}

func startTestBridge(t *testing.T) (*MQTTBridge, *storage.Store, string) {
	t.Helper()
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
	t.Cleanup(func() {
		bridge.Close()
		store.Close()
	})
	return bridge, store, addr
}

func mqttConnectClient(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	br := bufio.NewReader(conn)
	if _, err := conn.Write(mqttPacket(mqttConnect, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02})); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttConnack {
		t.Fatalf("expected CONNACK, got type=%d err=%v", ptype, err)
	}
	return conn, br
}

func readFixedPacket(br *bufio.Reader) (byte, []byte, error) {
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
	return first, payload, nil
}

func buildPublishQoS(topic string, data []byte, qos byte, packetID uint16) []byte {
	payload := []byte{byte(len(topic) >> 8), byte(len(topic))}
	payload = append(payload, topic...)
	if qos > 0 {
		payload = append(payload, byte(packetID>>8), byte(packetID))
	}
	payload = append(payload, data...)
	buf := []byte{mqttPublish<<4 | qos<<1}
	buf = appendRemainingLength(buf, len(payload))
	return append(buf, payload...)
}

func buildUnsubscribe(packetID uint16, filter string) []byte {
	payload := []byte{byte(packetID >> 8), byte(packetID), byte(len(filter) >> 8), byte(len(filter))}
	payload = append(payload, filter...)
	return mqttPacket(mqttUnsubscribe, payload)
}

func appendBridgedRecord(t *testing.T, store *storage.Store, kafkaTopic, mqttTopic, value string) {
	t.Helper()
	if store.GetTopic(kafkaTopic) == nil {
		store.EnsureTopic(kafkaTopic, 1)
	}
	p := store.GetPartition(kafkaTopic, 0)
	if p == nil {
		t.Fatalf("no partition for %s", kafkaTopic)
	}
	now := time.Now().UnixMilli()
	batch := &protocol.RecordBatch{
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records: []protocol.Record{{
			Key:     []byte("k"),
			Value:   []byte(value),
			Headers: []protocol.RecordHeader{{Key: MQTTTopicHeader, Value: []byte(mqttTopic)}},
		}},
	}
	raw, err := protocol.EncodeRecordBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Append(raw); err != nil {
		t.Fatal(err)
	}
}

func kafkaRecordHeader(t *testing.T, store *storage.Store, topic string, recIdx int, key string) string {
	t.Helper()
	p := store.GetPartition(topic, 0)
	if p == nil {
		return ""
	}
	raw, _, err := p.Read(0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	pos, idx := 0, 0
	for pos < len(raw) {
		h, err := storage.ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(storage.BatchTotalSize(h))
		b, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err == nil {
			for _, r := range b.Records {
				if idx == recIdx {
					for _, rh := range r.Headers {
						if rh.Key == key {
							return string(rh.Value)
						}
					}
					return ""
				}
				idx++
			}
		}
		pos += total
	}
	return ""
}

func TestMQTTBridgeCloseWithIdleClient(t *testing.T) {
	bridge, _, addr := startTestBridge(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for bridge.ActiveClients() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if bridge.ActiveClients() == 0 {
		t.Fatal("idle client was not accepted")
	}

	done := make(chan struct{})
	go func() {
		bridge.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not return within 1s with an idle client connected")
	}
}

func TestMQTTBridgeForwardLargeBacklog(t *testing.T) {
	_, store, addr := startTestBridge(t)
	conn, br := mqttConnectClient(t, addr)
	if _, err := conn.Write(buildSubscribe(1, "sensors/#")); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}

	body := make([]byte, 2048)
	for i := range body {
		body[i] = 'x'
	}

	for range 600 {
		appendBridgedRecord(t, store, "mqtt-sensors", "sensors/dev/temp", string(body))
	}
	appendBridgedRecord(t, store, "mqtt-sensors", "sensors/dev/temp", "MARKER")

	got := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		first, payload, err := readFixedPacket(br)
		if err != nil {
			break
		}
		if first>>4 != mqttPublish {
			continue
		}
		_, data, _, _, derr := parsePublish(first&0x0F, payload)
		if derr == nil && string(data) == "MARKER" {
			got = true
			break
		}
	}
	if !got {
		t.Fatal("marker after a >1MB backlog was never delivered")
	}
}

func TestMQTTBridgeTopicFilteringPreservesTopic(t *testing.T) {
	_, store, addr := startTestBridge(t)
	conn, br := mqttConnectClient(t, addr)
	if _, err := conn.Write(buildSubscribe(1, "sensors/a/temp")); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}

	if _, err := conn.Write(buildPublish("sensors/a/temp", []byte("AAA"))); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(buildPublish("sensors/b/temp", []byte("BBB"))); err != nil {
		t.Fatal(err)
	}

	recDeadline := time.Now().Add(2 * time.Second)
	for len(kafkaRecords(t, store, "mqtt-sensors")) < 2 && time.Now().Before(recDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if hdr := kafkaRecordHeader(t, store, "mqtt-sensors", 0, MQTTTopicHeader); hdr != "sensors/a/temp" {
		t.Fatalf("record header %s = %q, want sensors/a/temp", MQTTTopicHeader, hdr)
	}

	type pub struct{ topic, data string }
	var got []pub
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		first, payload, err := readFixedPacket(br)
		if err != nil {
			break
		}
		if first>>4 != mqttPublish {
			continue
		}
		topic, data, _, _, derr := parsePublish(first&0x0F, payload)
		if derr == nil {
			got = append(got, pub{topic, string(data)})
		}
	}
	if len(got) != 1 || got[0].data != "AAA" || got[0].topic != "sensors/a/temp" {
		t.Fatalf("subscriber received %v, want exactly sensors/a/temp=AAA", got)
	}
}

func TestMQTTBridgeNoDuplicateForwarding(t *testing.T) {
	_, _, addr := startTestBridge(t)
	conn, br := mqttConnectClient(t, addr)
	for id, filter := range []string{"sensors/#", "sensors/a/#"} {
		if _, err := conn.Write(buildSubscribe(uint16(id+1), filter)); err != nil {
			t.Fatal(err)
		}
		if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
			t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
		}
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := conn.Write(buildPublish("sensors/a/temp", []byte("once"))); err != nil {
		t.Fatal(err)
	}

	count := 0
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		first, payload, err := readFixedPacket(br)
		if err != nil {
			break
		}
		if first>>4 != mqttPublish {
			continue
		}
		if _, data, _, _, derr := parsePublish(first&0x0F, payload); derr == nil && string(data) == "once" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("overlapping filters forwarded the record %d times, want 1", count)
	}
}

func TestMQTTBridgeQoS1Publish(t *testing.T) {
	_, store, addr := startTestBridge(t)
	conn, br := mqttConnectClient(t, addr)
	if _, err := conn.Write(buildSubscribe(1, "sensors/#")); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}
	if _, err := conn.Write(buildPublishQoS("sensors/dev/temp", []byte("hello-qos1"), 1, 7)); err != nil {
		t.Fatal(err)
	}

	sawAck := false
	forwarded := ""
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && (!sawAck || forwarded == "") {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		first, payload, err := readFixedPacket(br)
		if err != nil {
			continue
		}
		switch first >> 4 {
		case mqttPuback:
			if len(payload) >= 2 && binary.BigEndian.Uint16(payload) == 7 {
				sawAck = true
			}
		case mqttPublish:
			if _, data, _, _, derr := parsePublish(first&0x0F, payload); derr == nil {
				forwarded = string(data)
			}
		}
	}
	if !sawAck {
		t.Fatal("QoS 1 publish did not receive PUBACK")
	}
	if forwarded != "hello-qos1" {
		t.Fatalf("forwarded payload = %q, want exact payload without packet id", forwarded)
	}
	recs := kafkaRecords(t, store, "mqtt-sensors")
	if len(recs) != 1 || recs[0][1] != "hello-qos1" {
		t.Fatalf("stored records = %v, want exact payload", recs)
	}
}

func TestMQTTUnsubscribeStopsForwarding(t *testing.T) {
	_, _, addr := startTestBridge(t)
	conn, br := mqttConnectClient(t, addr)
	if _, err := conn.Write(buildSubscribe(1, "sensors/#")); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}
	if _, err := conn.Write(buildUnsubscribe(2, "sensors/#")); err != nil {
		t.Fatal(err)
	}
	if first, _, err := readFixedPacket(br); err != nil || first>>4 != mqttUnsuback {
		t.Fatalf("expected UNSUBACK, got type=%d err=%v", first>>4, err)
	}

	time.Sleep(200 * time.Millisecond)
	if _, err := conn.Write(buildPublish("sensors/dev/temp", []byte("after-unsub"))); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
	for {
		first, payload, err := readFixedPacket(br)
		if err != nil {
			break
		}
		if first>>4 == mqttPublish {
			if _, data, _, _, derr := parsePublish(first&0x0F, payload); derr == nil {
				t.Fatalf("received %q after UNSUBSCRIBE", data)
			}
		}
	}
}

func TestMQTTPreConnectPacketLimit(t *testing.T) {
	_, _, addr := startTestBridge(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	head := []byte{mqttConnect << 4}
	head = appendRemainingLength(head, 100000)
	if _, err := conn.Write(head); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err == nil {
		t.Fatal("expected the connection to close on an oversized pre-CONNECT packet")
	}
}

func TestMQTTBridgeClientTracking(t *testing.T) {
	bridge, _, addr := startTestBridge(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)

	clientID := "test-client-alpha"
	payload := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3c}
	payload = append(payload, byte(len(clientID)>>8), byte(len(clientID)))
	payload = append(payload, clientID...)

	if _, err := conn.Write(mqttPacket(mqttConnect, payload)); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttConnack {
		t.Fatalf("expected CONNACK, got type=%d err=%v", ptype, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var snap []MQTTClientInfo
	for time.Now().Before(deadline) {
		snap = bridge.ClientsSnapshot()
		if len(snap) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(snap) != 1 {
		t.Fatalf("expected 1 tracked client, got %d", len(snap))
	}
	if snap[0].ClientID != clientID {
		t.Fatalf("expected clientID %q, got %q", clientID, snap[0].ClientID)
	}
	if snap[0].RemoteAddr != conn.LocalAddr().String() {
		t.Fatalf("expected remoteAddr %q, got %q", conn.LocalAddr().String(), snap[0].RemoteAddr)
	}
	if snap[0].SubsCount != 0 {
		t.Fatalf("expected 0 subs, got %d", snap[0].SubsCount)
	}
	if snap[0].ConnectedAt.IsZero() {
		t.Fatal("expected non-zero ConnectedAt")
	}

	if _, err := conn.Write(buildSubscribe(1, "sensors/#")); err != nil {
		t.Fatal(err)
	}
	if ptype, _, err := readPacket(br); err != nil || ptype != mqttSuback {
		t.Fatalf("expected SUBACK, got type=%d err=%v", ptype, err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap = bridge.ClientsSnapshot()
		if len(snap) == 1 && snap[0].SubsCount == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(snap) != 1 || snap[0].SubsCount != 1 {
		t.Fatalf("expected 1 sub, got %+v", snap)
	}

	conn.Close()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap = bridge.ClientsSnapshot()
		if len(snap) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(snap) != 0 {
		t.Fatalf("expected 0 tracked clients after disconnect, got %d", len(snap))
	}
}

func TestMQTTBridgePublishAndTopicMapping(t *testing.T) {
	bridge, store, _ := startTestBridge(t)

	if err := bridge.Publish("test-user", "sensors/temp/room1", []byte("22.4")); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if err := bridge.Publish("test-user", "", []byte("invalid")); err == nil {
		t.Fatal("expected error on empty topic")
	}

	recs := kafkaRecords(t, store, "mqtt-sensors")
	if len(recs) != 1 {
		t.Fatalf("expected 1 record in mqtt-sensors, got %d", len(recs))
	}
	if recs[0][0] != "temp" || recs[0][1] != "22.4" {
		t.Fatalf("unexpected record: key=%q value=%q", recs[0][0], recs[0][1])
	}

	cases := map[string]string{
		"sensors/device/metric": "mqtt-sensors",
		"/devices/power/status": "mqtt-devices",
		"iot":                   "mqtt-iot",
		"":                      "mqtt-inbound",
	}
	for in, want := range cases {
		if got := MQTTTopicToKafka(in); got != want {
			t.Errorf("MQTTTopicToKafka(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMQTTKeepaliveEnforced(t *testing.T) {
	bridge, _, addr := startTestBridge(t)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	payload := []byte{
		0x00, 0x04, 'M', 'Q', 'T', 'T',
		0x04,
		0x02,
		0x00, 0x01,
		0x00, 0x04, 't', 'e', 's', 't',
	}
	pkt := mqttPacket(mqttConnect, payload)
	if _, err := conn.Write(pkt); err != nil {
		t.Fatalf("write connect: %v", err)
	}

	br := bufio.NewReader(conn)
	ptype, _, err := readPacket(br)
	if err != nil || ptype != mqttConnack {
		t.Fatalf("expected connack, got ptype=%d err=%v", ptype, err)
	}

	if bridge.ActiveClients() != 1 {
		t.Fatalf("expected 1 active client, got %d", bridge.ActiveClients())
	}

	time.Sleep(2 * time.Second)

	buf := make([]byte, 16)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed after keepalive timeout")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && bridge.ActiveClients() != 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if bridge.ActiveClients() != 0 {
		t.Fatalf("expected 0 active clients after timeout, got %d", bridge.ActiveClients())
	}
}
