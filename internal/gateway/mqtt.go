package gateway

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// MQTT packet types (MQTT 3.1.1).
const (
	mqttConnect    = 1
	mqttConnack    = 2
	mqttPublish    = 3
	mqttPuback     = 4
	mqttSubscribe  = 8
	mqttSuback     = 9
	mqttPingreq    = 12
	mqttPingresp   = 13
	mqttDisconnect = 14
)

// MQTTBridge is a minimal MQTT 3.1.1 broker that bridges MQTT topics to Kafka
// topics on port 1883. A publish to sensors/<device>/<metric> is stored in the
// Kafka topic mqtt-<group> keyed by <device>; subscriptions fetch from Kafka and
// forward messages back to MQTT subscribers.
type MQTTBridge struct {
	store    *storage.Store
	listener net.Listener
	wg       sync.WaitGroup
	closeCh  chan struct{}

	active  atomic.Int32 // currently connected MQTT clients
	bridged atomic.Int64 // messages bridged MQTT -> Kafka since start

	// Security (optional). When secure is true a client must present valid
	// CONNECT credentials and every publish/subscribe is authorized against the
	// mapped Kafka topic, closing the MQTT authorization bypass.
	secure     bool
	users      []config.SecurityUser
	authorizer authz.Authorizer
}

// WithSecurity enables MQTT authentication and authorization. When enabled the
// bridge requires CONNECT username/password matching users and authorizes
// publish (Write) and subscribe (Read) against the mapped Kafka topic using
// a. Passing enabled=false keeps the historical unauthenticated behaviour.
func (b *MQTTBridge) WithSecurity(enabled bool, users []config.SecurityUser, a authz.Authorizer) *MQTTBridge {
	b.secure = enabled
	b.users = users
	// Security disabled always means allow-all (decision D2), even if a
	// default-deny store was passed in.
	if !enabled || a == nil {
		a = authz.AllowAllAuthorizer{}
	}
	b.authorizer = a
	return b
}

// authorize checks a principal's permission, defaulting to anonymous.
func (b *MQTTBridge) authorize(principal string, op authz.Operation, kafkaTopic string) error {
	if b.authorizer == nil {
		return nil
	}
	if principal == "" {
		principal = authz.AnonymPrincipal
	}
	return b.authorizer.Authorize(principal, op, authz.Resource{Type: authz.ResourceTopic, Name: kafkaTopic})
}

// authenticate returns the principal for a CONNECT packet, or ok=false when the
// credentials are missing or wrong.
func (b *MQTTBridge) authenticate(info *mqttConnectInfo) (string, bool) {
	for _, u := range b.users {
		if u.Username == info.Username && u.Password == info.Password && u.Username != "" {
			return u.Username, true
		}
	}
	return "", false
}

// ActiveClients reports the number of live MQTT client connections.
func (b *MQTTBridge) ActiveClients() int32 { return b.active.Load() }

// BridgedCount reports how many messages were bridged into Kafka.
func (b *MQTTBridge) BridgedCount() int64 { return b.bridged.Load() }

// NewMQTTBridge builds a bridge bound to the storage engine.
func NewMQTTBridge(store *storage.Store) *MQTTBridge {
	return &MQTTBridge{store: store, closeCh: make(chan struct{})}
}

// Start binds the MQTT listener and accepts connections.
func (b *MQTTBridge) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.listener = ln
	log.Printf("pocketkafka MQTT bridge on %s", addr)
	b.wg.Add(1)
	go b.acceptLoop()
	return nil
}

func (b *MQTTBridge) acceptLoop() {
	defer b.wg.Done()
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.closeCh:
				return
			default:
			}
			return
		}
		b.wg.Add(1)
		go b.serveClient(conn)
	}
}

// Close shuts down the bridge.
func (b *MQTTBridge) Close() error {
	select {
	case <-b.closeCh:
	default:
		close(b.closeCh)
	}
	if b.listener != nil {
		b.listener.Close()
	}
	b.wg.Wait()
	return nil
}

// ---------------------------------------------------------------------------
// MQTT client session
// ---------------------------------------------------------------------------

type mqttClient struct {
	conn      net.Conn
	br        *bufio.Reader
	mu        sync.Mutex // serializes writes
	stop      chan struct{}
	principal string
	authed    bool
}

func (b *MQTTBridge) serveClient(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()
	b.active.Add(1)
	defer b.active.Add(-1)
	c := &mqttClient{conn: conn, br: bufio.NewReader(conn), stop: make(chan struct{}), authed: !b.secure}
	defer close(c.stop)

	var subs []string // kafka topics subscribed
	for {
		ptype, payload, err := readMQTTPacket(c.br)
		if err != nil {
			return
		}
		if b.secure && !c.authed && ptype != mqttConnect {
			// No request is served before a successful CONNECT.
			return
		}
		switch ptype {
		case mqttConnect:
			if b.secure {
				info, err := parseConnect(payload)
				if err != nil {
					c.writePacket(mqttConnack, []byte{0x00, 0x01}) // unacceptable protocol
					return
				}
				user, ok := b.authenticate(info)
				if !ok {
					// 0x05 = not authorized.
					c.writePacket(mqttConnack, []byte{0x00, 0x05})
					return
				}
				c.principal = user
				c.authed = true
			}
			if err := c.writePacket(mqttConnack, []byte{0x00, 0x00}); err != nil {
				return
			}
		case mqttPublish:
			ptopic, data, err := parsePublish(payload)
			if err != nil {
				continue
			}
			if err := b.authorize(c.principal, authz.OpWrite, mqttTopicToKafka(ptopic)); err != nil {
				log.Printf("mqtt publish denied for %q: %v", c.principal, err)
				continue
			}
			if err := b.bridgePublish(ptopic, data); err != nil {
				log.Printf("mqtt bridge publish: %v", err)
			}
		case mqttSubscribe:
			packetID, filters, err := parseSubscribe(payload)
			if err != nil {
				continue
			}
			// Acknowledge with granted QoS 0, or 0x80 for denied filters.
			codes := make([]byte, len(filters))
			allowed := make([]bool, len(filters))
			for i, f := range filters {
				if b.authorize(c.principal, authz.OpRead, mqttFilterToKafka(f)) != nil {
					codes[i] = 0x80 // failure: not authorized
					continue
				}
				codes[i] = 0x00
				allowed[i] = true
			}
			suback := append(u16(packetID), codes...)
			if err := c.writePacket(mqttSuback, suback); err != nil {
				return
			}
			for i, f := range filters {
				if !allowed[i] {
					continue
				}
				kt := mqttFilterToKafka(f)
				subs = append(subs, kt)
				// Ensure the Kafka topic exists; capture the current offset so the
				// forward loop catches messages published after subscribe.
				b.store.EnsureTopic(kt, 1)
				start := int64(0)
				if part := b.store.GetPartition(kt, 0); part != nil {
					start = part.HighWatermark()
				}
				go b.forwardLoop(c, kt, start)
			}
		case mqttPingreq:
			if err := c.writePacket(mqttPingresp, nil); err != nil {
				return
			}
		case mqttDisconnect:
			return
		}
	}
}

// writePacket writes an MQTT packet under the client write mutex.
func (c *mqttClient) writePacket(ptype byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := []byte{ptype << 4}
	buf = appendRemainingLength(buf, len(payload))
	buf = append(buf, payload...)
	_, err := c.conn.Write(buf)
	return err
}

// bridgePublish stores an MQTT publish into Kafka.
func (b *MQTTBridge) bridgePublish(mqttTopic string, data []byte) error {
	kt := mqttTopicToKafka(mqttTopic)
	key := topicKey(mqttTopic)
	if b.store.GetTopic(kt) == nil {
		b.store.EnsureTopic(kt, 1)
	}
	part := b.store.GetPartition(kt, 0)
	if part == nil {
		return fmt.Errorf("partition unavailable for %s", kt)
	}
	now := time.Now().UnixMilli()
	batch := &protocol.RecordBatch{
		BaseTimestamp: now,
		MaxTimestamp:  now,
		ProducerID:    -1,
		ProducerEpoch: -1,
		BaseSequence:  -1,
		Records:       []protocol.Record{{Key: []byte(key), Value: data}},
	}
	raw, err := protocol.EncodeRecordBatch(batch)
	if err != nil {
		return err
	}
	_, err = part.Append(raw)
	if err == nil {
		b.bridged.Add(1)
	}
	return err
}

// forwardLoop polls a Kafka topic and forwards new records to the MQTT client.
func (b *MQTTBridge) forwardLoop(c *mqttClient, kafkaTopic string, next int64) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-b.closeCh:
			return
		case <-ticker.C:
			part := b.store.GetPartition(kafkaTopic, 0)
			if part == nil {
				continue
			}
			hwm := part.HighWatermark()
			if hwm <= next {
				continue
			}
			raw, _, err := part.Read(next, 1<<20)
			if err != nil || len(raw) == 0 {
				next = hwm
				continue
			}
			mqttTopic := kafkaTopicToMQTT(kafkaTopic)
			pos := 0
			for pos < len(raw) {
				h, err := storage.ParseRecordBatchHeader(raw[pos:])
				if err != nil {
					break
				}
				total := int(storage.BatchTotalSize(h))
				if pos+total > len(raw) {
					break
				}
				if batch, err := protocol.DecodeRecordBatch(raw[pos : pos+total]); err == nil {
					for _, rec := range batch.Records {
						payload := mqttPublishPayload(mqttTopic, rec.Value)
						if err := c.writePacket(mqttPublish, payload); err != nil {
							return
						}
					}
				}
				pos += total
			}
			next = hwm
		}
	}
}

// ---------------------------------------------------------------------------
// MQTT packet parsing
// ---------------------------------------------------------------------------

func readMQTTPacket(br *bufio.Reader) (byte, []byte, error) {
	first, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	length, err := readRemainingLength(br)
	if err != nil {
		return 0, nil, err
	}
	if length < 0 || length > 1<<24 {
		return 0, nil, fmt.Errorf("invalid MQTT remaining length %d", length)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	return first >> 4, payload, nil
}

func readRemainingLength(br *bufio.Reader) (int, error) {
	multiplier := 1
	value := 0
	for i := 0; i < 4; i++ {
		digit, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		value += int(digit&0x7f) * multiplier
		if digit&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("malformed remaining length")
}

func appendRemainingLength(dst []byte, length int) []byte {
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		dst = append(dst, digit)
		if length == 0 {
			break
		}
	}
	return dst
}

// mqttConnectInfo holds the fields the bridge needs from a CONNECT packet.
type mqttConnectInfo struct {
	ClientID    string
	Username    string
	Password    string
	HasUsername bool
	HasPassword bool
}

// parseConnect extracts the client ID and optional username/password from an
// MQTT 3.1.1 CONNECT payload.
func parseConnect(payload []byte) (*mqttConnectInfo, error) {
	if len(payload) < 10 {
		return nil, fmt.Errorf("short connect packet")
	}
	if !bytes.HasPrefix(payload, []byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}) {
		return nil, fmt.Errorf("unsupported protocol name")
	}
	pos := 6
	level := payload[pos]
	pos++
	if level != 0x04 {
		return nil, fmt.Errorf("unsupported protocol level %d", level)
	}
	flags := payload[pos]
	pos++
	pos += 2 // keepalive

	info := &mqttConnectInfo{}
	clientID, n, err := readMQTTString(payload[pos:])
	if err != nil {
		return nil, err
	}
	info.ClientID = clientID
	pos += n

	if flags&0x04 != 0 { // will flag: will topic then will payload
		if _, n, err = readMQTTString(payload[pos:]); err != nil {
			return nil, err
		}
		pos += n
		if _, n, err = readMQTTString(payload[pos:]); err != nil {
			return nil, err
		}
		pos += n
	}
	if flags&0x80 != 0 { // username
		u, n, err := readMQTTString(payload[pos:])
		if err != nil {
			return nil, err
		}
		info.Username = u
		info.HasUsername = true
		pos += n
	}
	if flags&0x40 != 0 { // password (last field; no need to advance pos)
		p, _, err := readMQTTString(payload[pos:])
		if err != nil {
			return nil, err
		}
		info.Password = p
		info.HasPassword = true
	}
	return info, nil
}

// readMQTTString reads a 2-byte-length-prefixed MQTT string, returning the
// value and the number of bytes consumed.
func readMQTTString(b []byte) (string, int, error) {
	if len(b) < 2 {
		return "", 0, fmt.Errorf("short mqtt string")
	}
	n := int(binary.BigEndian.Uint16(b[0:2]))
	if 2+n > len(b) {
		return "", 0, fmt.Errorf("mqtt string overrun")
	}
	return string(b[2 : 2+n]), 2 + n, nil
}

// parsePublish extracts the topic and payload from a QoS 0 PUBLISH payload.
func parsePublish(payload []byte) (string, []byte, error) {
	if len(payload) < 2 {
		return "", nil, fmt.Errorf("short publish")
	}
	tlen := int(binary.BigEndian.Uint16(payload[0:2]))
	if 2+tlen > len(payload) {
		return "", nil, fmt.Errorf("publish topic overrun")
	}
	topic := string(payload[2 : 2+tlen])
	data := payload[2+tlen:]
	return topic, data, nil
}

// parseSubscribe extracts the packet ID and topic filters.
func parseSubscribe(payload []byte) (uint16, []string, error) {
	if len(payload) < 3 {
		return 0, nil, fmt.Errorf("short subscribe")
	}
	packetID := binary.BigEndian.Uint16(payload[0:2])
	var filters []string
	pos := 2
	for pos < len(payload) {
		if pos+2 > len(payload) {
			break
		}
		tlen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+tlen+1 > len(payload) {
			break
		}
		filters = append(filters, string(payload[pos:pos+tlen]))
		pos += tlen + 1 // skip requested QoS byte
	}
	return packetID, filters, nil
}

// mqttPublishPayload builds a QoS 0 PUBLISH payload (topic + data).
func mqttPublishPayload(topic string, data []byte) []byte {
	out := make([]byte, 0, 2+len(topic)+len(data))
	out = append(out, u16(uint16(len(topic)))...)
	out = append(out, topic...)
	out = append(out, data...)
	return out
}

func u16(v uint16) []byte {
	return []byte{byte(v >> 8), byte(v)}
}

// ---------------------------------------------------------------------------
// Topic mapping
// ---------------------------------------------------------------------------

// mqttTopicToKafka maps an MQTT topic to a Kafka topic. Following the spec,
// sensors/<device>/<metric> -> mqtt-<group>.
func mqttTopicToKafka(mqttTopic string) string {
	seg := strings.SplitN(strings.TrimPrefix(mqttTopic, "/"), "/", 2)
	if len(seg) >= 1 && seg[0] != "" {
		return "mqtt-" + seg[0]
	}
	return "mqtt-inbound"
}

// topicKey returns the device key (second path segment) of an MQTT topic.
func topicKey(mqttTopic string) string {
	seg := strings.Split(strings.TrimPrefix(mqttTopic, "/"), "/")
	if len(seg) >= 2 {
		return seg[1]
	}
	return ""
}

// mqttFilterToKafka maps an MQTT subscription filter to the Kafka topic.
func mqttFilterToKafka(filter string) string {
	// Strip trailing wildcards for the Kafka topic lookup.
	f := strings.TrimSuffix(filter, "/#")
	f = strings.TrimSuffix(f, "/+")
	return mqttTopicToKafka(f)
}

// kafkaTopicToMQTT converts a Kafka topic back to an MQTT topic for forwarding.
func kafkaTopicToMQTT(kafkaTopic string) string {
	return strings.ReplaceAll(kafkaTopic, "-", "/")
}
