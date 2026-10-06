package gateway

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/audit"
	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/credential"
	"github.com/Yukaz0/pocketkafka/internal/ratelimit"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// MQTT packet types (MQTT 3.1.1).
const (
	mqttConnect     = 1
	mqttConnack     = 2
	mqttPublish     = 3
	mqttPuback      = 4
	mqttPubrec      = 5
	mqttPubrel      = 6
	mqttPubcomp     = 7
	mqttSubscribe   = 8
	mqttSuback      = 9
	mqttUnsubscribe = 10
	mqttUnsuback    = 11
	mqttPingreq     = 12
	mqttPingresp    = 13
	mqttDisconnect  = 14
)

// Resource bounds and timeouts for MQTT clients. They keep a flood of
// connections or bogus packets from exhausting memory and stop a stalled peer
// from pinning a goroutine or the forward loop forever.
const (
	mqttMaxConnections     = 1024
	mqttMaxPreConnectBytes = 64 << 10 // before a successful CONNECT
	mqttMaxPacketBytes     = 1 << 20  // after CONNECT
	mqttHandshakeTimeout   = 10 * time.Second
	mqttWriteTimeout       = 10 * time.Second
	mqttForwardFallback    = 500 * time.Millisecond
)

// MQTTMaxConnections is the bridge's client connection limit, exported so the
// dashboard can report it next to the live connection count.
const MQTTMaxConnections = mqttMaxConnections

// MQTTTopicHeader is the record header carrying the original MQTT topic so a
// subscriber can filter on the exact topic it asked for. The dashboard's
// Integrations page documents this name, so it is exported.
const MQTTTopicHeader = "mqtt-topic"

// MQTTBridge is a minimal MQTT 3.1.1 broker that bridges MQTT topics to Kafka
// topics on port 1883. A publish to sensors/<device>/<metric> is stored in the
// Kafka topic mqtt-<group> keyed by <device>; subscriptions fetch from Kafka and
// forward messages back to MQTT subscribers.
type MQTTBridge struct {
	store    *storage.Store
	listener net.Listener
	wg       sync.WaitGroup
	closeCh  chan struct{}

	// connMu guards conns, the set of live client connections. Close closes all
	// of them so serveClient goroutines parked on a read cannot stall shutdown.
	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	// connSem bounds the number of concurrent MQTT clients.
	connSem chan struct{}

	active  atomic.Int32 // currently connected MQTT clients
	bridged atomic.Int64 // messages bridged MQTT -> Kafka since start

	// Security (optional). When secure is true a client must present valid
	// CONNECT credentials and every publish/subscribe is authorized against the
	// mapped Kafka topic, closing the MQTT authorization bypass.
	secure     bool
	users      []config.SecurityUser
	authorizer authz.Authorizer
	// tlsConf wraps the MQTT listener when security.tls.mqtt is set.
	tlsConf *tls.Config
	// requireTLS refuses a CONNECT that carries a password over an unencrypted
	// connection: MQTT sends it in the clear inside the CONNECT packet.
	requireTLS bool
	// authLimiter delays a source that keeps failing CONNECT.
	authLimiter *ratelimit.Limiter
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

// WithTLS serves MQTT over TLS. requireTLS additionally refuses a CONNECT that
// carries a username/password over an unencrypted connection.
func (b *MQTTBridge) WithTLS(conf *tls.Config, requireTLS bool) *MQTTBridge {
	b.tlsConf = conf
	b.requireTLS = requireTLS
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
// credentials are missing or wrong. The comparison is constant-time and accepts
// a plaintext password or a stored PBKDF2 hash.
func (b *MQTTBridge) authenticate(info *mqttConnectInfo) (string, bool) {
	for i := range b.users {
		u := &b.users[i]
		if u.Username == "" || u.Username != info.Username {
			continue
		}
		stored := u.Password
		if stored == "" {
			stored = u.PasswordHash
		}
		if stored == "" {
			return "", false
		}
		if credential.Matches(stored, info.Password) {
			return u.Username, true
		}
		return "", false
	}
	return "", false
}

// ActiveClients reports the number of live MQTT client connections.
func (b *MQTTBridge) ActiveClients() int32 { return b.active.Load() }

// BridgedCount reports how many messages were bridged into Kafka.
func (b *MQTTBridge) BridgedCount() int64 { return b.bridged.Load() }

// NewMQTTBridge builds a bridge bound to the storage engine.
func NewMQTTBridge(store *storage.Store) *MQTTBridge {
	return &MQTTBridge{
		store:       store,
		closeCh:     make(chan struct{}),
		conns:       make(map[net.Conn]struct{}),
		connSem:     make(chan struct{}, mqttMaxConnections),
		authLimiter: ratelimit.New(3, time.Second, 30*time.Second),
	}
}

// isTLSConn reports whether a client connection is encrypted.
func isTLSConn(conn net.Conn) bool {
	_, ok := conn.(*tls.Conn)
	return ok
}

// remoteHost renders a connection's peer host for the audit trail and the
// per-source limiter.
func remoteHost(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		if host, _, err := net.SplitHostPort(ra.String()); err == nil {
			return host
		}
		return ra.String()
	}
	return ""
}

// Start binds the MQTT listener and accepts connections. When a TLS
// configuration was installed the listener speaks TLS.
func (b *MQTTBridge) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if b.tlsConf != nil {
		ln = tls.NewListener(ln, b.tlsConf)
		log.Printf("pocketkafka MQTT bridge on %s (TLS)", addr)
	} else {
		log.Printf("pocketkafka MQTT bridge on %s", addr)
	}
	b.listener = ln
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
		// The listener may have accepted a connection that raced with Close;
		// drop it instead of serving a client that will never be shut down.
		select {
		case <-b.closeCh:
			conn.Close()
			return
		case b.connSem <- struct{}{}:
		default:
			// Connection cap reached: refuse rather than spawn unbounded
			// goroutines and buffers.
			conn.Close()
			continue
		}
		b.trackConn(conn)
		b.wg.Add(1)
		go b.serveClient(conn)
	}
}

// trackConn registers a live client connection so Close can force it shut. A
// connection that races with Close is closed immediately instead of being left
// untracked (which would keep wg.Wait from returning).
func (b *MQTTBridge) trackConn(conn net.Conn) {
	select {
	case <-b.closeCh:
		conn.Close()
		return
	default:
	}
	b.connMu.Lock()
	b.conns[conn] = struct{}{}
	b.connMu.Unlock()
	select {
	case <-b.closeCh:
		conn.Close()
	default:
	}
}

// untrackConn removes a connection that has finished serving.
func (b *MQTTBridge) untrackConn(conn net.Conn) {
	b.connMu.Lock()
	delete(b.conns, conn)
	b.connMu.Unlock()
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
	// serveClient blocks on ReadByte; without closing every live connection an
	// idle client would keep wg.Wait from returning and stall broker shutdown.
	b.connMu.Lock()
	for conn := range b.conns {
		conn.Close()
	}
	b.connMu.Unlock()
	b.wg.Wait()
	return nil
}

// MQTT client session

type mqttClient struct {
	conn      net.Conn
	br        *bufio.Reader
	mu        sync.Mutex // serializes writes
	stop      chan struct{}
	principal string
	authed    bool

	// subs maps a Kafka topic to its shared forwarder; one loop per topic per
	// client, even when several MQTT filters map to the same Kafka topic.
	subMu sync.Mutex
	subs  map[string]*mqttForwarder
}

// mqttForwarder fans records from one Kafka topic out to the MQTT filters a
// client subscribed to.
type mqttForwarder struct {
	kafkaTopic string
	stop       chan struct{}
	mu         sync.Mutex
	filters    map[string]struct{}
}

func (f *mqttForwarder) add(filter string) {
	f.mu.Lock()
	f.filters[filter] = struct{}{}
	f.mu.Unlock()
}

func (f *mqttForwarder) remove(filter string) {
	f.mu.Lock()
	delete(f.filters, filter)
	f.mu.Unlock()
}

func (f *mqttForwarder) empty() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.filters) == 0
}

// matches reports whether any active filter accepts the original MQTT topic.
func (f *mqttForwarder) matches(topic string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for filter := range f.filters {
		if mqttTopicMatches(filter, topic) {
			return true
		}
	}
	return false
}

// addSubscription registers filter for kafkaTopic, reusing the existing
// forwarder when one is already running. It returns the forwarder, the start
// offset a new loop should resume from, and whether the caller must start it.
func (c *mqttClient) addSubscription(kafkaTopic, filter string, start int64) (*mqttForwarder, int64, bool) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	if fwd, ok := c.subs[kafkaTopic]; ok {
		fwd.add(filter)
		return fwd, 0, false
	}
	fwd := &mqttForwarder{
		kafkaTopic: kafkaTopic,
		stop:       make(chan struct{}),
		filters:    map[string]struct{}{filter: {}},
	}
	c.subs[kafkaTopic] = fwd
	return fwd, start, true
}

// removeSubscription drops filter; once no filters remain the forwarder is
// stopped, so UNSUBSCRIBE terminates the matching loop.
func (c *mqttClient) removeSubscription(kafkaTopic, filter string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	fwd, ok := c.subs[kafkaTopic]
	if !ok {
		return
	}
	fwd.remove(filter)
	if fwd.empty() {
		close(fwd.stop)
		delete(c.subs, kafkaTopic)
	}
}

// closeSubs stops every forwarder owned by the client.
func (c *mqttClient) closeSubs() {
	c.subMu.Lock()
	defer c.subMu.Unlock()
	for kt, fwd := range c.subs {
		close(fwd.stop)
		delete(c.subs, kt)
	}
}

func (b *MQTTBridge) serveClient(conn net.Conn) {
	defer b.wg.Done()
	defer conn.Close()
	defer b.untrackConn(conn)
	defer func() { <-b.connSem }()
	b.active.Add(1)
	defer b.active.Add(-1)
	addr := remoteHost(conn)
	if b.secure {
		if ok, _ := b.authLimiter.Allow(addr); !ok {
			// A source that keeps guessing is not even offered the handshake.
			return
		}
	}
	c := &mqttClient{
		conn:   conn,
		br:     bufio.NewReader(conn),
		stop:   make(chan struct{}),
		authed: !b.secure,
		subs:   make(map[string]*mqttForwarder),
	}
	defer close(c.stop)
	defer c.closeSubs()

	// Bound the unauthenticated handshake: a peer that never sends CONNECT must
	// not pin a goroutine and its pre-CONNECT buffer forever. The bound is
	// lifted once a CONNECT is processed (or, when security is off, on the
	// first packet).
	handshake := true
	maxLen := mqttMaxPreConnectBytes
	conn.SetReadDeadline(time.Now().Add(mqttHandshakeTimeout))

	for {
		first, payload, err := readMQTTPacket(c.br, maxLen)
		if err != nil {
			return
		}
		ptype := first >> 4
		flags := first & 0x0F
		if b.secure && !c.authed && ptype != mqttConnect {
			// No request is served before a successful CONNECT.
			return
		}
		if handshake && (ptype == mqttConnect || !b.secure) {
			handshake = false
			maxLen = mqttMaxPacketBytes
			conn.SetReadDeadline(time.Time{})
		}
		switch ptype {
		case mqttConnect:
			if b.secure {
				info, err := parseConnect(payload)
				if err != nil {
					c.writePacket(mqttConnack, []byte{0x00, 0x01}) // unacceptable protocol
					return
				}
				if b.requireTLS && !isTLSConn(conn) && (info.Username != "" || info.Password != "") {
					// The password travelled in cleartext; refuse it instead of
					// validating a credential the network already saw.
					audit.Log(audit.Event{
						Actor: audit.Anonymous, Action: "auth.mqtt", Resource: "mqtt",
						Result: audit.ResultDeny, Detail: "credentials over an unencrypted connection",
						RemoteAddr: addr,
					})
					c.writePacket(mqttConnack, []byte{0x00, 0x05})
					return
				}
				user, ok := b.authenticate(info)
				if !ok {
					delay := b.authLimiter.Fail(addr)
					audit.Log(audit.Event{
						Actor: info.Username, Action: "auth.mqtt", Resource: "mqtt",
						Result:     audit.ResultDeny,
						Detail:     fmt.Sprintf("invalid credentials backoff_ms=%d", delay.Milliseconds()),
						RemoteAddr: addr,
					})
					// 0x05 = not authorized.
					c.writePacket(mqttConnack, []byte{0x00, 0x05})
					return
				}
				b.authLimiter.Succeed(addr)
				c.principal = user
				c.authed = true
			}
			if err := c.writePacket(mqttConnack, []byte{0x00, 0x00}); err != nil {
				return
			}
		case mqttPublish:
			ptopic, data, qos, packetID, err := parsePublish(flags, payload)
			if err != nil {
				continue
			}
			if err := b.authorize(c.principal, authz.OpWrite, mqttTopicToKafka(ptopic)); err != nil {
				audit.Log(audit.Event{
					Actor: c.principal, Action: "mqtt.publish", Resource: ptopic,
					Result: audit.ResultDeny, Detail: err.Error(), RemoteAddr: addr,
				})
				continue
			}
			bridgeErr := b.bridgePublish(ptopic, data)
			if bridgeErr != nil {
				log.Printf("mqtt bridge publish: %v", bridgeErr)
			}
			// Acknowledge per requested QoS so a QoS 1/2 publisher is not left
			// waiting. Failed stores are not acked; the client retries.
			switch {
			case qos == 1 && bridgeErr == nil:
				if err := c.writePacket(mqttPuback, u16(packetID)); err != nil {
					return
				}
			case qos == 2 && bridgeErr == nil:
				if err := c.writePacket(mqttPubrec, u16(packetID)); err != nil {
					return
				}
			}
		case mqttPubrel:
			// Complete the inbound QoS 2 handshake.
			if len(payload) >= 2 {
				if err := c.writePacket(mqttPubcomp, payload[:2]); err != nil {
					return
				}
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
				if err := b.authorize(c.principal, authz.OpRead, mqttFilterToKafka(f)); err != nil {
					codes[i] = 0x80 // failure: not authorized
					audit.Log(audit.Event{
						Actor: c.principal, Action: "mqtt.subscribe", Resource: f,
						Result: audit.ResultDeny, Detail: err.Error(), RemoteAddr: addr,
					})
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
				part := b.store.GetPartition(kt, 0)
				if part == nil {
					// A principal with only Read must not implicitly create the
					// topic; require write/create permission before EnsureTopic.
					if b.authorize(c.principal, authz.OpWrite, kt) == nil {
						b.store.EnsureTopic(kt, 1)
						part = b.store.GetPartition(kt, 0)
					}
				}
				// Capture the current offset so the loop catches messages
				// published after subscribe.
				start := int64(0)
				if part != nil {
					start = part.HighWatermark()
				}
				fwd, startOff, created := c.addSubscription(kt, f, start)
				if created {
					go b.forwardLoop(c, fwd, startOff)
				}
			}
		case mqttUnsubscribe:
			packetID, filters, err := parseUnsubscribe(payload)
			if err != nil {
				continue
			}
			for _, f := range filters {
				c.removeSubscription(mqttFilterToKafka(f), f)
			}
			if err := c.writePacket(mqttUnsuback, u16(packetID)); err != nil {
				return
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
	// A stalled subscriber must not block the write mutex (and with it PINGRESP
	// and every forward loop) forever.
	c.conn.SetWriteDeadline(time.Now().Add(mqttWriteTimeout))
	_, err := c.conn.Write(buf)
	c.conn.SetWriteDeadline(time.Time{})
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
		Records: []protocol.Record{{
			Key:     []byte(key),
			Value:   data,
			Headers: []protocol.RecordHeader{{Key: MQTTTopicHeader, Value: []byte(mqttTopic)}},
		}},
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

// forwardLoop tails one Kafka topic and forwards matching records to the MQTT
// client. It waits on the partition's DataWaiter (with a fallback timer) so an
// idle subscription costs no polling, and advances next only past fully parsed
// batches so a large backlog is never skipped.
func (b *MQTTBridge) forwardLoop(c *mqttClient, fwd *mqttForwarder, next int64) {
	for {
		part := b.store.GetPartition(fwd.kafkaTopic, 0)
		if part == nil {
			if !b.waitForData(c, fwd, nil) {
				return
			}
			continue
		}
		ch, hwm := part.DataWaiter()
		if hwm <= next {
			if !b.waitForData(c, fwd, ch) {
				return
			}
			continue
		}
		before := next
		advanced, ok := b.forwardOnce(c, fwd, part, next)
		next = advanced
		if !ok {
			return
		}
		if next == before {
			// Nothing parsed (read error or an unparseable batch): back off
			// instead of spinning on the same bytes.
			if !b.waitForData(c, fwd, nil) {
				return
			}
		}
	}
}

// waitForData blocks until data may be available, the subscription is stopped,
// or the fallback timer fires. A nil ch (partition missing) relies on the timer.
func (b *MQTTBridge) waitForData(c *mqttClient, fwd *mqttForwarder, ch <-chan struct{}) bool {
	timer := time.NewTimer(mqttForwardFallback)
	defer timer.Stop()
	select {
	case <-c.stop:
		return false
	case <-b.closeCh:
		return false
	case <-fwd.stop:
		return false
	case <-ch:
		return true
	case <-timer.C:
		return true
	}
}

// forwardOnce forwards the readable backlog once, returning the next offset and
// false when a client write failed (the loop should stop).
func (b *MQTTBridge) forwardOnce(c *mqttClient, fwd *mqttForwarder, part *storage.Partition, next int64) (int64, bool) {
	raw, _, err := part.Read(next, 1<<20)
	if err != nil || len(raw) == 0 {
		return next, true
	}
	pos := 0
	for pos < len(raw) {
		h, err := storage.ParseRecordBatchHeader(raw[pos:])
		if err != nil {
			break
		}
		total := int(storage.BatchTotalSize(h))
		if total <= 0 || pos+total > len(raw) {
			break
		}
		batch, err := protocol.DecodeRecordBatch(raw[pos : pos+total])
		if err != nil {
			break
		}
		// The batch is fully parsed: advance past all of its offsets. We never
		// jump to the HWM, so records beyond a partial read are not lost.
		if end := batch.BaseOffset + int64(batch.LastOffsetDelta) + 1; end > next {
			next = end
		}
		for _, rec := range batch.Records {
			topic := recordMQTTTopic(rec, fwd.kafkaTopic)
			if !fwd.matches(topic) {
				continue
			}
			if err := c.writePacket(mqttPublish, mqttPublishPayload(topic, rec.Value)); err != nil {
				return next, false
			}
		}
		pos += total
	}
	return next, true
}

// recordMQTTTopic recovers the original MQTT topic carried in the record header,
// falling back to the Kafka topic mapping for records written without one.
func recordMQTTTopic(rec protocol.Record, kafkaTopic string) string {
	for _, h := range rec.Headers {
		if h.Key == MQTTTopicHeader {
			return string(h.Value)
		}
	}
	return kafkaTopicToMQTT(kafkaTopic)
}

// mqttTopicMatches reports whether an MQTT topic matches a subscription filter
// using the MQTT '+' (single level) and '#' (multi level) wildcards.
func mqttTopicMatches(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := range f {
		if f[i] == "#" {
			// '#' is valid only as the final level and matches the rest.
			return i == len(f)-1
		}
		if i >= len(t) {
			return false
		}
		if f[i] == "+" {
			continue
		}
		if f[i] != t[i] {
			return false
		}
	}
	return len(f) == len(t)
}

// MQTT packet parsing

// readMQTTPacket reads one MQTT packet, returning the full fixed-header byte
// (type in the high nibble, flags in the low nibble) and the payload. maxLen
// bounds the remaining length before allocation so a bogus header cannot make
// the bridge reserve up to 16 MB per connection pre-authentication.
func readMQTTPacket(br *bufio.Reader, maxLen int) (byte, []byte, error) {
	first, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	length, err := readRemainingLength(br)
	if err != nil {
		return 0, nil, err
	}
	if length < 0 || length > maxLen {
		return 0, nil, fmt.Errorf("invalid MQTT remaining length %d (max %d)", length, maxLen)
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	return first, payload, nil
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

// parsePublish extracts the topic, payload, QoS and packet identifier from a
// PUBLISH packet. flags is the low nibble of the fixed header. For QoS > 0 the
// 2-byte packet identifier is stripped from the payload.
func parsePublish(flags byte, payload []byte) (string, []byte, byte, uint16, error) {
	if len(payload) < 2 {
		return "", nil, 0, 0, fmt.Errorf("short publish")
	}
	qos := (flags >> 1) & 0x03
	if qos == 3 {
		return "", nil, 0, 0, fmt.Errorf("invalid publish qos")
	}
	tlen := int(binary.BigEndian.Uint16(payload[0:2]))
	if 2+tlen > len(payload) {
		return "", nil, 0, 0, fmt.Errorf("publish topic overrun")
	}
	topic := string(payload[2 : 2+tlen])
	data := payload[2+tlen:]
	var packetID uint16
	if qos > 0 {
		if len(data) < 2 {
			return "", nil, 0, 0, fmt.Errorf("short publish packet id")
		}
		packetID = binary.BigEndian.Uint16(data[0:2])
		data = data[2:]
	}
	return topic, data, qos, packetID, nil
}

// parseUnsubscribe extracts the packet ID and topic filters from an
// UNSUBSCRIBE payload.
func parseUnsubscribe(payload []byte) (uint16, []string, error) {
	if len(payload) < 4 {
		return 0, nil, fmt.Errorf("short unsubscribe")
	}
	packetID := binary.BigEndian.Uint16(payload[0:2])
	var filters []string
	pos := 2
	for pos+2 <= len(payload) {
		tlen := int(binary.BigEndian.Uint16(payload[pos : pos+2]))
		pos += 2
		if pos+tlen > len(payload) {
			break
		}
		filters = append(filters, string(payload[pos:pos+tlen]))
		pos += tlen
	}
	return packetID, filters, nil
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

// Topic mapping

// Documented mapping surfaced to the dashboard Integrations page. Kept next to
// mqttTopicToKafka/topicKey so the card text cannot drift from the code that
// performs the mapping.
const (
	MQTTTopicPattern      = "sensors/<device>/<metric>"
	MQTTKafkaTopicPattern = "mqtt-<group>"
)

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
