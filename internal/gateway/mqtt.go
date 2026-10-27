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
	"sort"
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

const (
	mqttMaxConnections     = 1024
	mqttMaxPreConnectBytes = 64 << 10
	mqttMaxPacketBytes     = 1 << 20
	mqttHandshakeTimeout   = 10 * time.Second
	mqttWriteTimeout       = 10 * time.Second
	mqttForwardFallback    = 500 * time.Millisecond
)

const MQTTMaxConnections = mqttMaxConnections

const MQTTTopicHeader = "mqtt-topic"

type MQTTBridge struct {
	store    *storage.Store
	listener net.Listener
	wg       sync.WaitGroup
	closeCh  chan struct{}

	connMu sync.Mutex
	conns  map[net.Conn]struct{}

	clientMu sync.RWMutex
	clients  map[string]*mqttClientEntry

	connSem chan struct{}

	active  atomic.Int32
	bridged atomic.Int64

	secure     bool
	users      []config.SecurityUser
	authorizer authz.Authorizer

	tlsConf *tls.Config

	requireTLS bool

	authLimiter *ratelimit.Limiter
}

func (b *MQTTBridge) WithSecurity(enabled bool, users []config.SecurityUser, a authz.Authorizer) *MQTTBridge {
	b.secure = enabled
	b.users = users

	if !enabled || a == nil {
		a = authz.AllowAllAuthorizer{}
	}
	b.authorizer = a
	return b
}

func (b *MQTTBridge) WithTLS(conf *tls.Config, requireTLS bool) *MQTTBridge {
	b.tlsConf = conf
	b.requireTLS = requireTLS
	return b
}

func (b *MQTTBridge) authorize(principal string, op authz.Operation, kafkaTopic string) error {
	if b.authorizer == nil {
		return nil
	}
	if principal == "" {
		principal = authz.AnonymPrincipal
	}
	return b.authorizer.Authorize(principal, op, authz.Resource{Type: authz.ResourceTopic, Name: kafkaTopic})
}

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

func (b *MQTTBridge) ActiveClients() int32 { return b.active.Load() }

func (b *MQTTBridge) BridgedCount() int64 { return b.bridged.Load() }

type MQTTClientInfo struct {
	ClientID    string    `json:"clientId"`
	RemoteAddr  string    `json:"remoteAddr"`
	ConnectedAt time.Time `json:"connectedAt"`
	SubsCount   int       `json:"subsCount"`
}

type mqttClientEntry struct {
	clientID    string
	remoteAddr  string
	connectedAt time.Time
	client      *mqttClient
}

func (b *MQTTBridge) registerClient(remoteAddr, clientID string, c *mqttClient) {
	b.clientMu.Lock()
	if b.clients == nil {
		b.clients = make(map[string]*mqttClientEntry)
	}
	b.clients[remoteAddr] = &mqttClientEntry{
		clientID:    clientID,
		remoteAddr:  remoteAddr,
		connectedAt: time.Now(),
		client:      c,
	}
	b.clientMu.Unlock()
}

func (b *MQTTBridge) unregisterClient(remoteAddr string) {
	b.clientMu.Lock()
	delete(b.clients, remoteAddr)
	b.clientMu.Unlock()
}

func (b *MQTTBridge) ClientsSnapshot() []MQTTClientInfo {
	b.clientMu.RLock()
	type item struct {
		info MQTTClientInfo
		c    *mqttClient
	}
	items := make([]item, 0, len(b.clients))
	for _, e := range b.clients {
		items = append(items, item{
			info: MQTTClientInfo{
				ClientID:    e.clientID,
				RemoteAddr:  e.remoteAddr,
				ConnectedAt: e.connectedAt,
			},
			c: e.client,
		})
	}
	b.clientMu.RUnlock()

	out := make([]MQTTClientInfo, len(items))
	for i, it := range items {
		subs := 0
		if it.c != nil {
			it.c.subMu.Lock()
			subs = len(it.c.subs)
			it.c.subMu.Unlock()
		}
		it.info.SubsCount = subs
		out[i] = it.info
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ConnectedAt.Equal(out[j].ConnectedAt) {
			return out[i].RemoteAddr < out[j].RemoteAddr
		}
		return out[i].ConnectedAt.Before(out[j].ConnectedAt)
	})
	return out
}

func (b *MQTTBridge) Publish(principal, mqttTopic string, data []byte) error {
	if mqttTopic == "" {
		return fmt.Errorf("mqtt topic is required")
	}
	kt := mqttTopicToKafka(mqttTopic)
	if err := b.authorize(principal, authz.OpWrite, kt); err != nil {
		return err
	}
	return b.bridgePublish(mqttTopic, data)
}

func MQTTTopicToKafka(mqttTopic string) string {
	return mqttTopicToKafka(mqttTopic)
}

func NewMQTTBridge(store *storage.Store) *MQTTBridge {
	return &MQTTBridge{
		store:       store,
		closeCh:     make(chan struct{}),
		conns:       make(map[net.Conn]struct{}),
		clients:     make(map[string]*mqttClientEntry),
		connSem:     make(chan struct{}, mqttMaxConnections),
		authLimiter: ratelimit.New(3, time.Second, 30*time.Second),
	}
}

func isTLSConn(conn net.Conn) bool {
	_, ok := conn.(*tls.Conn)
	return ok
}

func remoteHost(conn net.Conn) string {
	if ra := conn.RemoteAddr(); ra != nil {
		if host, _, err := net.SplitHostPort(ra.String()); err == nil {
			return host
		}
		return ra.String()
	}
	return ""
}

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

		select {
		case <-b.closeCh:
			conn.Close()
			return
		case b.connSem <- struct{}{}:
		default:

			conn.Close()
			continue
		}
		b.trackConn(conn)
		b.wg.Add(1)
		go b.serveClient(conn)
	}
}

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

func (b *MQTTBridge) untrackConn(conn net.Conn) {
	b.connMu.Lock()
	delete(b.conns, conn)
	b.connMu.Unlock()
}

func (b *MQTTBridge) Close() error {
	select {
	case <-b.closeCh:
	default:
		close(b.closeCh)
	}
	if b.listener != nil {
		b.listener.Close()
	}

	b.connMu.Lock()
	for conn := range b.conns {
		conn.Close()
	}
	b.connMu.Unlock()
	b.wg.Wait()
	return nil
}

type mqttClient struct {
	clientID  string
	conn      net.Conn
	br        *bufio.Reader
	mu        sync.Mutex
	stop      chan struct{}
	principal string
	authed    bool

	subMu sync.Mutex
	subs  map[string]*mqttForwarder
}

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
	remoteAddr := addr
	if ra := conn.RemoteAddr(); ra != nil {
		remoteAddr = ra.String()
	}
	if b.secure {
		if ok, _ := b.authLimiter.Allow(addr); !ok {

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

			return
		}
		if handshake && (ptype == mqttConnect || !b.secure) {
			handshake = false
			maxLen = mqttMaxPacketBytes
			conn.SetReadDeadline(time.Time{})
		}
		switch ptype {
		case mqttConnect:
			info, parseErr := parseConnect(payload)
			if b.secure {
				if parseErr != nil {
					c.writePacket(mqttConnack, []byte{0x00, 0x01})
					return
				}
				if b.requireTLS && !isTLSConn(conn) && (info.Username != "" || info.Password != "") {
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
					c.writePacket(mqttConnack, []byte{0x00, 0x05})
					return
				}
				b.authLimiter.Succeed(addr)
				c.principal = user
				c.authed = true
			}
			if parseErr == nil && info != nil {
				c.clientID = info.ClientID
			}
			if err := c.writePacket(mqttConnack, []byte{0x00, 0x00}); err != nil {
				return
			}
			b.registerClient(remoteAddr, c.clientID, c)
			defer b.unregisterClient(remoteAddr)
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

			codes := make([]byte, len(filters))
			allowed := make([]bool, len(filters))
			for i, f := range filters {
				if err := b.authorize(c.principal, authz.OpRead, mqttFilterToKafka(f)); err != nil {
					codes[i] = 0x80
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

					if b.authorize(c.principal, authz.OpWrite, kt) == nil {
						b.store.EnsureTopic(kt, 1)
						part = b.store.GetPartition(kt, 0)
					}
				}

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

func (c *mqttClient) writePacket(ptype byte, payload []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := []byte{ptype << 4}
	buf = appendRemainingLength(buf, len(payload))
	buf = append(buf, payload...)

	c.conn.SetWriteDeadline(time.Now().Add(mqttWriteTimeout))
	_, err := c.conn.Write(buf)
	c.conn.SetWriteDeadline(time.Time{})
	return err
}

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

			if !b.waitForData(c, fwd, nil) {
				return
			}
		}
	}
}

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

func recordMQTTTopic(rec protocol.Record, kafkaTopic string) string {
	for _, h := range rec.Headers {
		if h.Key == MQTTTopicHeader {
			return string(h.Value)
		}
	}
	return kafkaTopicToMQTT(kafkaTopic)
}

func mqttTopicMatches(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := range f {
		if f[i] == "#" {

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

type mqttConnectInfo struct {
	ClientID    string
	Username    string
	Password    string
	HasUsername bool
	HasPassword bool
}

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
	pos += 2

	info := &mqttConnectInfo{}
	clientID, n, err := readMQTTString(payload[pos:])
	if err != nil {
		return nil, err
	}
	info.ClientID = clientID
	pos += n

	if flags&0x04 != 0 {
		if _, n, err = readMQTTString(payload[pos:]); err != nil {
			return nil, err
		}
		pos += n
		if _, n, err = readMQTTString(payload[pos:]); err != nil {
			return nil, err
		}
		pos += n
	}
	if flags&0x80 != 0 {
		u, n, err := readMQTTString(payload[pos:])
		if err != nil {
			return nil, err
		}
		info.Username = u
		info.HasUsername = true
		pos += n
	}
	if flags&0x40 != 0 {
		p, _, err := readMQTTString(payload[pos:])
		if err != nil {
			return nil, err
		}
		info.Password = p
		info.HasPassword = true
	}
	return info, nil
}

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
		pos += tlen + 1
	}
	return packetID, filters, nil
}

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

const (
	MQTTTopicPattern      = "sensors/<device>/<metric>"
	MQTTKafkaTopicPattern = "mqtt-<group>"
)

func mqttTopicToKafka(mqttTopic string) string {
	seg := strings.SplitN(strings.TrimPrefix(mqttTopic, "/"), "/", 2)
	if len(seg) >= 1 && seg[0] != "" {
		return "mqtt-" + seg[0]
	}
	return "mqtt-inbound"
}

func topicKey(mqttTopic string) string {
	seg := strings.Split(strings.TrimPrefix(mqttTopic, "/"), "/")
	if len(seg) >= 2 {
		return seg[1]
	}
	return ""
}

func mqttFilterToKafka(filter string) string {

	f := strings.TrimSuffix(filter, "/#")
	f = strings.TrimSuffix(f, "/+")
	return mqttTopicToKafka(f)
}

func kafkaTopicToMQTT(kafkaTopic string) string {
	return strings.ReplaceAll(kafkaTopic, "-", "/")
}
