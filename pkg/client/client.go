package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// BrokerNode describes one broker and its cached connection.
type BrokerNode struct {
	NodeID int32
	Host   string
	Port   int32
	conn   net.Conn
}

// PartitionMeta describes one topic partition's leader.
type PartitionMeta struct {
	Topic       string
	PartitionID int32
	LeaderID    int32
}

// KafkaClient is a low-level client with a connection pool and metadata cache.
type KafkaClient struct {
	brokers     []string
	clientID    string
	mu          sync.Mutex
	conn        net.Conn
	corrID      int32
	metadata    map[string][]PartitionMeta
	timeout     time.Duration
	apiVersions map[int16]int16 // negotiated max version per key
}

// NewClient connects to the given brokers and refreshes cluster metadata.
func NewClient(brokers []string, clientID string) (*KafkaClient, error) {
	c := &KafkaClient{
		brokers:     brokers,
		clientID:    clientID,
		metadata:    make(map[string][]PartitionMeta),
		timeout:     10 * time.Second,
		apiVersions: defaultAPIVersions(),
	}
	if err := c.connect(); err != nil {
		return nil, err
	}
	if err := c.negotiate(); err != nil {
		return nil, err
	}
	if err := c.RefreshMetadata(); err != nil {
		return nil, err
	}
	return c, nil
}

func defaultAPIVersions() map[int16]int16 {
	out := make(map[int16]int16)
	for _, k := range []int16{
		protocol.APKProduce, protocol.APKFetch, protocol.APKListOffsets,
		protocol.APKMetadata, protocol.APKOffsetCommit, protocol.APKOffsetFetch,
		protocol.APKFindCoordinator, protocol.APKJoinGroup, protocol.APKHeartbeat,
		protocol.APKLeaveGroup, protocol.APKSyncGroup, protocol.APKApiVersions,
		protocol.APKCreateTopics, protocol.APKDeleteTopics,
		protocol.APKDescribeGroups, protocol.APKListGroups, protocol.APKDeleteGroups,
		protocol.APKInitProducerID, protocol.APKAddPartitionsToTxn,
		protocol.APKAddOffsetsToTxn, protocol.APKEndTxn,
	} {
		out[k] = protocol.MaxVersion(k)
	}
	return out
}

// version returns the negotiated version for an API key.
func (c *KafkaClient) version(key int16) int16 {
	if v, ok := c.apiVersions[key]; ok {
		return v
	}
	return 0
}

func (c *KafkaClient) connect() error {
	var lastErr error
	for _, addr := range c.brokers {
		conn, err := net.DialTimeout("tcp", addr, c.timeout)
		if err == nil {
			c.conn = conn
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("connect to brokers %v: %w", c.brokers, lastErr)
}

// negotiate requests ApiVersions to cap versions to what the broker supports.
func (c *KafkaClient) negotiate() error {
	respBody, err := c.roundTrip(protocol.APKApiVersions, 0, nil)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeApiVersionsResponse(0, respBody)
	if err != nil {
		return err
	}
	for _, k := range resp.ApiKeys {
		if cur, ok := c.apiVersions[k.ApiKey]; ok && k.MaxVersion < cur {
			c.apiVersions[k.ApiKey] = k.MaxVersion
		}
	}
	return nil
}

// roundTrip sends a request body and returns the response body (excluding the
// correlation ID). Requests on one client are serialized.
func (c *KafkaClient) roundTrip(apiKey, version int16, reqBody []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.corrID++
	corrID := c.corrID

	frame := make([]byte, 0, len(reqBody)+32)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(reqBody)+8+2+len(c.clientID)))
	frame = binary.BigEndian.AppendUint16(frame, uint16(apiKey))
	frame = binary.BigEndian.AppendUint16(frame, uint16(version))
	frame = binary.BigEndian.AppendUint32(frame, uint32(corrID))
	frame = binary.BigEndian.AppendUint16(frame, uint16(len(c.clientID)))
	frame = append(frame, c.clientID...)
	frame = append(frame, reqBody...)

	c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}

	c.conn.SetReadDeadline(time.Now().Add(c.timeout))
	var lenBuf [4]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if length < 4 {
		return nil, errors.New("invalid response length")
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		return nil, err
	}
	respCorr := int32(binary.BigEndian.Uint32(body[:4]))
	if respCorr != corrID {
		return nil, fmt.Errorf("correlation id mismatch: got %d want %d", respCorr, corrID)
	}
	return body[4:], nil
}

// RefreshMetadata updates the topic -> partition leader cache.
func (c *KafkaClient) RefreshMetadata() error {
	req := &protocol.MetadataRequest{
		Version:                c.version(protocol.APKMetadata),
		Topics:                 nil,
		AllowAutoTopicCreation: true,
	}
	body, err := protocol.EncodeMetadataRequest(req)
	if err != nil {
		return err
	}
	respBody, err := c.roundTrip(protocol.APKMetadata, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeMetadataResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.metadata = make(map[string][]PartitionMeta)
	for _, t := range resp.Topics {
		var parts []PartitionMeta
		for _, p := range t.Partitions {
			parts = append(parts, PartitionMeta{Topic: t.Name, PartitionID: p.Partition, LeaderID: p.Leader})
		}
		c.metadata[t.Name] = parts
	}
	c.mu.Unlock()
	return nil
}

// Partitions returns the cached partition metadata for a topic.
func (c *KafkaClient) Partitions(topic string) []PartitionMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.metadata[topic]
}

// Close closes the underlying connection.
func (c *KafkaClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// RoundTrip sends a raw request body for an API key/version and returns the
// raw response body (excluding the correlation ID). It is used by tests and
// advanced tooling that need direct protocol access.
func (c *KafkaClient) RoundTrip(apiKey, version int16, reqBody []byte) ([]byte, error) {
	return c.roundTrip(apiKey, version, reqBody)
}
