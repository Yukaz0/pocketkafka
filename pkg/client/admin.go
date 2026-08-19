package client

import (
	"sort"

	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// CreateTopic creates a topic with the given partition count.
func (c *KafkaClient) CreateTopic(name string, partitions int) error {
	req := &protocol.CreateTopicsRequest{
		Version:   c.version(protocol.APKCreateTopics),
		Topics:    []protocol.CreateTopicsRequestTopic{{Topic: name, NumPartitions: int32(partitions)}},
		TimeoutMs: 10000,
	}
	body, err := protocol.EncodeCreateTopicsRequest(req)
	if err != nil {
		return err
	}
	respBody, err := c.roundTrip(protocol.APKCreateTopics, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeCreateTopicsResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: resp.Topics[0].ErrorCode}
	}
	return nil
}

// DeleteTopic deletes a topic and its data.
func (c *KafkaClient) DeleteTopic(name string) error {
	req := &protocol.DeleteTopicsRequest{
		Version:    c.version(protocol.APKDeleteTopics),
		TopicNames: []string{name},
		TimeoutMs:  10000,
	}
	body, err := protocol.EncodeDeleteTopicsRequest(req)
	if err != nil {
		return err
	}
	respBody, err := c.roundTrip(protocol.APKDeleteTopics, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeDeleteTopicsResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	if len(resp.Topics) > 0 && resp.Topics[0].ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: resp.Topics[0].ErrorCode}
	}
	return nil
}

// ListTopics returns the names of all known topics.
func (c *KafkaClient) ListTopics() []string {
	if err := c.RefreshMetadata(); err != nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.metadata))
	for name := range c.metadata {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// QueryOffset returns the offset for a partition at the given timestamp
// (-1 latest, -2 earliest).
func (c *KafkaClient) QueryOffset(topic string, partition int32, ts int64) (int64, error) {
	req := &protocol.ListOffsetsRequest{
		Version:   c.version(protocol.APKListOffsets),
		ReplicaID: -1,
		Topics: []protocol.ListOffsetsRequestTopic{{
			Topic:      topic,
			Partitions: []protocol.ListOffsetsRequestPartition{{Partition: partition, Timestamp: ts}},
		}},
	}
	body, err := protocol.EncodeListOffsetsRequest(req)
	if err != nil {
		return -1, err
	}
	respBody, err := c.roundTrip(protocol.APKListOffsets, req.Version, body)
	if err != nil {
		return -1, err
	}
	resp, err := protocol.DecodeListOffsetsResponse(req.Version, respBody)
	if err != nil {
		return -1, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return -1, errNoResponse
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != protocol.ErrNone {
		return -1, &KafkaError{Code: p.ErrorCode}
	}
	return p.Offset, nil
}
