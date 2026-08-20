package client

import (
	"sort"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
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

// GroupInfo is the client-side view of one consumer group.
type GroupInfo struct {
	GroupID      string
	ProtocolType string
	State        string
	Members      []string
	Offsets      map[string]map[int32]int64
}

// ListGroups returns all consumer groups known to the broker (Key 16).
func (c *KafkaClient) ListGroups() ([]string, error) {
	req := &protocol.ListGroupsRequest{Version: c.version(protocol.APKListGroups)}
	body, err := protocol.EncodeListGroupsRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := c.roundTrip(protocol.APKListGroups, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeListGroupsResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != protocol.ErrNone {
		return nil, &KafkaError{Code: resp.ErrorCode}
	}
	names := make([]string, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		names = append(names, g.GroupID)
	}
	return names, nil
}

// DescribeGroup returns detailed state for one group (Key 15).
func (c *KafkaClient) DescribeGroup(group string) (*GroupInfo, error) {
	req := &protocol.DescribeGroupsRequest{
		Version:  c.version(protocol.APKDescribeGroups),
		GroupIDs: []string{group},
	}
	body, err := protocol.EncodeDescribeGroupsRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := c.roundTrip(protocol.APKDescribeGroups, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeDescribeGroupsResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	if len(resp.Groups) == 0 {
		return nil, errNoResponse
	}
	g := resp.Groups[0]
	if g.ErrorCode != protocol.ErrNone {
		return nil, &KafkaError{Code: g.ErrorCode}
	}
	info := &GroupInfo{
		GroupID: g.GroupID,
		State:   g.State,
	}
	for _, m := range g.Members {
		info.Members = append(info.Members, m.MemberID)
	}
	return info, nil
}

// DeleteGroup deletes a consumer group if it is empty (Key 42).
func (c *KafkaClient) DeleteGroup(group string) error {
	req := &protocol.DeleteGroupsRequest{
		Version:  c.version(protocol.APKDeleteGroups),
		GroupIDs: []string{group},
	}
	body, err := protocol.EncodeDeleteGroupsRequest(req)
	if err != nil {
		return err
	}
	respBody, err := c.roundTrip(protocol.APKDeleteGroups, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeDeleteGroupsResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	if len(resp.Groups) > 0 && resp.Groups[0].ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: resp.Groups[0].ErrorCode}
	}
	return nil
}

// ResetOffset commits a new offset for a group/topic/partition (Key 8). It is
// the CLI/admin backend for `pkctl offset reset`.
func (c *KafkaClient) ResetOffset(group, topic string, partition int32, offset int64) error {
	req := &protocol.OffsetCommitRequest{
		Version: c.version(protocol.APKOffsetCommit),
		Group:   group,
		Topics: []protocol.OffsetCommitRequestTopic{{
			Topic: topic,
			Partitions: []protocol.OffsetCommitRequestPartition{{
				Partition: partition,
				Offset:    offset,
			}},
		}},
	}
	body, err := protocol.EncodeOffsetCommitRequest(req)
	if err != nil {
		return err
	}
	respBody, err := c.roundTrip(protocol.APKOffsetCommit, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeOffsetCommitResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 &&
		resp.Topics[0].Partitions[0].ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: resp.Topics[0].Partitions[0].ErrorCode}
	}
	return nil
}

// CommittedOffset fetches the committed offset for a group/topic/partition
// (Key 9).
func (c *KafkaClient) CommittedOffset(group, topic string, partition int32) (int64, error) {
	req := &protocol.OffsetFetchRequest{
		Version: c.version(protocol.APKOffsetFetch),
		Group:   group,
		Topics: []protocol.OffsetFetchRequestTopic{{
			Topic:      topic,
			Partitions: []int32{partition},
		}},
	}
	body, err := protocol.EncodeOffsetFetchRequest(req)
	if err != nil {
		return -1, err
	}
	respBody, err := c.roundTrip(protocol.APKOffsetFetch, req.Version, body)
	if err != nil {
		return -1, err
	}
	resp, err := protocol.DecodeOffsetFetchResponse(req.Version, respBody)
	if err != nil {
		return -1, err
	}
	if len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		return resp.Topics[0].Partitions[0].Offset, nil
	}
	return -1, errNoResponse
}
