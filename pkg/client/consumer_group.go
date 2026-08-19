package client

import (
	"context"
	"time"

	"github.com/neu/go-kafka-neu/pkg/protocol"
)

// ConsumedMessage is a single record received by a consumer.
type ConsumedMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

// MessageHandler processes one consumed message.
type MessageHandler func(msg *ConsumedMessage) error

// ConsumerGroup is a high-level group consumer with automatic rebalance and
// optional offset auto-commit.
type ConsumerGroup struct {
	client     *KafkaClient
	cfg        ConsumerGroupConfig
	memberID   string
	generation int32
	topics     []string
	handler    MessageHandler

	assignment map[string][]int32         // topic -> assigned partitions
	positions  map[string]map[int32]int64 // topic -> partition -> next offset to fetch
	committed  map[string]map[int32]int64 // topic -> partition -> committed offset

	subTopics []string // explicit subscription, overrides metadata topics

	stopChan chan struct{}
}

// SetGroupID overrides the group ID.
func (cg *ConsumerGroup) SetGroupID(id string) { cg.cfg.GroupID = id }

// SetTopics sets the explicit topic subscription.
func (cg *ConsumerGroup) SetTopics(topics []string) { cg.subTopics = topics }

// NewConsumerGroup builds a consumer group that subscribes to the given topics.
func NewConsumerGroup(client *KafkaClient, cfg ConsumerGroupConfig, handler MessageHandler) *ConsumerGroup {
	if cfg.SessionTimeout == 0 {
		cfg.SessionTimeout = 30 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}
	if cfg.InitialOffset == 0 {
		cfg.InitialOffset = -1
	}
	return &ConsumerGroup{
		client:     client,
		cfg:        cfg,
		handler:    handler,
		assignment: make(map[string][]int32),
		positions:  make(map[string]map[int32]int64),
		committed:  make(map[string]map[int32]int64),
		stopChan:   make(chan struct{}),
	}
}

// Start runs the group consume loop until ctx is cancelled.
func (cg *ConsumerGroup) Start(ctx context.Context) error {
	if err := cg.findCoordinator(ctx); err != nil {
		return err
	}
	if err := cg.joinAndSync(ctx); err != nil {
		return err
	}
	if err := cg.initPositions(ctx); err != nil {
		return err
	}

	go cg.heartbeatLoop(ctx)
	go cg.autoCommitLoop(ctx)

	err := cg.consumeLoop(ctx)
	// Best effort final commit.
	cg.commit(ctx)
	return err
}

func (cg *ConsumerGroup) findCoordinator(ctx context.Context) error {
	req := &protocol.FindCoordinatorRequest{
		Version: cg.client.version(protocol.APKFindCoordinator),
		Key:     cg.cfg.GroupID,
		KeyType: 0,
	}
	body, err := protocol.EncodeFindCoordinatorRequest(req)
	if err != nil {
		return err
	}
	respBody, err := cg.client.roundTrip(protocol.APKFindCoordinator, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeFindCoordinatorResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	if resp.ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: resp.ErrorCode}
	}
	return nil
}

// joinAndSync performs the join -> (leader assign) -> sync handshake.
func (cg *ConsumerGroup) joinAndSync(ctx context.Context) error {
	// Determine the topic subscription: explicit if set, else all known topics.
	var topics []string
	if len(cg.subTopics) > 0 {
		topics = cg.subTopics
	} else {
		for name := range cg.client.metadata {
			topics = append(topics, name)
		}
	}
	cg.topics = topics

	subMeta := encodeSubscription(topics)
	joinReq := &protocol.JoinGroupRequest{
		Version:            cg.client.version(protocol.APKJoinGroup),
		Group:              cg.cfg.GroupID,
		SessionTimeoutMs:   int32(cg.cfg.SessionTimeout / time.Millisecond),
		RebalanceTimeoutMs: int32(cg.cfg.SessionTimeout / time.Millisecond),
		MemberID:           cg.memberID,
		ProtocolType:       "consumer",
		Protocols: []protocol.JoinGroupRequestProtocol{{
			Name:     "range",
			Metadata: subMeta,
		}},
	}
	body, err := protocol.EncodeJoinGroupRequest(joinReq)
	if err != nil {
		return err
	}
	respBody, err := cg.client.roundTrip(protocol.APKJoinGroup, joinReq.Version, body)
	if err != nil {
		return err
	}
	joinResp, err := protocol.DecodeJoinGroupResponse(joinReq.Version, respBody)
	if err != nil {
		return err
	}
	if joinResp.ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: joinResp.ErrorCode}
	}
	cg.memberID = joinResp.MemberID
	cg.generation = joinResp.GenerationID

	syncReq := &protocol.SyncGroupRequest{
		Version:      cg.client.version(protocol.APKSyncGroup),
		Group:        cg.cfg.GroupID,
		GenerationID: joinResp.GenerationID,
		MemberID:     cg.memberID,
	}

	if joinResp.LeaderID == cg.memberID {
		assignments := cg.computeAssignment(topics, joinResp.Members)
		for _, m := range assignments {
			syncReq.Assignments = append(syncReq.Assignments, protocol.SyncGroupRequestAssignment{
				MemberID:   m.MemberID,
				Assignment: encodeAssignment(m.TopicPartitions),
			})
		}
	}

	syncBody, err := protocol.EncodeSyncGroupRequest(syncReq)
	if err != nil {
		return err
	}
	syncRespBody, err := cg.client.roundTrip(protocol.APKSyncGroup, syncReq.Version, syncBody)
	if err != nil {
		return err
	}
	syncResp, err := protocol.DecodeSyncGroupResponse(syncReq.Version, syncRespBody)
	if err != nil {
		return err
	}
	if syncResp.ErrorCode != protocol.ErrNone {
		return &KafkaError{Code: syncResp.ErrorCode}
	}
	assignments, err := decodeAssignment(syncResp.Assignment)
	if err != nil {
		return err
	}
	cg.assignment = assignments
	return nil
}

// computeAssignment round-robins all partitions of all subscribed topics across
// the group members.
func (cg *ConsumerGroup) computeAssignment(topics []string, members []protocol.JoinGroupResponseMember) []memberAssignment {
	all := make([]string, 0) // "topic:partition"
	for _, t := range topics {
		for _, pm := range cg.client.Partitions(t) {
			all = append(all, topicPartition(t, pm.PartitionID))
		}
	}
	assign := make(map[string][]string, len(members))
	names := make([]string, 0, len(members))
	for _, m := range members {
		names = append(names, m.MemberID)
		assign[m.MemberID] = nil
	}
	for i, tp := range all {
		m := names[i%len(names)]
		assign[m] = append(assign[m], tp)
	}
	out := make([]memberAssignment, 0, len(members))
	for _, m := range members {
		out = append(out, memberAssignment{MemberID: m.MemberID, TopicPartitions: toMap(assign[m.MemberID])})
	}
	return out
}

type memberAssignment struct {
	MemberID        string
	TopicPartitions map[string][]int32
}

func toMap(tps []string) map[string][]int32 {
	out := make(map[string][]int32)
	for _, tp := range tps {
		t, p := splitTopicPartition(tp)
		out[t] = append(out[t], p)
	}
	return out
}

// initPositions determines the start offset for each assigned partition.
func (cg *ConsumerGroup) initPositions(ctx context.Context) error {
	var topics []protocol.ListOffsetsRequestTopic
	for t, parts := range cg.assignment {
		var plist []protocol.ListOffsetsRequestPartition
		for _, p := range parts {
			plist = append(plist, protocol.ListOffsetsRequestPartition{Partition: p, Timestamp: cg.cfg.InitialOffset})
		}
		topics = append(topics, protocol.ListOffsetsRequestTopic{Topic: t, Partitions: plist})
	}
	req := &protocol.ListOffsetsRequest{
		Version:   cg.client.version(protocol.APKListOffsets),
		ReplicaID: -1,
		Topics:    topics,
	}
	body, err := protocol.EncodeListOffsetsRequest(req)
	if err != nil {
		return err
	}
	respBody, err := cg.client.roundTrip(protocol.APKListOffsets, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeListOffsetsResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			cg.setPosition(t.Topic, p.Partition, p.Offset)
		}
	}
	return nil
}

func (cg *ConsumerGroup) setPosition(topic string, partition int32, offset int64) {
	if cg.positions[topic] == nil {
		cg.positions[topic] = make(map[int32]int64)
	}
	cg.positions[topic][partition] = offset
}

// consumeLoop fetches and processes records for all assigned partitions.
func (cg *ConsumerGroup) consumeLoop(ctx context.Context) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-cg.stopChan:
			return nil
		case <-ticker.C:
			if err := cg.fetchOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (cg *ConsumerGroup) fetchOnce(ctx context.Context) error {
	var topics []protocol.FetchRequestTopic
	for t, parts := range cg.assignment {
		var plist []protocol.FetchRequestPartition
		for _, p := range parts {
			off := cg.positions[t][p]
			plist = append(plist, protocol.FetchRequestPartition{
				Partition:         p,
				FetchOffset:       off,
				PartitionMaxBytes: cg.cfg.FetchMaxBytes,
			})
		}
		topics = append(topics, protocol.FetchRequestTopic{Topic: t, Partitions: plist})
	}
	if len(topics) == 0 {
		return nil
	}
	req := &protocol.FetchRequest{
		Version:        cg.client.version(protocol.APKFetch),
		ReplicaID:      -1,
		MaxWaitMillis:  cg.cfg.FetchMaxWait,
		MinBytes:       1,
		MaxBytes:       cg.cfg.FetchMaxBytes,
		IsolationLevel: 0,
		Topics:         topics,
	}
	body, err := protocol.EncodeFetchRequest(req)
	if err != nil {
		return err
	}
	respBody, err := cg.client.roundTrip(protocol.APKFetch, req.Version, body)
	if err != nil {
		return err
	}
	resp, err := protocol.DecodeFetchResponse(req.Version, respBody)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if len(p.Records) == 0 {
				continue
			}
			batch, err := protocol.DecodeRecordBatch(p.Records)
			if err != nil {
				continue
			}
			for _, rec := range batch.Records {
				off := batch.BaseOffset + int64(rec.OffsetDelta)
				msg := &ConsumedMessage{
					Topic:     t.Topic,
					Partition: p.Partition,
					Offset:    off,
					Key:       rec.Key,
					Value:     rec.Value,
					Headers:   map[string][]byte{},
				}
				for _, h := range rec.Headers {
					msg.Headers[h.Key] = h.Value
				}
				if err := cg.handler(msg); err != nil {
					return err
				}
			}
			cg.setPosition(t.Topic, p.Partition, batch.BaseOffset+int64(batch.LastOffsetDelta)+1)
		}
	}
	return nil
}

func (cg *ConsumerGroup) commit(ctx context.Context) error {
	req := &protocol.OffsetCommitRequest{
		Version:    cg.client.version(protocol.APKOffsetCommit),
		Group:      cg.cfg.GroupID,
		Generation: cg.generation,
		MemberID:   cg.memberID,
	}
	for t, parts := range cg.positions {
		var plist []protocol.OffsetCommitRequestPartition
		for p, off := range parts {
			plist = append(plist, protocol.OffsetCommitRequestPartition{Partition: p, Offset: off})
		}
		req.Topics = append(req.Topics, protocol.OffsetCommitRequestTopic{Topic: t, Partitions: plist})
	}
	body, err := protocol.EncodeOffsetCommitRequest(req)
	if err != nil {
		return err
	}
	_, err = cg.client.roundTrip(protocol.APKOffsetCommit, req.Version, body)
	return err
}

func (cg *ConsumerGroup) autoCommitLoop(ctx context.Context) {
	if !cg.cfg.AutoCommit || cg.cfg.AutoCommitInterval <= 0 {
		return
	}
	t := time.NewTicker(cg.cfg.AutoCommitInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cg.commit(ctx)
		}
	}
}

func (cg *ConsumerGroup) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(cg.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			req := &protocol.HeartbeatRequest{
				Version:      cg.client.version(protocol.APKHeartbeat),
				Group:        cg.cfg.GroupID,
				GenerationID: cg.generation,
				MemberID:     cg.memberID,
			}
			body, err := protocol.EncodeHeartbeatRequest(req)
			if err != nil {
				continue
			}
			cg.client.roundTrip(protocol.APKHeartbeat, req.Version, body)
		}
	}
}

// Close stops the consumer group.
func (cg *ConsumerGroup) Close() {
	select {
	case <-cg.stopChan:
	default:
		close(cg.stopChan)
	}
}

// ---------------------------------------------------------------------------
// ConsumerProtocol subscription / assignment codecs (classic encoding).
// ---------------------------------------------------------------------------

func encodeSubscription(topics []string) []byte {
	w := protocol.NewWriter(32)
	w.WriteArrayLen(len(topics))
	for _, t := range topics {
		w.WriteString(t)
	}
	w.WriteBytes(nil) // user_data
	return w.Bytes()
}

func encodeAssignment(assign map[string][]int32) []byte {
	w := protocol.NewWriter(32)
	w.WriteArrayLen(len(assign))
	for t, parts := range assign {
		w.WriteString(t)
		w.WriteArrayLen(len(parts))
		for _, p := range parts {
			w.WriteInt32(p)
		}
	}
	w.WriteBytes(nil) // user_data
	return w.Bytes()
}

func decodeAssignment(b []byte) (map[string][]int32, error) {
	if b == nil {
		return map[string][]int32{}, nil
	}
	r := protocol.NewReader(b)
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]int32)
	for i := 0; i < n; i++ {
		topic, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		var parts []int32
		for j := 0; j < pn; j++ {
			p, err := r.ReadInt32()
			if err != nil {
				return nil, err
			}
			parts = append(parts, p)
		}
		out[topic] = parts
	}
	return out, nil
}

func topicPartition(topic string, p int32) string {
	return topic + "\x00" + itoa(int64(p))
}

func splitTopicPartition(tp string) (string, int32) {
	for i := 0; i < len(tp); i++ {
		if tp[i] == 0 {
			t := tp[:i]
			p := parseInt(tp[i+1:])
			return t, p
		}
	}
	return tp, 0
}

func parseInt(s string) int32 {
	var v int32
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		v = v*10 + int32(s[i]-'0')
	}
	return v
}
