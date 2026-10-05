package client

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// Kafka error codes this file routes around.
const (
	errNotLeaderOrFollower int16 = 6
	errNotCoordinator      int16 = 16
	errFencedLeaderEpoch   int16 = 74
)

// Broker is one node of a cluster, as advertised in metadata.
type Broker struct {
	ID   int32
	Host string
	Port int32
}

// Addr is the dialable address of the broker.
func (b Broker) Addr() string { return net.JoinHostPort(b.Host, strconv.Itoa(int(b.Port))) }

// PartitionInfo is one partition's topology and whether its replicas are in sync.
type PartitionInfo struct {
	ID          int32
	Leader      int32
	LeaderEpoch int32
	Replicas    []int32
	ISR         []int32
	Offline     []int32
	Error       int16
}

// TopicInfo is one topic as the cluster reports it.
type TopicInfo struct {
	Name       string
	Internal   bool
	Partitions []PartitionInfo
}

// ClusterClient talks to every broker of a cluster. Authentication is per
// connection: a SASL cluster has no session spanning brokers, so a connection
// pool without credentials only works for the one broker it was opened to, and
// every request routed by leader or coordinator needs its own authenticated
// connection.
type ClusterClient struct {
	clientID string
	opts     Options

	mu         sync.Mutex
	seeds      []string
	clusterID  *string
	controller int32
	brokers    map[int32]Broker
	topics     []TopicInfo
	conns      map[int32]*KafkaClient
}

// NewClusterClient connects to a seed, authenticates, and reads the topology.
func NewClusterClient(seeds []string, clientID string, opts Options) (*ClusterClient, error) {
	if len(seeds) == 0 {
		return nil, fmt.Errorf("no brokers given")
	}
	cc := &ClusterClient{
		clientID: clientID,
		opts:     opts,
		seeds:    seeds,
		brokers:  make(map[int32]Broker),
		conns:    make(map[int32]*KafkaClient),
	}
	if err := cc.Refresh(); err != nil {
		cc.Close()
		return nil, err
	}
	return cc, nil
}

// Refresh re-reads the cluster topology.
func (cc *ClusterClient) Refresh() error {
	cl, err := cc.any()
	if err != nil {
		return err
	}
	resp, err := cl.fetchMetadata()
	if err != nil {
		return err
	}
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.clusterID = resp.ClusterID
	cc.controller = resp.ControllerID
	brokers := make(map[int32]Broker, len(resp.Brokers))
	for _, b := range resp.Brokers {
		brokers[b.NodeID] = Broker{ID: b.NodeID, Host: b.Host, Port: b.Port}
	}
	cc.brokers = brokers
	topics := make([]TopicInfo, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		info := TopicInfo{Name: t.Name, Internal: t.IsInternal}
		for _, p := range t.Partitions {
			info.Partitions = append(info.Partitions, PartitionInfo{
				ID:          p.Partition,
				Leader:      p.Leader,
				LeaderEpoch: p.LeaderEpoch,
				Replicas:    p.Replicas,
				ISR:         p.ISR,
				Offline:     p.OfflineReplicas,
				Error:       p.ErrorCode,
			})
		}
		sort.Slice(info.Partitions, func(i, j int) bool { return info.Partitions[i].ID < info.Partitions[j].ID })
		topics = append(topics, info)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	cc.topics = topics
	return nil
}

// ClusterID returns the cluster's id, if the brokers report one.
func (cc *ClusterClient) ClusterID() string {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.clusterID == nil {
		return ""
	}
	return *cc.clusterID
}

// Controller returns the controller broker id, or -1.
func (cc *ClusterClient) Controller() int32 {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.controller
}

// Brokers returns the known brokers, sorted by id.
func (cc *ClusterClient) Brokers() []Broker {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	out := make([]Broker, 0, len(cc.brokers))
	for _, b := range cc.brokers {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Topics returns the topology read by the last Refresh.
func (cc *ClusterClient) Topics() []TopicInfo {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return append([]TopicInfo(nil), cc.topics...)
}

// Close closes every connection this client opened.
func (cc *ClusterClient) Close() error {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	var firstErr error
	for id, cl := range cc.conns {
		if err := cl.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(cc.conns, id)
	}
	return firstErr
}

// seedConnID keys the connection opened from the configured seed list, so it is
// reused and closed with the rest instead of leaking one per call.
const seedConnID int32 = -1

// any returns a connection to any broker, opening one to a seed if needed.
func (cc *ClusterClient) any() (*KafkaClient, error) {
	cc.mu.Lock()
	if cl, ok := cc.conns[seedConnID]; ok {
		cc.mu.Unlock()
		return cl, nil
	}
	for _, cl := range cc.conns {
		cc.mu.Unlock()
		return cl, nil // any live connection serves a cluster-wide request
	}
	seeds := append([]string(nil), cc.seeds...)
	cc.mu.Unlock()

	cl, err := NewClientWithOptions(seeds, cc.clientID, cc.opts)
	if err != nil {
		return nil, err
	}
	cc.mu.Lock()
	cc.conns[seedConnID] = cl
	cc.mu.Unlock()
	return cl, nil
}

// connFor returns the cached client of a broker, connecting when needed.
func (cc *ClusterClient) connFor(id int32, cached *KafkaClient) (*KafkaClient, error) {
	if cached != nil {
		return cached, nil
	}
	cc.mu.Lock()
	addr, ok := cc.brokers[id]
	cc.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown broker id %d", id)
	}
	cl, err := NewClientWithOptions([]string{addr.Addr()}, cc.clientID, cc.opts)
	if err != nil {
		return nil, fmt.Errorf("broker %d (%s): %w", id, addr.Addr(), err)
	}
	cc.mu.Lock()
	if existing, ok := cc.conns[id]; ok {
		cc.mu.Unlock()
		cl.Close()
		return existing, nil
	}
	cc.conns[id] = cl
	cc.mu.Unlock()
	return cl, nil
}

// brokerFor returns the authenticated client of one broker.
func (cc *ClusterClient) brokerFor(id int32) (*KafkaClient, error) {
	cc.mu.Lock()
	cached := cc.conns[id]
	cc.mu.Unlock()
	return cc.connFor(id, cached)
}

// ListEndOffsets returns each partition's log end offset, batched per leader.
func (cc *ClusterClient) ListEndOffsets(topic string) (map[int32]int64, error) {
	return cc.listOffsets(topic, -1)
}

// ListStartOffsets returns each partition's earliest retained offset.
func (cc *ClusterClient) ListStartOffsets(topic string) (map[int32]int64, error) {
	return cc.listOffsets(topic, -2)
}

// FetchedRecord is one record read from a partition, with the timestamp and
// headers the record batch carries.
type FetchedRecord struct {
	Offset    int64
	Timestamp int64
	Key       []byte
	Value     []byte
	Headers   []protocol.RecordHeader
}

// FetchRecords reads from topic/partition starting at offset, bounded by
// maxBytes, and returns the records plus the offset to fetch next. A short read
// means the log end was reached when the broker answered. The request goes to
// the partition leader: a broker that does not lead the partition answers
// NOT_LEADER, it does not proxy.
func (cc *ClusterClient) FetchRecords(topic string, partition int32, offset int64, maxBytes int) ([]FetchedRecord, int64, error) {
	leader, ok := cc.leaderFor(topic, partition)
	if !ok {
		return nil, offset, fmt.Errorf("unknown partition %s/%d", topic, partition)
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	records, next, err := cc.fetchFrom(leader, topic, partition, offset, maxBytes)
	if err == nil {
		return records, next, nil
	}
	// One refresh lets a moved leadership take effect; a second failure is the
	// broker's answer and ends the read.
	if rerr := cc.Refresh(); rerr != nil {
		return nil, offset, err
	}
	moved, ok := cc.leaderFor(topic, partition)
	if !ok || moved == leader {
		return nil, offset, err
	}
	records, next, err = cc.fetchFrom(moved, topic, partition, offset, maxBytes)
	if err != nil {
		return nil, offset, err
	}
	return records, next, nil
}

// leaderFor resolves the current leader of one partition from cached metadata.
func (cc *ClusterClient) leaderFor(topic string, partition int32) (int32, bool) {
	info, ok := cc.topicInfo(topic)
	if !ok {
		return 0, false
	}
	for _, p := range info.Partitions {
		if p.ID == partition {
			return p.Leader, true
		}
	}
	return 0, false
}

func (cc *ClusterClient) fetchFrom(brokerID int32, topic string, partition int32, offset int64, maxBytes int) ([]FetchedRecord, int64, error) {
	cl, err := cc.brokerFor(brokerID)
	if err != nil {
		return nil, offset, err
	}
	req := &protocol.FetchRequest{
		Version:        cl.version(protocol.APKFetch),
		ReplicaID:      -1,
		MaxWaitMillis:  0,
		MinBytes:       0,
		MaxBytes:       int32(maxBytes),
		IsolationLevel: 0,
		Topics: []protocol.FetchRequestTopic{{
			Topic: topic,
			Partitions: []protocol.FetchRequestPartition{{
				Partition:         partition,
				FetchOffset:       offset,
				PartitionMaxBytes: int32(maxBytes),
			}},
		}},
	}
	body, err := protocol.EncodeFetchRequest(req)
	if err != nil {
		return nil, offset, err
	}
	respBody, err := cl.RoundTrip(protocol.APKFetch, req.Version, body)
	if err != nil {
		return nil, offset, err
	}
	resp, err := protocol.DecodeFetchResponse(req.Version, respBody)
	if err != nil {
		return nil, offset, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return nil, offset, nil
	}
	p := resp.Topics[0].Partitions[0]
	if p.ErrorCode != protocol.ErrNone {
		return nil, offset, fmt.Errorf("fetch %s/%d on broker %d: kafka error code %d", topic, partition, brokerID, p.ErrorCode)
	}
	if len(p.Records) == 0 {
		return nil, p.HighWatermark, nil
	}
	batch, err := protocol.DecodeRecordBatch(p.Records)
	if err != nil {
		return nil, offset, err
	}
	out := make([]FetchedRecord, 0, len(batch.Records))
	for _, rec := range batch.Records {
		abs := batch.BaseOffset + int64(rec.OffsetDelta)
		if abs < offset {
			continue
		}
		out = append(out, FetchedRecord{
			Offset:    abs,
			Timestamp: batch.BaseTimestamp + rec.TimestampDelta,
			Key:       rec.Key,
			Value:     rec.Value,
			Headers:   rec.Headers,
		})
	}
	next := offset
	if len(out) > 0 {
		next = out[len(out)-1].Offset + 1
	}
	return out, next, nil
}

// listOffsets reads one timestamp position (-1 latest, -2 earliest) for every
// partition of a topic, sending each request to the partition's leader: a broker
// that does not lead the partition answers NOT_LEADER, it does not proxy. The
// request carries the leader epoch from metadata, or -1 for unknown: sending 0
// claims a specific epoch and a real broker answers FENCED_LEADER_EPOCH.
func (cc *ClusterClient) listOffsets(topic string, ts int64) (map[int32]int64, error) {
	info, ok := cc.topicInfo(topic)
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topic)
	}
	byLeader := make(map[int32][]listOffsetPart)
	epochs := make(map[int32]int32, len(info.Partitions))
	for _, p := range info.Partitions {
		byLeader[p.Leader] = append(byLeader[p.Leader], listOffsetPart{id: p.ID, epoch: p.LeaderEpoch})
	}
	out := make(map[int32]int64, len(info.Partitions))
	for leader, parts := range byLeader {
		offs, err := cc.listOffsetsFrom(leader, topic, parts, ts)
		if err != nil {
			// One refresh makes a moved leadership take effect, the second
			// failure is the broker's answer.
			if rerr := cc.Refresh(); rerr != nil {
				return nil, err
			}
			refreshed, ok := cc.topicInfo(topic)
			if !ok {
				return nil, err
			}
			for _, p := range refreshed.Partitions {
				epochs[p.ID] = p.LeaderEpoch
			}
			for i := range parts {
				if e, ok := epochs[parts[i].id]; ok {
					parts[i].epoch = e
				}
			}
			offs, err = cc.listOffsetsFrom(leader, topic, parts, ts)
			if err != nil {
				return nil, err
			}
		}
		for pid, off := range offs {
			out[pid] = off
		}
	}
	return out, nil
}

// listOffsetPart is one partition with the leader epoch the request must declare.
type listOffsetPart struct {
	id    int32
	epoch int32
}

func (cc *ClusterClient) listOffsetsFrom(brokerID int32, topic string, partitions []listOffsetPart, ts int64) (map[int32]int64, error) {
	cl, err := cc.brokerFor(brokerID)
	if err != nil {
		return nil, err
	}
	req := &protocol.ListOffsetsRequest{
		Version:   cl.version(protocol.APKListOffsets),
		ReplicaID: -1,
		Topics: []protocol.ListOffsetsRequestTopic{{
			Topic: topic,
		}},
	}
	for _, p := range partitions {
		epoch := p.epoch
		if epoch < 0 {
			epoch = -1
		}
		req.Topics[0].Partitions = append(req.Topics[0].Partitions,
			protocol.ListOffsetsRequestPartition{Partition: p.id, CurrentLeaderEpoch: epoch, Timestamp: ts})
	}
	body, err := protocol.EncodeListOffsetsRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := cl.RoundTrip(protocol.APKListOffsets, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeListOffsetsResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(partitions))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != protocol.ErrNone {
				return nil, fmt.Errorf("list offsets %s/%d on broker %d: kafka error code %d",
					topic, p.Partition, brokerID, p.ErrorCode)
			}
			out[p.Partition] = p.Offset
		}
	}
	return out, nil
}

// ListOffsetsBulk reads one timestamp position (-1 latest, -2 earliest) for
// every partition of every topic in a single request per broker. One request per
// topic per leader is what makes a 75-topic cluster take minutes instead of a
// second: leadership is spread over the brokers, so batching by broker is the
// difference between a pass that finishes and one that never does.
func (cc *ClusterClient) ListOffsetsBulk(ts int64) (map[string]map[int32]int64, error) {
	topics := cc.Topics()
	byLeader := make(map[int32]map[string][]listOffsetPart)
	for _, t := range topics {
		if t.Internal {
			continue
		}
		for _, p := range t.Partitions {
			if byLeader[p.Leader] == nil {
				byLeader[p.Leader] = make(map[string][]listOffsetPart)
			}
			byLeader[p.Leader][t.Name] = append(byLeader[p.Leader][t.Name],
				listOffsetPart{id: p.ID, epoch: p.LeaderEpoch})
		}
	}
	out := make(map[string]map[int32]int64, len(topics))
	for leader, perTopic := range byLeader {
		res, err := cc.listOffsetsBulkFrom(leader, perTopic, ts)
		if err != nil {
			// One refresh lets a moved leadership take effect; a second failure
			// is the broker's answer and ends the pass.
			if rerr := cc.Refresh(); rerr != nil {
				return nil, err
			}
			perTopic = cc.reEpoch(perTopic)
			res, err = cc.listOffsetsBulkFrom(leader, perTopic, ts)
			if err != nil {
				return nil, err
			}
		}
		for topic, parts := range res {
			if out[topic] == nil {
				out[topic] = make(map[int32]int64, len(parts))
			}
			for pid, off := range parts {
				out[topic][pid] = off
			}
		}
	}
	return out, nil
}

// reEpoch rebuilds a per-topic request plan with the epochs from a fresh
// metadata read, which is what a FENCED_LEADER_EPOCH answer asks for.
func (cc *ClusterClient) reEpoch(perTopic map[string][]listOffsetPart) map[string][]listOffsetPart {
	out := make(map[string][]listOffsetPart, len(perTopic))
	for topic, parts := range perTopic {
		info, ok := cc.topicInfo(topic)
		if !ok {
			out[topic] = parts
			continue
		}
		epochs := make(map[int32]int32, len(info.Partitions))
		for _, p := range info.Partitions {
			epochs[p.ID] = p.LeaderEpoch
		}
		updated := make([]listOffsetPart, 0, len(parts))
		for _, p := range parts {
			if e, ok := epochs[p.id]; ok {
				p.epoch = e
			}
			updated = append(updated, p)
		}
		out[topic] = updated
	}
	return out
}

// listOffsetsBulkFrom sends one request carrying every topic this broker leads.
func (cc *ClusterClient) listOffsetsBulkFrom(brokerID int32, perTopic map[string][]listOffsetPart, ts int64) (map[string]map[int32]int64, error) {
	cl, err := cc.brokerFor(brokerID)
	if err != nil {
		return nil, err
	}
	req := &protocol.ListOffsetsRequest{
		Version:   cl.version(protocol.APKListOffsets),
		ReplicaID: -1,
	}
	names := make([]string, 0, len(perTopic))
	for name := range perTopic {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entry := protocol.ListOffsetsRequestTopic{Topic: name}
		for _, p := range perTopic[name] {
			epoch := p.epoch
			if epoch < 0 {
				epoch = -1
			}
			entry.Partitions = append(entry.Partitions,
				protocol.ListOffsetsRequestPartition{Partition: p.id, CurrentLeaderEpoch: epoch, Timestamp: ts})
		}
		req.Topics = append(req.Topics, entry)
	}
	body, err := protocol.EncodeListOffsetsRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := cl.RoundTrip(protocol.APKListOffsets, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeListOffsetsResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[int32]int64, len(resp.Topics))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != protocol.ErrNone {
				return nil, fmt.Errorf("list offsets %s/%d on broker %d: kafka error code %d",
					t.Topic, p.Partition, brokerID, p.ErrorCode)
			}
			if out[t.Topic] == nil {
				out[t.Topic] = make(map[int32]int64)
			}
			out[t.Topic][p.Partition] = p.Offset
		}
	}
	return out, nil
}

// DescribeGroupsAll asks every broker for the given groups in one request each
// and merges what came back. A broker answers NOT_COORDINATOR for groups it does
// not coordinate, which is the filter, so this needs no FindCoordinator per
// group.
func (cc *ClusterClient) DescribeGroupsAll(groups []string) (map[string]GroupState, error) {
	out := make(map[string]GroupState, len(groups))
	for _, b := range cc.Brokers() {
		cl, err := cc.brokerFor(b.ID)
		if err != nil {
			return nil, err
		}
		req := &protocol.DescribeGroupsRequest{
			Version:  cl.version(protocol.APKDescribeGroups),
			GroupIDs: groups,
		}
		body, err := protocol.EncodeDescribeGroupsRequest(req)
		if err != nil {
			return nil, err
		}
		respBody, err := cl.RoundTrip(protocol.APKDescribeGroups, req.Version, body)
		if err != nil {
			return nil, err
		}
		resp, err := protocol.DecodeDescribeGroupsResponse(req.Version, respBody)
		if err != nil {
			return nil, err
		}
		for _, g := range resp.Groups {
			if g.ErrorCode != protocol.ErrNone {
				continue
			}
			out[g.GroupID] = GroupState{
				Group:        g.GroupID,
				State:        g.State,
				ProtocolType: g.ProtocolType,
				Members:      len(g.Members),
			}
		}
	}
	return out, nil
}

// ListGroups returns every group the cluster knows, merged over all brokers:
// each broker answers only for the groups it coordinates.
func (cc *ClusterClient) ListGroups() ([]string, error) {
	seen := make(map[string]bool)
	for _, b := range cc.Brokers() {
		cl, err := cc.brokerFor(b.ID)
		if err != nil {
			return nil, err
		}
		names, err := cl.ListGroups()
		if err != nil {
			return nil, fmt.Errorf("list groups on broker %d: %w", b.ID, err)
		}
		for _, n := range names {
			seen[n] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// GroupState is a group's coordinator-reported state.
type GroupState struct {
	Group        string
	State        string
	ProtocolType string
	Members      int
}

// DescribeGroup returns one group's state, asked of its coordinator.
func (cc *ClusterClient) DescribeGroup(group string) (GroupState, error) {
	cl, err := cc.coordinator(group)
	if err != nil {
		return GroupState{}, err
	}
	req := &protocol.DescribeGroupsRequest{
		Version:  cl.version(protocol.APKDescribeGroups),
		GroupIDs: []string{group},
	}
	body, err := protocol.EncodeDescribeGroupsRequest(req)
	if err != nil {
		return GroupState{}, err
	}
	respBody, err := cl.RoundTrip(protocol.APKDescribeGroups, req.Version, body)
	if err != nil {
		return GroupState{}, err
	}
	resp, err := protocol.DecodeDescribeGroupsResponse(req.Version, respBody)
	if err != nil {
		return GroupState{}, err
	}
	if len(resp.Groups) == 0 {
		return GroupState{}, fmt.Errorf("no description returned for group %q", group)
	}
	g := resp.Groups[0]
	if g.ErrorCode != protocol.ErrNone {
		return GroupState{}, fmt.Errorf("describe group %q: kafka error code %d", group, g.ErrorCode)
	}
	return GroupState{
		Group:        g.GroupID,
		State:        g.State,
		ProtocolType: g.ProtocolType,
		Members:      len(g.Members),
	}, nil
}

// GroupOffsets returns a group's committed offsets for every topic it holds
// offsets on, asked of its coordinator.
func (cc *ClusterClient) GroupOffsets(group string) (map[string]map[int32]int64, error) {
	cl, err := cc.coordinator(group)
	if err != nil {
		return nil, err
	}
	req := &protocol.OffsetFetchRequest{
		Version: cl.version(protocol.APKOffsetFetch),
		Group:   group,
		Topics:  nil, // every topic the group has offsets for
	}
	body, err := protocol.EncodeOffsetFetchRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := cl.RoundTrip(protocol.APKOffsetFetch, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeOffsetFetchResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[int32]int64, len(resp.Topics))
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != protocol.ErrNone || p.Offset < 0 {
				continue
			}
			if out[t.Topic] == nil {
				out[t.Topic] = make(map[int32]int64)
			}
			out[t.Topic][p.Partition] = p.Offset
		}
	}
	return out, nil
}

// coordinator resolves the broker that coordinates a group's offsets.
func (cc *ClusterClient) coordinator(group string) (*KafkaClient, error) {
	cl, err := cc.any()
	if err != nil {
		return nil, err
	}
	req := &protocol.FindCoordinatorRequest{
		Version: cl.version(protocol.APKFindCoordinator),
		Key:     group,
	}
	body, err := protocol.EncodeFindCoordinatorRequest(req)
	if err != nil {
		return nil, err
	}
	respBody, err := cl.RoundTrip(protocol.APKFindCoordinator, req.Version, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeFindCoordinatorResponse(req.Version, respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != protocol.ErrNone {
		return nil, fmt.Errorf("find coordinator for %q: kafka error code %d", group, resp.ErrorCode)
	}
	return cc.brokerFor(resp.NodeID)
}

func (cc *ClusterClient) topicInfo(topic string) (TopicInfo, bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, t := range cc.topics {
		if t.Name == topic {
			return t, true
		}
	}
	return TopicInfo{}, false
}
