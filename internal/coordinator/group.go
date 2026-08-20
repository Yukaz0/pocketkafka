package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

// GroupState models the consumer group rebalance state machine.
type GroupState int

const (
	StateEmpty GroupState = iota
	StatePreparingRebalance
	StateCompletingRebalance
	StateStable
)

func (s GroupState) String() string {
	switch s {
	case StateEmpty:
		return "Empty"
	case StatePreparingRebalance:
		return "PreparingRebalance"
	case StateCompletingRebalance:
		return "CompletingRebalance"
	case StateStable:
		return "Stable"
	}
	return "Unknown"
}

// Member is one consumer in a group.
type Member struct {
	ID             string
	InstanceID     *string
	SessionTimeout int32
	ProtocolType   string
	Protocols      []protocol.JoinGroupRequestProtocol
	LastHeartbeat  time.Time
}

// Group is the coordinator state for a single consumer group.
type Group struct {
	mu           sync.Mutex
	Name         string
	State        GroupState
	Generation   int32
	ProtocolType string
	Protocol     string
	LeaderID     string
	Members      map[string]*Member
	JoinOrder    []string
	assignments  map[string][]byte // memberID -> assignment bytes (from SyncGroup)
	offsets      *OffsetStore
}

// GroupManager coordinates all consumer groups.
type GroupManager struct {
	mu             sync.RWMutex
	groups         map[string]*Group
	offsets        *OffsetStore
	brokerNodeID   int32
	brokerHost     string
	brokerPort     int32
	defaultSession int32
	nextMemberSeq  int64
	producers      *ProducerIDManager
}

// NewGroupManager builds a coordinator for the given broker identity.
func NewGroupManager(offsets *OffsetStore, nodeID int32, host string, port int32, defaultSession int32) *GroupManager {
	return &GroupManager{
		groups:         make(map[string]*Group),
		offsets:        offsets,
		brokerNodeID:   nodeID,
		brokerHost:     host,
		brokerPort:     port,
		defaultSession: defaultSession,
		producers:      NewProducerIDManager(),
	}
}

// NextProducerID allocates a producer ID/epoch for idempotent or transactional
// producers (InitProducerId, Key 22).
func (gm *GroupManager) NextProducerID(transactionalID *string) (int64, int16) {
	return gm.producers.Next(transactionalID)
}

// ValidateProducer checks a producer's (transactional ID, PID, epoch) triple.
func (gm *GroupManager) ValidateProducer(transactionalID string, pid int64, epoch int16) bool {
	return gm.producers.Validate(transactionalID, pid, epoch)
}

func (gm *GroupManager) getOrCreate(name string) *Group {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	if g, ok := gm.groups[name]; ok {
		return g
	}
	g := &Group{
		Name:        name,
		State:       StateEmpty,
		Generation:  0,
		Members:     make(map[string]*Member),
		assignments: make(map[string][]byte),
		offsets:     gm.offsets,
	}
	gm.groups[name] = g
	return g
}

func (gm *GroupManager) nextMemberID() string {
	gm.mu.Lock()
	gm.nextMemberSeq++
	seq := gm.nextMemberSeq
	gm.mu.Unlock()
	return fmt.Sprintf("pocketkafka-%d", seq)
}

// GroupInfo is a read-only snapshot of a consumer group for the web UI.
type GroupInfo struct {
	Name       string
	State      string
	Generation int32
	LeaderID   string
	Members    []string
	Offsets    map[string]map[int32]int64 // topic -> partition -> committed offset
}

// MemberInfo is a read-only member snapshot used by DescribeGroups and the web
// UI group detail page.
type MemberInfo struct {
	MemberID     string
	ClientID     string
	ClientHost   string
	ProtocolType string
	Metadata     []byte
	Assignment   []byte
	Assigned     map[string][]int32 // topic -> partitions (decoded from assignment)
}

// ListGroups returns a snapshot of all consumer groups.
func (gm *GroupManager) ListGroups() []GroupInfo {
	gm.mu.RLock()
	names := make([]string, 0, len(gm.groups))
	for n := range gm.groups {
		names = append(names, n)
	}
	gm.mu.RUnlock()

	out := make([]GroupInfo, 0, len(names))
	for _, name := range names {
		g := gm.getOrCreate(name)
		g.mu.Lock()
		info := GroupInfo{
			Name:       name,
			State:      g.State.String(),
			Generation: g.Generation,
			LeaderID:   g.LeaderID,
			Offsets:    gm.offsets.GroupOffsets(name),
		}
		for _, id := range g.JoinOrder {
			info.Members = append(info.Members, id)
		}
		g.mu.Unlock()
		out = append(out, info)
	}
	return out
}

// FindCoordinator returns this broker's identity as the coordinator.
func (gm *GroupManager) FindCoordinator(req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	return &protocol.FindCoordinatorResponse{
		Version:   req.Version,
		ErrorCode: protocol.ErrNone,
		NodeID:    gm.brokerNodeID,
		Host:      gm.brokerHost,
		Port:      gm.brokerPort,
	}
}

// JoinGroup processes a member join request.
func (gm *GroupManager) JoinGroup(req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.JoinGroupResponse{
		Version:      req.Version,
		ErrorCode:    protocol.ErrNone,
		GenerationID: -1,
		MemberID:     req.MemberID,
	}

	if len(req.Protocols) == 0 {
		resp.ErrorCode = protocol.ErrInconsistentGroupProtocol
		return resp
	}

	memberID := req.MemberID
	var member *Member
	if memberID != "" {
		if m, ok := g.Members[memberID]; ok {
			member = m
		} else {
			// Unknown member ID; re-join with a fresh one.
			memberID = ""
		}
	}

	if memberID == "" {
		memberID = gm.nextMemberID()
		member = &Member{
			ID:             memberID,
			InstanceID:     req.InstanceID,
			SessionTimeout: req.SessionTimeoutMs,
			ProtocolType:   req.ProtocolType,
			Protocols:      req.Protocols,
			LastHeartbeat:  time.Now(),
		}
		g.Members[memberID] = member
		g.JoinOrder = append(g.JoinOrder, memberID)
	} else {
		member.Protocols = req.Protocols
		member.InstanceID = req.InstanceID
		member.SessionTimeout = req.SessionTimeoutMs
		member.ProtocolType = req.ProtocolType
		member.LastHeartbeat = time.Now()
	}

	// A join triggers a rebalance: bump generation and pick the leader.
	g.State = StateCompletingRebalance
	g.Generation++
	g.ProtocolType = req.ProtocolType
	if len(req.Protocols) > 0 {
		g.Protocol = req.Protocols[0].Name
	}
	g.LeaderID = g.JoinOrder[0]
	g.assignments = make(map[string][]byte)

	resp.GenerationID = g.Generation
	resp.ProtocolName = g.Protocol
	resp.LeaderID = g.LeaderID
	resp.MemberID = memberID
	g.Members[memberID].LastHeartbeat = time.Now()

	// Only the leader receives the full member list.
	if memberID == g.LeaderID {
		resp.Members = make([]protocol.JoinGroupResponseMember, 0, len(g.Members))
		for _, id := range g.JoinOrder {
			m := g.Members[id]
			var meta []byte
			for _, p := range m.Protocols {
				if p.Name == g.Protocol {
					meta = p.Metadata
					break
				}
			}
			resp.Members = append(resp.Members, protocol.JoinGroupResponseMember{
				MemberID:   m.ID,
				InstanceID: m.InstanceID,
				Metadata:   meta,
			})
		}
	}
	return resp
}

// SyncGroup processes assignment synchronization.
func (gm *GroupManager) SyncGroup(req *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.SyncGroupResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	if _, ok := g.Members[req.MemberID]; !ok {
		resp.ErrorCode = protocol.ErrUnknownMemberID
		return resp
	}

	// The leader uploads the assignments; store them for all members.
	for _, a := range req.Assignments {
		g.assignments[a.MemberID] = a.Assignment
	}
	if a, ok := g.assignments[req.MemberID]; ok {
		resp.Assignment = a
	}
	g.State = StateStable
	return resp
}

// Heartbeat keeps a member alive and detects stale generations.
func (gm *GroupManager) Heartbeat(req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.HeartbeatResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	member, ok := g.Members[req.MemberID]
	if !ok {
		resp.ErrorCode = protocol.ErrUnknownMemberID
		return resp
	}
	if req.GenerationID != g.Generation {
		resp.ErrorCode = protocol.ErrRebalanceInProgress
		return resp
	}
	member.LastHeartbeat = time.Now()
	return resp
}

// LeaveGroup removes members from a group.
func (gm *GroupManager) LeaveGroup(req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.LeaveGroupResponse{
		Version:   req.Version,
		ErrorCode: protocol.ErrNone,
	}
	var toRemove []string
	if req.Version < 3 {
		toRemove = []string{req.MemberID}
	} else {
		for _, m := range req.Members {
			toRemove = append(toRemove, m.MemberID)
		}
	}
	for _, id := range toRemove {
		if _, ok := g.Members[id]; ok {
			delete(g.Members, id)
			resp.Members = append(resp.Members, protocol.LeaveGroupResponseMember{MemberID: id})
		}
	}
	g.JoinOrder = filterOrder(g.JoinOrder, toRemove)
	if len(g.Members) == 0 {
		g.State = StateEmpty
		g.Generation = 0
	} else {
		// A member leaving triggers a rebalance for the rest.
		g.State = StatePreparingRebalance
		g.Generation++
		g.LeaderID = g.JoinOrder[0]
	}
	return resp
}

// OffsetCommit stores committed offsets for a group.
func (gm *GroupManager) OffsetCommit(req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.OffsetCommitResponse{Version: req.Version}
	topics := make([]protocol.OffsetCommitResponseTopic, 0, len(req.Topics))
	for _, t := range req.Topics {
		rt := protocol.OffsetCommitResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := protocol.OffsetCommitResponsePartition{Partition: p.Partition, ErrorCode: protocol.ErrNone}
			if err := g.offsets.Commit(req.Group, t.Topic, p.Partition, &CommittedOffset{
				Offset:      p.Offset,
				Metadata:    p.Metadata,
				LeaderEpoch: p.LeaderEpoch,
			}); err != nil {
				rp.ErrorCode = protocol.ErrUnknownServerError
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		topics = append(topics, rt)
	}
	resp.Topics = topics
	return resp
}

// OffsetFetch returns committed offsets for a group.
func (gm *GroupManager) OffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.OffsetFetchResponse{Version: req.Version, ErrorCode: protocol.ErrNone}

	// Build the list of topics/partitions to look up.
	type tp struct {
		topic string
		parts []int32
	}
	var lookups []tp
	if req.Topics == nil {
		for _, topic := range g.offsets.GroupTopics(req.Group) {
			lookups = append(lookups, tp{topic, g.offsets.Partitions(req.Group, topic)})
		}
	} else {
		for _, t := range req.Topics {
			lookups = append(lookups, tp{t.Topic, t.Partitions})
		}
	}

	for _, l := range lookups {
		rt := protocol.OffsetFetchResponseTopic{Topic: l.topic}
		for _, pid := range l.parts {
			rp := protocol.OffsetFetchResponsePartition{Partition: pid, Offset: -1, LeaderEpoch: -1}
			if off, ok := g.offsets.Fetch(req.Group, l.topic, pid); ok {
				rp.Offset = off.Offset
				rp.Metadata = off.Metadata
				rp.LeaderEpoch = off.LeaderEpoch
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	return resp
}

func filterOrder(order []string, remove []string) []string {
	rm := make(map[string]bool, len(remove))
	for _, id := range remove {
		rm[id] = true
	}
	out := order[:0]
	for _, id := range order {
		if !rm[id] {
			out = append(out, id)
		}
	}
	return out
}

// ListGroupIDs returns the names of all known consumer groups.
func (gm *GroupManager) ListGroupIDs() []string {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	names := make([]string, 0, len(gm.groups))
	for n := range gm.groups {
		names = append(names, n)
	}
	return names
}

// GroupExists reports whether a group is registered with the coordinator.
func (gm *GroupManager) GroupExists(name string) bool {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	_, ok := gm.groups[name]
	return ok
}

// DescribeGroup returns a detailed snapshot of one group for DescribeGroups and
// the web UI. It returns nil when the group does not exist.
func (gm *GroupManager) DescribeGroup(name string) *GroupInfo {
	gm.mu.RLock()
	g, ok := gm.groups[name]
	gm.mu.RUnlock()
	if !ok {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	info := &GroupInfo{
		Name:       name,
		State:      g.State.String(),
		Generation: g.Generation,
		LeaderID:   g.LeaderID,
		Offsets:    gm.offsets.GroupOffsets(name),
	}
	for _, id := range g.JoinOrder {
		if m, ok := g.Members[id]; ok {
			_ = m
			info.Members = append(info.Members, id)
		}
	}
	return info
}

// MembersDetail returns per-member metadata/assignment for a group.
func (gm *GroupManager) MembersDetail(name string) []MemberInfo {
	gm.mu.RLock()
	g, ok := gm.groups[name]
	gm.mu.RUnlock()
	if !ok {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]MemberInfo, 0, len(g.Members))
	for _, id := range g.JoinOrder {
		m, ok := g.Members[id]
		if !ok {
			continue
		}
		mi := MemberInfo{
			MemberID:     m.ID,
			ClientID:     m.ID,
			ClientHost:   "/" + m.ID,
			ProtocolType: m.ProtocolType,
			Metadata:     protocolMetadata(m.Protocols, g.Protocol),
		}
		if a, ok := g.assignments[id]; ok {
			mi.Assignment = a
			mi.Assigned = decodeAssignmentFrom(a)
		}
		out = append(out, mi)
	}
	return out
}

// DeleteGroup removes a group if it is Empty or Dead. It returns an error when
// the group is actively consuming (Stable/PreparingRebalance).
func (gm *GroupManager) DeleteGroup(name string) error {
	gm.mu.RLock()
	g, ok := gm.groups[name]
	gm.mu.RUnlock()
	if !ok {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.Members) > 0 {
		return errNonEmptyGroup
	}
	g.State = StateEmpty
	g.Generation = 0
	g.LeaderID = ""
	g.Members = make(map[string]*Member)
	g.JoinOrder = nil
	g.assignments = make(map[string][]byte)

	gm.mu.Lock()
	delete(gm.groups, name)
	gm.mu.Unlock()
	return nil
}

// ResetOffsets sets the committed offset for a group/topic/partition to the
// given value. It is the backend for the admin reset-offset API.
func (gm *GroupManager) ResetOffsets(group, topic string, partition int32, offset int64) error {
	return gm.offsets.Commit(group, topic, partition, &CommittedOffset{Offset: offset})
}

var errNonEmptyGroup = fmt.Errorf("group is not empty")

// protocolMetadata picks the metadata bytes for the group's chosen protocol.
func protocolMetadata(protocols []protocol.JoinGroupRequestProtocol, chosen string) []byte {
	for _, p := range protocols {
		if p.Name == chosen {
			return p.Metadata
		}
	}
	if len(protocols) > 0 {
		return protocols[0].Metadata
	}
	return nil
}

// decodeAssignmentFrom parses a consumer protocol assignment payload
// (topic array -> partition array) into a map.
func decodeAssignmentFrom(b []byte) map[string][]int32 {
	out := make(map[string][]int32)
	if len(b) == 0 {
		return out
	}
	r := protocol.NewReader(b)
	n, err := r.ReadArrayLen()
	if err != nil || n < 0 {
		return out
	}
	for i := 0; i < n; i++ {
		topic, err := r.ReadString()
		if err != nil {
			break
		}
		pn, err := r.ReadArrayLen()
		if err != nil || pn < 0 {
			break
		}
		var parts []int32
		for j := 0; j < pn; j++ {
			p, err := r.ReadInt32()
			if err != nil {
				break
			}
			parts = append(parts, p)
		}
		out[topic] = parts
	}
	return out
}
