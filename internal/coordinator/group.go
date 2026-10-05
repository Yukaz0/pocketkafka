package coordinator

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
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
	// RebalanceTimeout bounds how long this member may take to rejoin during a
	// rebalance; it is the per-member deadline the join barrier uses.
	RebalanceTimeout time.Duration
	ProtocolType     string
	Protocols        []protocol.JoinGroupRequestProtocol
	LastHeartbeat    time.Time
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
	// assignmentsChanged is closed (and replaced) whenever assignments are
	// written, so SyncGroup followers can wait event-driven instead of
	// sleep-polling every 20ms.
	assignmentsChanged chan struct{}
	offsets            *OffsetStore

	// Join barrier: a rebalance completes only once every known member has
	// rejoined (or its rebalance timeout elapsed), so the generation is bumped
	// once per round instead of once per join.
	joined            map[string]bool // members that rejoined in the current round
	rebalanceDeadline time.Time
	joinNotify        chan struct{} // closed+replaced when a join round completes

	// deleted is set by DeleteGroup so a concurrent caller holding this pointer
	// can detect that the group was removed and stop mutating a detached group.
	deleted atomic.Bool
}

// minDuration returns the smaller of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// syncWaitTimeout bounds how long a follower's SyncGroup blocks waiting for
// the leader to upload assignments (Kafka uses the rebalance sync timeout).
const syncWaitTimeout = 10 * time.Second

// GroupManager coordinates all consumer groups.
type GroupManager struct {
	mu             sync.RWMutex
	groups         map[string]*Group
	offsets        *OffsetStore
	brokerNodeID   int32
	brokerHost     string
	brokerPort     int32
	defaultSession int32
	// defaultRebalance is used only when a JoinGroup request carries neither a
	// rebalance nor a session timeout, so coordinator.rebalance_timeout_ms is
	// not dead config.
	defaultRebalance time.Duration
	nextMemberSeq    int64
	producers        *ProducerIDManager
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

// PruneOffsets drops committed offsets that have not been updated within
// retention relative to now, implementing offsets_retention_minutes. It returns
// the number of offsets removed. A non-positive retention disables pruning.
func (gm *GroupManager) PruneOffsets(now time.Time, retention time.Duration) int {
	if retention <= 0 {
		return 0
	}
	return gm.offsets.PruneOlderThan(now.Add(-retention))
}

// get returns an existing group without creating one. Read paths use it so a
// mistyped group name cannot grow the map forever.
func (gm *GroupManager) get(name string) *Group {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	g := gm.groups[name]
	if g != nil && g.deleted.Load() {
		return nil
	}
	return g
}

func (gm *GroupManager) getOrCreate(name string) *Group {
	gm.mu.RLock()
	if g, ok := gm.groups[name]; ok && !g.deleted.Load() {
		gm.mu.RUnlock()
		return g
	}
	gm.mu.RUnlock()

	gm.mu.Lock()
	defer gm.mu.Unlock()
	if g, ok := gm.groups[name]; ok && !g.deleted.Load() {
		return g
	}
	g := &Group{
		Name:               name,
		State:              StateEmpty,
		Generation:         0,
		Members:            make(map[string]*Member),
		assignments:        make(map[string][]byte),
		assignmentsChanged: make(chan struct{}),
		joinNotify:         make(chan struct{}),
		joined:             make(map[string]bool),
		offsets:            gm.offsets,
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
		g := gm.get(name)
		if g == nil {
			continue
		}
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

// JoinGroup processes a member join request. It implements Kafka's join barrier:
// a rebalance starts when a member joins a Stable/Empty group, and it completes
// only once every known member has rejoined or its rebalance timeout has
// elapsed. The generation is bumped once per completed round, so members that
// rejoin in the same round agree on it and the group settles instead of
// endlessly invalidating itself.
func (gm *GroupManager) JoinGroup(req *protocol.JoinGroupRequest) *protocol.JoinGroupResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()

	resp := &protocol.JoinGroupResponse{
		Version:      req.Version,
		ErrorCode:    protocol.ErrNone,
		GenerationID: -1,
		MemberID:     req.MemberID,
	}

	if len(req.Protocols) == 0 {
		resp.ErrorCode = protocol.ErrInconsistentGroupProtocol
		g.mu.Unlock()
		return resp
	}

	memberID := req.MemberID
	member, existing := g.Members[memberID]
	if req.MemberID != "" && !existing {
		// Unknown member ID; assign a fresh one on the re-join.
		memberID = ""
	}

	// Rejoining a stable group with unchanged metadata must not start a
	// rebalance, otherwise a reconnecting client churns the generation.
	if existing && g.State == StateStable && sameMemberMetadata(member, req) {
		member.LastHeartbeat = time.Now()
		g.fillJoinResponseLocked(resp, memberID)
		g.mu.Unlock()
		return resp
	}

	if memberID == "" {
		memberID = gm.nextMemberID()
		member = &Member{ID: memberID}
		g.Members[memberID] = member
		g.JoinOrder = append(g.JoinOrder, memberID)
	}
	member.InstanceID = req.InstanceID
	member.SessionTimeout = req.SessionTimeoutMs
	member.RebalanceTimeout = gm.rebalanceTimeout(req)
	member.ProtocolType = req.ProtocolType
	member.Protocols = req.Protocols
	member.LastHeartbeat = time.Now()
	resp.MemberID = memberID

	if g.State != StatePreparingRebalance {
		g.beginRebalanceLocked()
	}
	g.joined[memberID] = true

	if len(g.joined) >= len(g.Members) {
		g.completeJoinLocked()
	} else {
		deadline := g.rebalanceDeadline
		for g.State == StatePreparingRebalance {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				g.expireRebalanceLocked()
				break
			}
			// Wait for the round to complete without holding g.mu.
			ch := g.joinNotify
			g.mu.Unlock()
			timer := time.NewTimer(remaining)
			select {
			case <-ch:
			case <-timer.C:
			}
			timer.Stop()
			g.mu.Lock()
			if _, ok := g.Members[memberID]; !ok {
				// Dropped as a non-responder while waiting.
				resp.ErrorCode = protocol.ErrUnknownMemberID
				g.mu.Unlock()
				return resp
			}
			if !time.Now().Before(deadline) {
				g.expireRebalanceLocked()
				break
			}
		}
	}

	g.fillJoinResponseLocked(resp, memberID)
	g.mu.Unlock()
	return resp
}

// sameMemberMetadata reports whether a rejoining member's subscription metadata
// is unchanged, so the rejoin cannot require a new assignment.
func sameMemberMetadata(m *Member, req *protocol.JoinGroupRequest) bool {
	if m.ProtocolType != req.ProtocolType {
		return false
	}
	if !sameStringPtr(m.InstanceID, req.InstanceID) {
		return false
	}
	if len(m.Protocols) != len(req.Protocols) {
		return false
	}
	for i := range m.Protocols {
		if m.Protocols[i].Name != req.Protocols[i].Name {
			return false
		}
		if !bytes.Equal(m.Protocols[i].Metadata, req.Protocols[i].Metadata) {
			return false
		}
	}
	return true
}

func sameStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// WithDefaultRebalanceTimeout sets the rebalance timeout used when a JoinGroup
// request supplies neither a rebalance nor a session timeout.
func (gm *GroupManager) WithDefaultRebalanceTimeout(d time.Duration) *GroupManager {
	gm.defaultRebalance = d
	return gm
}

// rebalanceTimeout resolves the join barrier deadline for one request: the
// request's rebalance timeout, else its session timeout, else the configured
// default, else 10s.
func (gm *GroupManager) rebalanceTimeout(req *protocol.JoinGroupRequest) time.Duration {
	ms := req.RebalanceTimeoutMs
	if ms <= 0 {
		ms = req.SessionTimeoutMs
	}
	if ms <= 0 {
		if gm.defaultRebalance > 0 {
			return gm.defaultRebalance
		}
		ms = 10000
	}
	return time.Duration(ms) * time.Millisecond
}

// beginRebalanceLocked opens a new join round and arms the rebalance deadline.
func (g *Group) beginRebalanceLocked() {
	g.State = StatePreparingRebalance
	g.joined = make(map[string]bool, len(g.Members))
	deadline := time.Now().Add(10 * time.Second)
	first := true
	for _, m := range g.Members {
		d := m.RebalanceTimeout
		if d <= 0 {
			d = time.Duration(m.SessionTimeout) * time.Millisecond
		}
		if d <= 0 {
			continue
		}
		if t := time.Now().Add(d); first || t.After(deadline) {
			deadline = t
			first = false
		}
	}
	g.rebalanceDeadline = deadline
}

// completeJoinLocked finishes the current round: it bumps the generation once,
// selects the protocol and leader, and releases every waiting JoinGroup call.
func (g *Group) completeJoinLocked() {
	if len(g.Members) == 0 {
		g.State = StateEmpty
		g.LeaderID = ""
		g.joined = make(map[string]bool)
		g.rebalanceDeadline = time.Time{}
		g.closeJoinWaitersLocked()
		return
	}
	g.Generation++
	g.State = StateCompletingRebalance
	g.LeaderID = g.JoinOrder[0]
	leader := g.Members[g.LeaderID]
	g.ProtocolType = leader.ProtocolType
	g.Protocol = chooseProtocol(g.Members, g.JoinOrder, leader)
	g.assignments = make(map[string][]byte)
	g.joined = make(map[string]bool)
	g.rebalanceDeadline = time.Time{}
	g.closeJoinWaitersLocked()
}

// expireRebalanceLocked drops members that did not rejoin this round and
// completes the round with the survivors.
func (g *Group) expireRebalanceLocked() {
	var dead []string
	for id := range g.Members {
		if !g.joined[id] {
			dead = append(dead, id)
		}
	}
	for _, id := range dead {
		delete(g.Members, id)
	}
	g.JoinOrder = filterOrder(g.JoinOrder, dead)
	g.completeJoinLocked()
}

func (g *Group) closeJoinWaitersLocked() {
	close(g.joinNotify)
	g.joinNotify = make(chan struct{})
}

// chooseProtocol picks the first protocol supported by every member, falling
// back to the leader's first choice when there is no common protocol.
func chooseProtocol(members map[string]*Member, order []string, leader *Member) string {
	if leader == nil || len(leader.Protocols) == 0 {
		return ""
	}
	for _, cand := range leader.Protocols {
		ok := true
		for _, id := range order {
			m := members[id]
			if m == nil {
				continue
			}
			supported := false
			for _, p := range m.Protocols {
				if p.Name == cand.Name {
					supported = true
					break
				}
			}
			if !supported {
				ok = false
				break
			}
		}
		if ok {
			return cand.Name
		}
	}
	return leader.Protocols[0].Name
}

// fillJoinResponseLocked sets the fields every JoinGroup reply carries. Only the
// leader receives the full member list.
func (g *Group) fillJoinResponseLocked(resp *protocol.JoinGroupResponse, memberID string) {
	resp.GenerationID = g.Generation
	resp.ProtocolName = g.Protocol
	resp.LeaderID = g.LeaderID
	resp.MemberID = memberID
	resp.Members = nil
	if memberID != g.LeaderID {
		return
	}
	members := make([]protocol.JoinGroupResponseMember, 0, len(g.Members))
	for _, id := range g.JoinOrder {
		m := g.Members[id]
		if m == nil {
			continue
		}
		var meta []byte
		for _, p := range m.Protocols {
			if p.Name == g.Protocol {
				meta = p.Metadata
				break
			}
		}
		members = append(members, protocol.JoinGroupResponseMember{
			MemberID:   m.ID,
			InstanceID: m.InstanceID,
			Metadata:   meta,
		})
	}
	resp.Members = members
}

// SyncGroup processes assignment synchronization.
func (gm *GroupManager) SyncGroup(req *protocol.SyncGroupRequest) *protocol.SyncGroupResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.SyncGroupResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	if g.State == StatePreparingRebalance {
		resp.ErrorCode = protocol.ErrRebalanceInProgress
		return resp
	}
	if _, ok := g.Members[req.MemberID]; !ok {
		resp.ErrorCode = protocol.ErrUnknownMemberID
		return resp
	}
	// A member from an older round must not upload assignments into the current
	// generation.
	if req.GenerationID != g.Generation {
		resp.ErrorCode = protocol.ErrIllegalGeneration
		return resp
	}

	// The leader uploads the assignments; store them for all members.
	for _, a := range req.Assignments {
		g.assignments[a.MemberID] = a.Assignment
	}
	// Bangunkan follower yang sedang menunggu assignment.
	close(g.assignmentsChanged)
	g.assignmentsChanged = make(chan struct{})

	// Followers may reach SyncGroup before the leader uploads assignments.
	// Blocking here (like Kafka does until the sync timeout) prevents them
	// from receiving a null assignment, which clients such as sarama reject
	// with "invalid byteslice length" while decoding the response.
	if _, ok := g.assignments[req.MemberID]; !ok && req.MemberID != g.LeaderID {
		// Event-driven: tunggu sinyal assignmentsChanged, bukan
		// sleep-poll 20ms berkala.
		deadline := time.Now().Add(syncWaitTimeout)
		for {
			if _, ok := g.assignments[req.MemberID]; ok {
				break
			}
			// A new rebalance invalidated this round; stop waiting and let the
			// client rejoin instead of applying a stale assignment.
			if g.State != StateCompletingRebalance || g.Generation != req.GenerationID {
				break
			}
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			changed := g.assignmentsChanged
			g.mu.Unlock()
			timer := time.NewTimer(minDuration(remaining, 250*time.Millisecond))
			select {
			case <-changed:
			case <-timer.C:
			}
			timer.Stop()
			g.mu.Lock()
		}
	}
	if a, ok := g.assignments[req.MemberID]; ok {
		resp.Assignment = a
	}
	// Only the round that produced these assignments may settle the group; a
	// rebalance that started while this follower waited must not be marked
	// Stable.
	if g.State == StateCompletingRebalance && req.GenerationID == g.Generation {
		g.State = StateStable
	}
	return resp
}

// Heartbeat keeps a member alive and detects stale generations. A heartbeat
// during a rebalance tells the client to rejoin; an unknown group is not
// created.
func (gm *GroupManager) Heartbeat(req *protocol.HeartbeatRequest) *protocol.HeartbeatResponse {
	resp := &protocol.HeartbeatResponse{Version: req.Version, ErrorCode: protocol.ErrNone}
	g := gm.get(req.Group)
	if g == nil {
		resp.ErrorCode = protocol.ErrUnknownMemberID
		return resp
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	member, ok := g.Members[req.MemberID]
	if !ok {
		resp.ErrorCode = protocol.ErrUnknownMemberID
		return resp
	}
	if g.State == StatePreparingRebalance {
		resp.ErrorCode = protocol.ErrRebalanceInProgress
		return resp
	}
	if req.GenerationID != g.Generation {
		resp.ErrorCode = protocol.ErrIllegalGeneration
		return resp
	}
	member.LastHeartbeat = time.Now()
	return resp
}

// LeaveGroup removes members from a group.
func (gm *GroupManager) LeaveGroup(req *protocol.LeaveGroupRequest) *protocol.LeaveGroupResponse {
	resp := &protocol.LeaveGroupResponse{
		Version:   req.Version,
		ErrorCode: protocol.ErrNone,
	}
	g := gm.get(req.Group)
	if g == nil {
		return resp
	}
	g.mu.Lock()
	defer g.mu.Unlock()

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
			delete(g.joined, id)
			resp.Members = append(resp.Members, protocol.LeaveGroupResponseMember{MemberID: id})
		}
	}
	g.JoinOrder = filterOrder(g.JoinOrder, toRemove)
	if len(g.Members) == 0 {
		g.State = StateEmpty
		g.Generation = 0
		g.LeaderID = ""
	} else {
		// A member leaving triggers a rebalance for the rest; the generation is
		// bumped when that round completes, not here.
		if g.State == StatePreparingRebalance && len(g.joined) >= len(g.Members) {
			g.completeJoinLocked()
		} else {
			g.beginRebalanceLocked()
		}
	}
	return resp
}

// OffsetCommit stores committed offsets for a group. Generation-carrying commits
// are fenced against the current member set, and all partitions in the request
// are persisted with a single WAL fsync.
func (gm *GroupManager) OffsetCommit(req *protocol.OffsetCommitRequest) *protocol.OffsetCommitResponse {
	g := gm.getOrCreate(req.Group)
	g.mu.Lock()
	defer g.mu.Unlock()

	resp := &protocol.OffsetCommitResponse{Version: req.Version}
	fenceErr := protocol.ErrNone
	if req.Generation >= 0 {
		switch {
		case g.State == StatePreparingRebalance:
			fenceErr = protocol.ErrRebalanceInProgress
		default:
			if _, ok := g.Members[req.MemberID]; !ok {
				fenceErr = protocol.ErrUnknownMemberID
			} else if req.Generation != g.Generation {
				fenceErr = protocol.ErrIllegalGeneration
			}
		}
	}

	topics := make([]protocol.OffsetCommitResponseTopic, 0, len(req.Topics))
	recs := make([]OffsetCommitRecord, 0, len(req.Topics))
	for _, t := range req.Topics {
		rt := protocol.OffsetCommitResponseTopic{Topic: t.Topic}
		for _, p := range t.Partitions {
			rp := protocol.OffsetCommitResponsePartition{Partition: p.Partition, ErrorCode: fenceErr}
			if fenceErr == protocol.ErrNone {
				recs = append(recs, OffsetCommitRecord{
					Group:     req.Group,
					Topic:     t.Topic,
					Partition: p.Partition,
					Offset: &CommittedOffset{
						Offset:      p.Offset,
						Metadata:    p.Metadata,
						LeaderEpoch: p.LeaderEpoch,
					},
				})
			}
			rt.Partitions = append(rt.Partitions, rp)
		}
		topics = append(topics, rt)
	}
	if len(recs) > 0 {
		if err := g.offsets.CommitBatch(recs); err != nil {
			for i := range topics {
				for j := range topics[i].Partitions {
					topics[i].Partitions[j].ErrorCode = protocol.ErrUnknownServerError
				}
			}
		}
	}
	resp.Topics = topics
	return resp
}

// OffsetFetch returns committed offsets for a group. It reads the offset store
// directly and does not take the group lock, so a fetch cannot be blocked behind
// a heartbeat or commit.
func (gm *GroupManager) OffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	resp := &protocol.OffsetFetchResponse{Version: req.Version, ErrorCode: protocol.ErrNone}

	// Build the list of topics/partitions to look up.
	type tp struct {
		topic string
		parts []int32
	}
	var lookups []tp
	if req.Topics == nil {
		for _, topic := range gm.offsets.GroupTopics(req.Group) {
			lookups = append(lookups, tp{topic, gm.offsets.Partitions(req.Group, topic)})
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
			if off, ok := gm.offsets.Fetch(req.Group, l.topic, pid); ok {
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
	g.joined = make(map[string]bool)
	// A caller that already holds this pointer must be able to detect that the
	// group was deleted instead of adding members to a detached group.
	g.deleted.Store(true)

	gm.mu.Lock()
	if gm.groups[name] == g {
		delete(gm.groups, name)
	}
	gm.mu.Unlock()
	// Kafka deletes the group's committed offsets with the group.
	return gm.offsets.DeleteGroup(name)
}

// ResetOffsets sets the committed offset for a group/topic/partition to the
// given value. It is the backend for the admin reset-offset API. The group lock
// is taken when the group exists so an admin reset cannot reorder against an
// in-flight OffsetCommit for the same key.
func (gm *GroupManager) ResetOffsets(group, topic string, partition int32, offset int64) error {
	if g := gm.get(group); g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
	}
	return gm.offsets.Commit(group, topic, partition, &CommittedOffset{Offset: offset})
}

// ReapExpiredSessions removes members whose session timeout elapsed without a
// heartbeat and triggers a rebalance for the survivors. It returns the number of
// members removed.
func (gm *GroupManager) ReapExpiredSessions(now time.Time) int {
	gm.mu.RLock()
	groups := make([]*Group, 0, len(gm.groups))
	for _, g := range gm.groups {
		groups = append(groups, g)
	}
	gm.mu.RUnlock()

	removed := 0
	for _, g := range groups {
		removed += g.reapExpired(now, gm.defaultSession)
	}
	return removed
}

// StartSessionReaper runs ReapExpiredSessions on a ticker until the returned
// stop function is called. A non-positive interval disables the loop.
func (gm *GroupManager) StartSessionReaper(interval time.Duration) func() {
	if interval <= 0 {
		return func() {}
	}
	stop := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				gm.ReapExpiredSessions(time.Now())
			}
		}
	}()
	return func() { once.Do(func() { close(stop) }) }
}

// reapExpired drops members past their session timeout. A group that still has
// members must rebalance, because the assignment lost a participant.
func (g *Group) reapExpired(now time.Time, defaultSession int32) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	var dead []string
	for id, m := range g.Members {
		timeout := m.SessionTimeout
		if timeout <= 0 {
			timeout = defaultSession
		}
		if timeout <= 0 {
			continue
		}
		if now.Sub(m.LastHeartbeat) > time.Duration(timeout)*time.Millisecond {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		return 0
	}
	for _, id := range dead {
		delete(g.Members, id)
		delete(g.joined, id)
	}
	g.JoinOrder = filterOrder(g.JoinOrder, dead)
	switch {
	case len(g.Members) == 0:
		g.State = StateEmpty
		g.Generation = 0
		g.LeaderID = ""
	case g.State == StatePreparingRebalance:
		if len(g.joined) >= len(g.Members) {
			g.completeJoinLocked()
		}
	default:
		g.beginRebalanceLocked()
	}
	return len(dead)
}

// GroupOffsetsSnapshot returns the committed offsets for one group as
// topic -> partition -> offset (used by the UI snapshot export).
func (gm *GroupManager) GroupOffsetsSnapshot(group string) map[string]map[int32]int64 {
	return gm.offsets.GroupOffsets(group)
}

// CommittedGroups returns every group that has committed offsets, including
// groups that have no live members. A consumer that stopped still holds a
// backlog an operator must be able to see, so monitoring cannot rely on
// ListGroups alone (which only knows about groups with live/buffered members).
// The result is group -> topic -> partition -> offset.
func (gm *GroupManager) CommittedGroups() map[string]map[string]map[int32]int64 {
	out := map[string]map[string]map[int32]int64{}
	for _, name := range gm.offsets.Groups() {
		offsets := gm.offsets.GroupOffsets(name)
		if len(offsets) == 0 {
			continue
		}
		out[name] = offsets
	}
	return out
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
