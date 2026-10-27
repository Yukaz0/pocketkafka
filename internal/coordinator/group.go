package coordinator

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Yukaz0/pocketkafka/pkg/protocol"
)

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

type Member struct {
	ID             string
	InstanceID     *string
	SessionTimeout int32

	RebalanceTimeout time.Duration
	ProtocolType     string
	Protocols        []protocol.JoinGroupRequestProtocol
	LastHeartbeat    time.Time
}

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
	assignments  map[string][]byte

	assignmentsChanged chan struct{}
	offsets            *OffsetStore

	joined            map[string]bool
	rebalanceDeadline time.Time
	joinNotify        chan struct{}

	deleted atomic.Bool
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

const syncWaitTimeout = 10 * time.Second

type GroupManager struct {
	mu             sync.RWMutex
	groups         map[string]*Group
	offsets        *OffsetStore
	brokerNodeID   int32
	brokerHost     string
	brokerPort     int32
	defaultSession int32

	defaultRebalance time.Duration
	nextMemberSeq    int64
	producers        *ProducerIDManager
}

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

func (gm *GroupManager) NextProducerID(transactionalID *string) (int64, int16) {
	return gm.producers.Next(transactionalID)
}

func (gm *GroupManager) ValidateProducer(transactionalID string, pid int64, epoch int16) bool {
	return gm.producers.Validate(transactionalID, pid, epoch)
}

func (gm *GroupManager) PruneOffsets(now time.Time, retention time.Duration) int {
	if retention <= 0 {
		return 0
	}
	return gm.offsets.PruneOlderThan(now.Add(-retention))
}

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

type GroupInfo struct {
	Name       string
	State      string
	Generation int32
	LeaderID   string
	Members    []string
	Offsets    map[string]map[int32]int64
}

type MemberInfo struct {
	MemberID     string
	ClientID     string
	ClientHost   string
	ProtocolType string
	Metadata     []byte
	Assignment   []byte
	Assigned     map[string][]int32
}

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

func (gm *GroupManager) FindCoordinator(req *protocol.FindCoordinatorRequest) *protocol.FindCoordinatorResponse {
	return &protocol.FindCoordinatorResponse{
		Version:   req.Version,
		ErrorCode: protocol.ErrNone,
		NodeID:    gm.brokerNodeID,
		Host:      gm.brokerHost,
		Port:      gm.brokerPort,
	}
}

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

		memberID = ""
	}

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

func (gm *GroupManager) WithDefaultRebalanceTimeout(d time.Duration) *GroupManager {
	gm.defaultRebalance = d
	return gm
}

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

	if req.GenerationID != g.Generation {
		resp.ErrorCode = protocol.ErrIllegalGeneration
		return resp
	}

	for _, a := range req.Assignments {
		g.assignments[a.MemberID] = a.Assignment
	}

	close(g.assignmentsChanged)
	g.assignmentsChanged = make(chan struct{})

	if _, ok := g.assignments[req.MemberID]; !ok && req.MemberID != g.LeaderID {

		deadline := time.Now().Add(syncWaitTimeout)
		for {
			if _, ok := g.assignments[req.MemberID]; ok {
				break
			}

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

	if g.State == StateCompletingRebalance && req.GenerationID == g.Generation {
		g.State = StateStable
	}
	return resp
}

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

		if g.State == StatePreparingRebalance && len(g.joined) >= len(g.Members) {
			g.completeJoinLocked()
		} else {
			g.beginRebalanceLocked()
		}
	}
	return resp
}

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

func (gm *GroupManager) OffsetFetch(req *protocol.OffsetFetchRequest) *protocol.OffsetFetchResponse {
	resp := &protocol.OffsetFetchResponse{Version: req.Version, ErrorCode: protocol.ErrNone}

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

func (gm *GroupManager) ListGroupIDs() []string {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	names := make([]string, 0, len(gm.groups))
	for n := range gm.groups {
		names = append(names, n)
	}
	return names
}

func (gm *GroupManager) GroupExists(name string) bool {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	_, ok := gm.groups[name]
	return ok
}

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

	g.deleted.Store(true)

	gm.mu.Lock()
	if gm.groups[name] == g {
		delete(gm.groups, name)
	}
	gm.mu.Unlock()

	return gm.offsets.DeleteGroup(name)
}

func (gm *GroupManager) ResetOffsets(group, topic string, partition int32, offset int64) error {
	if g := gm.get(group); g != nil {
		g.mu.Lock()
		defer g.mu.Unlock()
	}
	return gm.offsets.Commit(group, topic, partition, &CommittedOffset{Offset: offset})
}

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

func (gm *GroupManager) GroupOffsetsSnapshot(group string) map[string]map[int32]int64 {
	return gm.offsets.GroupOffsets(group)
}

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
