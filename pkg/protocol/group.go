package protocol

// ---------------------------------------------------------------------------
// FindCoordinator (Key 10), v0-v3
// ---------------------------------------------------------------------------

// FindCoordinatorRequest requests the coordinator for a group.
type FindCoordinatorRequest struct {
	Version int16
	Key     string
	KeyType int8
}

// DecodeFindCoordinatorRequest parses the request body for versions up to 3.
func DecodeFindCoordinatorRequest(version int16, body []byte) (*FindCoordinatorRequest, error) {
	r := NewReader(body)
	req := &FindCoordinatorRequest{Version: version}
	var err error
	if req.Key, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if req.KeyType, err = r.ReadInt8(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// FindCoordinatorResponse reports the coordinator broker.
type FindCoordinatorResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   *string
	NodeID         int32
	Host           string
	Port           int32
}

// EncodeFindCoordinatorResponse serializes the response body.
func EncodeFindCoordinatorResponse(resp *FindCoordinatorResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if resp.Version >= 1 {
		w.WriteNullableString(resp.ErrorMessage)
	}
	w.WriteInt32(resp.NodeID)
	w.WriteString(resp.Host)
	w.WriteInt32(resp.Port)
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// JoinGroup (Key 11), v0-v6
// ---------------------------------------------------------------------------

// JoinGroupRequestProtocol is a proposed rebalance protocol.
type JoinGroupRequestProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupRequest asks to join a consumer group.
type JoinGroupRequest struct {
	Version            int16
	Group              string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	MemberID           string
	InstanceID         *string
	ProtocolType       string
	Protocols          []JoinGroupRequestProtocol
}

// DecodeJoinGroupRequest parses the request body for versions up to 6.
func DecodeJoinGroupRequest(version int16, body []byte) (*JoinGroupRequest, error) {
	r := NewReader(body)
	req := &JoinGroupRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.SessionTimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if req.RebalanceTimeoutMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	} else {
		req.RebalanceTimeoutMs = req.SessionTimeoutMs
	}
	if req.MemberID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version >= 5 {
		if req.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if req.ProtocolType, err = r.ReadString(); err != nil {
		return nil, err
	}
	pn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Protocols = make([]JoinGroupRequestProtocol, 0, pn)
	for i := 0; i < pn; i++ {
		var p JoinGroupRequestProtocol
		if p.Name, err = r.ReadString(); err != nil {
			return nil, err
		}
		if p.Metadata, err = r.ReadBytes(); err != nil {
			return nil, err
		}
		req.Protocols = append(req.Protocols, p)
	}
	return req, nil
}

// JoinGroupResponseMember is another member in the group.
type JoinGroupResponseMember struct {
	MemberID   string
	InstanceID *string
	Metadata   []byte
}

// JoinGroupResponse is the result of a join request.
type JoinGroupResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	GenerationID   int32
	ProtocolName   string
	LeaderID       string
	MemberID       string
	Members        []JoinGroupResponseMember
}

// EncodeJoinGroupResponse serializes the response body.
func EncodeJoinGroupResponse(resp *JoinGroupResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt32(resp.GenerationID)
	if resp.Version >= 7 {
		w.WriteNullableString(nil) // protocol_type
		w.WriteNullableString(&resp.ProtocolName)
	} else {
		w.WriteString(resp.ProtocolName)
	}
	w.WriteString(resp.LeaderID)
	w.WriteString(resp.MemberID)
	w.WriteArrayLen(len(resp.Members))
	for _, m := range resp.Members {
		w.WriteString(m.MemberID)
		if resp.Version >= 5 {
			w.WriteNullableString(m.InstanceID)
		}
		w.WriteBytes(m.Metadata)
	}
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// SyncGroup (Key 14), v0-v4
// ---------------------------------------------------------------------------

// SyncGroupRequestAssignment is the leader's assignment for one member.
type SyncGroupRequestAssignment struct {
	MemberID   string
	Assignment []byte
}

// SyncGroupRequest asks to synchronize group assignments.
type SyncGroupRequest struct {
	Version      int16
	Group        string
	GenerationID int32
	MemberID     string
	InstanceID   *string
	Assignments  []SyncGroupRequestAssignment
}

// DecodeSyncGroupRequest parses the request body for versions up to 4.
func DecodeSyncGroupRequest(version int16, body []byte) (*SyncGroupRequest, error) {
	r := NewReader(body)
	req := &SyncGroupRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.GenerationID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MemberID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version >= 3 {
		if req.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if version >= 5 {
		if _, err = r.ReadNullableString(); err != nil { // protocol_type
			return nil, err
		}
		if _, err = r.ReadNullableString(); err != nil { // protocol_name
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Assignments = make([]SyncGroupRequestAssignment, 0, n)
	for i := 0; i < n; i++ {
		var a SyncGroupRequestAssignment
		if a.MemberID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if a.Assignment, err = r.ReadBytes(); err != nil {
			return nil, err
		}
		req.Assignments = append(req.Assignments, a)
	}
	return req, nil
}

// SyncGroupResponse returns the assignment for the requesting member.
type SyncGroupResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Assignment     []byte
}

// EncodeSyncGroupResponse serializes the response body.
func EncodeSyncGroupResponse(resp *SyncGroupResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if resp.Version >= 5 {
		w.WriteNullableString(nil) // protocol_type
		w.WriteNullableString(nil) // protocol_name
	}
	w.WriteBytes(resp.Assignment)
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// Heartbeat (Key 12), v0-v3
// ---------------------------------------------------------------------------

// HeartbeatRequest keeps a group member alive.
type HeartbeatRequest struct {
	Version      int16
	Group        string
	GenerationID int32
	MemberID     string
	InstanceID   *string
}

// DecodeHeartbeatRequest parses the request body for versions up to 3.
func DecodeHeartbeatRequest(version int16, body []byte) (*HeartbeatRequest, error) {
	r := NewReader(body)
	req := &HeartbeatRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.GenerationID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MemberID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version >= 3 {
		if req.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// HeartbeatResponse reports heartbeat status.
type HeartbeatResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
}

// EncodeHeartbeatResponse serializes the response body.
func EncodeHeartbeatResponse(resp *HeartbeatResponse) ([]byte, error) {
	w := NewWriter(16)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// LeaveGroup (Key 13), v0-v3
// ---------------------------------------------------------------------------

// LeaveGroupRequestMember identifies a member leaving.
type LeaveGroupRequestMember struct {
	MemberID   string
	InstanceID *string
}

// LeaveGroupRequest asks to leave a group.
type LeaveGroupRequest struct {
	Version  int16
	Group    string
	MemberID string
	Members  []LeaveGroupRequestMember
}

// DecodeLeaveGroupRequest parses the request body for versions up to 3.
func DecodeLeaveGroupRequest(version int16, body []byte) (*LeaveGroupRequest, error) {
	r := NewReader(body)
	req := &LeaveGroupRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version < 3 {
		if req.MemberID, err = r.ReadString(); err != nil {
			return nil, err
		}
		return req, nil
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Members = make([]LeaveGroupRequestMember, 0, n)
	for i := 0; i < n; i++ {
		var m LeaveGroupRequestMember
		if m.MemberID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if m.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
		if version >= 5 {
			if _, err = r.ReadNullableString(); err != nil { // reason
				return nil, err
			}
		}
		req.Members = append(req.Members, m)
	}
	return req, nil
}

// LeaveGroupResponse reports members that left.
type LeaveGroupResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Members        []LeaveGroupResponseMember
}

// LeaveGroupResponseMember is the per-member leave result.
type LeaveGroupResponseMember struct {
	MemberID   string
	InstanceID *string
}

// EncodeLeaveGroupResponse serializes the response body.
func EncodeLeaveGroupResponse(resp *LeaveGroupResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if resp.Version >= 3 {
		w.WriteArrayLen(len(resp.Members))
		for _, m := range resp.Members {
			w.WriteString(m.MemberID)
			w.WriteNullableString(m.InstanceID)
			if resp.Version >= 4 {
				w.WriteInt16(ErrNone) // member error_code
			}
		}
	}
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// OffsetCommit (Key 8), v0-v7
// ---------------------------------------------------------------------------

// OffsetCommitRequestPartition commits one partition offset.
type OffsetCommitRequestPartition struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
}

// OffsetCommitRequestTopic groups commit partitions by topic.
type OffsetCommitRequestTopic struct {
	Topic      string
	Partitions []OffsetCommitRequestPartition
}

// OffsetCommitRequest commits consumer group offsets.
type OffsetCommitRequest struct {
	Version    int16
	Group      string
	Generation int32
	MemberID   string
	InstanceID *string
	Topics     []OffsetCommitRequestTopic
}

// DecodeOffsetCommitRequest parses the request body for versions up to 7.
func DecodeOffsetCommitRequest(version int16, body []byte) (*OffsetCommitRequest, error) {
	r := NewReader(body)
	req := &OffsetCommitRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if req.Generation, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if req.MemberID, err = r.ReadString(); err != nil {
			return nil, err
		}
	}
	if version == 1 {
		if _, err = r.ReadInt64(); err != nil { // retention_time
			return nil, err
		}
	}
	if version >= 7 {
		if req.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if version >= 2 && version <= 4 {
		if _, err = r.ReadInt64(); err != nil { // retention_time
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]OffsetCommitRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t OffsetCommitRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]OffsetCommitRequestPartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p OffsetCommitRequestPartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.Offset, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version == 1 {
				if _, err = r.ReadInt64(); err != nil { // commit_timestamp
					return nil, err
				}
			}
			if version >= 6 {
				if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			if p.Metadata, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

// OffsetCommitResponseTopic groups commit results by topic.
type OffsetCommitResponseTopic struct {
	Topic      string
	Partitions []OffsetCommitResponsePartition
}

// OffsetCommitResponsePartition is the commit result for one partition.
type OffsetCommitResponsePartition struct {
	Partition int32
	ErrorCode int16
}

// OffsetCommitResponse is the result of a commit.
type OffsetCommitResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []OffsetCommitResponseTopic
}

// EncodeOffsetCommitResponse serializes the response body.
func EncodeOffsetCommitResponse(resp *OffsetCommitResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt16(p.ErrorCode)
		}
	}
	return w.Bytes(), nil
}

// ---------------------------------------------------------------------------
// OffsetFetch (Key 9), v0-v6
// ---------------------------------------------------------------------------

// OffsetFetchRequestTopic requests committed offsets for a topic's partitions.
type OffsetFetchRequestTopic struct {
	Topic      string
	Partitions []int32
}

// OffsetFetchRequest fetches committed group offsets.
type OffsetFetchRequest struct {
	Version int16
	Group   string
	Topics  []OffsetFetchRequestTopic // nil = all topics
}

// DecodeOffsetFetchRequest parses the request body for versions up to 6.
func DecodeOffsetFetchRequest(version int16, body []byte) (*OffsetFetchRequest, error) {
	r := NewReader(body)
	req := &OffsetFetchRequest{Version: version}
	var err error
	if req.Group, err = r.ReadString(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return req, nil // null topics => all
	}
	req.Topics = make([]OffsetFetchRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t OffsetFetchRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		if t.Partitions, err = readInt32Array(r); err != nil {
			return nil, err
		}
		req.Topics = append(req.Topics, t)
	}
	if version >= 7 {
		if _, err = r.ReadBool(); err != nil { // require_stable
			return nil, err
		}
	}
	return req, nil
}

// OffsetFetchResponseTopic groups fetch results by topic.
type OffsetFetchResponseTopic struct {
	Topic      string
	Partitions []OffsetFetchResponsePartition
}

// OffsetFetchResponsePartition is the fetched offset for one partition.
type OffsetFetchResponsePartition struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

// OffsetFetchResponse is the result of a fetch.
type OffsetFetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []OffsetFetchResponseTopic
	ErrorCode      int16
}

// EncodeOffsetFetchResponse serializes the response body.
func EncodeOffsetFetchResponse(resp *OffsetFetchResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt64(p.Offset)
			if resp.Version >= 5 {
				w.WriteInt32(p.LeaderEpoch)
			}
			w.WriteNullableString(p.Metadata)
			w.WriteInt16(p.ErrorCode)
		}
	}
	if resp.Version >= 2 {
		w.WriteInt16(resp.ErrorCode)
	}
	return w.Bytes(), nil
}
