package protocol

type FindCoordinatorRequest struct {
	Version int16
	Key     string
	KeyType int8
}

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

type FindCoordinatorResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   *string
	NodeID         int32
	Host           string
	Port           int32
}

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

type JoinGroupRequestProtocol struct {
	Name     string
	Metadata []byte
}

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

type JoinGroupResponseMember struct {
	MemberID   string
	InstanceID *string
	Metadata   []byte
}

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

func EncodeJoinGroupResponse(resp *JoinGroupResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt32(resp.GenerationID)
	if resp.Version >= 7 {
		w.WriteNullableString(nil)
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

		if len(m.Metadata) == 0 {
			w.WriteInt32(0)
		} else {
			w.WriteBytes(m.Metadata)
		}
	}
	return w.Bytes(), nil
}

type SyncGroupRequestAssignment struct {
	MemberID   string
	Assignment []byte
}

type SyncGroupRequest struct {
	Version      int16
	Group        string
	GenerationID int32
	MemberID     string
	InstanceID   *string
	Assignments  []SyncGroupRequestAssignment
}

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
		if _, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
		if _, err = r.ReadNullableString(); err != nil {
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

type SyncGroupResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Assignment     []byte
}

func EncodeSyncGroupResponse(resp *SyncGroupResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	if resp.Version >= 5 {
		w.WriteNullableString(nil)
		w.WriteNullableString(nil)
	}

	if len(resp.Assignment) == 0 {
		w.WriteInt32(0)
	} else {
		w.WriteBytes(resp.Assignment)
	}
	return w.Bytes(), nil
}

type HeartbeatRequest struct {
	Version      int16
	Group        string
	GenerationID int32
	MemberID     string
	InstanceID   *string
}

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

type HeartbeatResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
}

func EncodeHeartbeatResponse(resp *HeartbeatResponse) ([]byte, error) {
	w := NewWriter(16)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	return w.Bytes(), nil
}

type LeaveGroupRequestMember struct {
	MemberID   string
	InstanceID *string
}

type LeaveGroupRequest struct {
	Version  int16
	Group    string
	MemberID string
	Members  []LeaveGroupRequestMember
}

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
			if _, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		req.Members = append(req.Members, m)
	}
	return req, nil
}

type LeaveGroupResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Members        []LeaveGroupResponseMember
}

type LeaveGroupResponseMember struct {
	MemberID   string
	InstanceID *string
}

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
				w.WriteInt16(ErrNone)
			}
		}
	}
	return w.Bytes(), nil
}

type OffsetCommitRequestPartition struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
}

type OffsetCommitRequestTopic struct {
	Topic      string
	Partitions []OffsetCommitRequestPartition
}

type OffsetCommitRequest struct {
	Version    int16
	Group      string
	Generation int32
	MemberID   string
	InstanceID *string
	Topics     []OffsetCommitRequestTopic
}

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
		if _, err = r.ReadInt64(); err != nil {
			return nil, err
		}
	}
	if version >= 7 {
		if req.InstanceID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if version >= 2 && version <= 4 {
		if _, err = r.ReadInt64(); err != nil {
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
				if _, err = r.ReadInt64(); err != nil {
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

type OffsetCommitResponseTopic struct {
	Topic      string
	Partitions []OffsetCommitResponsePartition
}

type OffsetCommitResponsePartition struct {
	Partition int32
	ErrorCode int16
}

type OffsetCommitResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []OffsetCommitResponseTopic
}

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

type OffsetFetchRequestTopic struct {
	Topic      string
	Partitions []int32
}

type OffsetFetchRequest struct {
	Version int16
	Group   string
	Topics  []OffsetFetchRequestTopic
}

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
		return req, nil
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
		if _, err = r.ReadBool(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

type OffsetFetchResponseTopic struct {
	Topic      string
	Partitions []OffsetFetchResponsePartition
}

type OffsetFetchResponsePartition struct {
	Partition   int32
	Offset      int64
	LeaderEpoch int32
	Metadata    *string
	ErrorCode   int16
}

type OffsetFetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []OffsetFetchResponseTopic
	ErrorCode      int16
}

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
