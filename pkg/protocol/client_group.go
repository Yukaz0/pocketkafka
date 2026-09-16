package protocol

func EncodeFindCoordinatorRequest(req *FindCoordinatorRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteString(req.Key)
	if req.Version >= 1 {
		w.WriteInt8(req.KeyType)
	}
	return w.Bytes(), nil
}

func DecodeFindCoordinatorResponse(version int16, body []byte) (*FindCoordinatorResponse, error) {
	r := NewReader(body)
	resp := &FindCoordinatorResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if resp.ErrorMessage, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if resp.NodeID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if resp.Host, err = r.ReadString(); err != nil {
		return nil, err
	}
	if resp.Port, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	return resp, nil
}

func EncodeJoinGroupRequest(req *JoinGroupRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteString(req.Group)
	w.WriteInt32(req.SessionTimeoutMs)
	if req.Version >= 1 {
		w.WriteInt32(req.RebalanceTimeoutMs)
	}
	w.WriteString(req.MemberID)
	if req.Version >= 5 {
		w.WriteNullableString(req.InstanceID)
	}
	w.WriteString(req.ProtocolType)
	w.WriteArrayLen(len(req.Protocols))
	for _, p := range req.Protocols {
		w.WriteString(p.Name)
		w.WriteBytes(p.Metadata)
	}
	return w.Bytes(), nil
}

func DecodeJoinGroupResponse(version int16, body []byte) (*JoinGroupResponse, error) {
	r := NewReader(body)
	resp := &JoinGroupResponse{Version: version}
	var err error
	if version >= 2 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if resp.GenerationID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 7 {
		if _, err = r.ReadNullableString(); err != nil { // protocol_type
			return nil, err
		}
		var pn *string
		if pn, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
		if pn != nil {
			resp.ProtocolName = *pn
		}
	} else {
		if resp.ProtocolName, err = r.ReadString(); err != nil {
			return nil, err
		}
	}
	if resp.LeaderID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if resp.MemberID, err = r.ReadString(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Members = make([]JoinGroupResponseMember, 0, n)
	for i := 0; i < n; i++ {
		var m JoinGroupResponseMember
		if m.MemberID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if version >= 5 {
			if m.InstanceID, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		if m.Metadata, err = r.ReadBytes(); err != nil {
			return nil, err
		}
		resp.Members = append(resp.Members, m)
	}
	return resp, nil
}

func EncodeSyncGroupRequest(req *SyncGroupRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteString(req.Group)
	w.WriteInt32(req.GenerationID)
	w.WriteString(req.MemberID)
	if req.Version >= 3 {
		w.WriteNullableString(req.InstanceID)
	}
	if req.Version >= 5 {
		w.WriteNullableString(nil) // protocol_type
		w.WriteNullableString(nil) // protocol_name
	}
	w.WriteArrayLen(len(req.Assignments))
	for _, a := range req.Assignments {
		w.WriteString(a.MemberID)
		w.WriteBytes(a.Assignment)
	}
	return w.Bytes(), nil
}

func DecodeSyncGroupResponse(version int16, body []byte) (*SyncGroupResponse, error) {
	r := NewReader(body)
	resp := &SyncGroupResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if version >= 5 {
		if _, err = r.ReadNullableString(); err != nil { // protocol_type
			return nil, err
		}
		if _, err = r.ReadNullableString(); err != nil { // protocol_name
			return nil, err
		}
	}
	if resp.Assignment, err = r.ReadBytes(); err != nil {
		return nil, err
	}
	return resp, nil
}

func EncodeHeartbeatRequest(req *HeartbeatRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteString(req.Group)
	w.WriteInt32(req.GenerationID)
	w.WriteString(req.MemberID)
	if req.Version >= 3 {
		w.WriteNullableString(req.InstanceID)
	}
	return w.Bytes(), nil
}

func DecodeHeartbeatResponse(version int16, body []byte) (*HeartbeatResponse, error) {
	r := NewReader(body)
	resp := &HeartbeatResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	return resp, nil
}

func EncodeOffsetCommitRequest(req *OffsetCommitRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteString(req.Group)
	if req.Version >= 1 {
		w.WriteInt32(req.Generation)
		w.WriteString(req.MemberID)
	}
	if req.Version == 1 {
		w.WriteInt64(0) // retention_time
	}
	if req.Version >= 7 {
		w.WriteNullableString(req.InstanceID)
	}
	if req.Version >= 2 && req.Version <= 4 {
		w.WriteInt64(0) // retention_time
	}
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt64(p.Offset)
			if req.Version == 1 {
				w.WriteInt64(0) // commit_timestamp
			}
			if req.Version >= 6 {
				w.WriteInt32(p.LeaderEpoch)
			}
			w.WriteNullableString(p.Metadata)
		}
	}
	return w.Bytes(), nil
}

func DecodeOffsetCommitResponse(version int16, body []byte) (*OffsetCommitResponse, error) {
	r := NewReader(body)
	resp := &OffsetCommitResponse{Version: version}
	var err error
	if version >= 3 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]OffsetCommitResponseTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t OffsetCommitResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]OffsetCommitResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p OffsetCommitResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

func EncodeOffsetFetchRequest(req *OffsetFetchRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteString(req.Group)
	if req.Topics == nil {
		w.WriteArrayLen(-1)
	} else {
		w.WriteArrayLen(len(req.Topics))
		for _, t := range req.Topics {
			w.WriteString(t.Topic)
			w.WriteArrayLen(len(t.Partitions))
			for _, p := range t.Partitions {
				w.WriteInt32(p)
			}
		}
	}
	if req.Version >= 7 {
		w.WriteBool(false) // require_stable
	}
	return w.Bytes(), nil
}

func DecodeOffsetFetchResponse(version int16, body []byte) (*OffsetFetchResponse, error) {
	r := NewReader(body)
	resp := &OffsetFetchResponse{Version: version}
	var err error
	if version >= 3 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]OffsetFetchResponseTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t OffsetFetchResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]OffsetFetchResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p OffsetFetchResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.Offset, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version >= 5 {
				if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			if p.Metadata, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	if version >= 2 {
		if resp.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}
