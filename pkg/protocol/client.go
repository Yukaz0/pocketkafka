package protocol

// This file contains the client-side codecs: encoding requests and decoding
// responses. They mirror the server-side codecs in the same package.

// ---------------------------------------------------------------------------
// ApiVersions
// ---------------------------------------------------------------------------

// EncodeApiVersionsRequest returns an empty body for versions 0-2.
func EncodeApiVersionsRequest(version int16, req *ApiVersionsRequest) ([]byte, error) {
	return nil, nil
}

// DecodeApiVersionsResponse parses a response (versions 0-2).
func DecodeApiVersionsResponse(version int16, body []byte) (*ApiVersionsResponse, error) {
	r := NewReader(body)
	resp := &ApiVersionsResponse{Version: version}
	var err error
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.ApiKeys = make([]ApiKeySupport, 0, n)
	for i := 0; i < n; i++ {
		var k ApiKeySupport
		if k.ApiKey, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if k.MinVersion, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if k.MaxVersion, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		resp.ApiKeys = append(resp.ApiKeys, k)
	}
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Metadata
// ---------------------------------------------------------------------------

// EncodeMetadataRequest serializes the request body.
func EncodeMetadataRequest(req *MetadataRequest) ([]byte, error) {
	w := NewWriter(64)
	if req.Topics == nil {
		w.WriteArrayLen(-1) // all topics
	} else {
		w.WriteArrayLen(len(req.Topics))
		for _, t := range req.Topics {
			w.WriteString(t.Topic)
		}
	}
	if req.Version >= 4 {
		w.WriteBool(req.AllowAutoTopicCreation)
	}
	if req.Version >= 8 {
		w.WriteBool(req.IncludeClusterAuthorizedOperations)
		w.WriteBool(req.IncludeTopicAuthorizedOperations)
	}
	return w.Bytes(), nil
}

// DecodeMetadataResponse parses the response body.
func DecodeMetadataResponse(version int16, body []byte) (*MetadataResponse, error) {
	r := NewReader(body)
	resp := &MetadataResponse{Version: version}
	var err error
	if version >= 3 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	bn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Brokers = make([]MetadataBroker, 0, bn)
	for i := 0; i < bn; i++ {
		var b MetadataBroker
		if b.NodeID, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if b.Host, err = r.ReadString(); err != nil {
			return nil, err
		}
		if b.Port, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if version >= 1 {
			if b.Rack, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		resp.Brokers = append(resp.Brokers, b)
	}
	if version >= 2 {
		if resp.ClusterID, err = r.ReadNullableString(); err != nil {
			return nil, err
		}
	}
	if version >= 1 {
		if resp.ControllerID, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]MetadataTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t MetadataTopic
		if t.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if t.Name, err = r.ReadString(); err != nil {
			return nil, err
		}
		if version >= 1 {
			if t.IsInternal, err = r.ReadBool(); err != nil {
				return nil, err
			}
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]MetadataPartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p MetadataPartition
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.Leader, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if version >= 7 {
				if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			if p.Replicas, err = readInt32Array(r); err != nil {
				return nil, err
			}
			if p.ISR, err = readInt32Array(r); err != nil {
				return nil, err
			}
			if version >= 5 {
				if p.OfflineReplicas, err = readInt32Array(r); err != nil {
					return nil, err
				}
			}
			t.Partitions = append(t.Partitions, p)
		}
		if version >= 8 {
			if t.AuthorizedOperations, err = r.ReadInt32(); err != nil {
				return nil, err
			}
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Produce
// ---------------------------------------------------------------------------

// EncodeProduceRequest serializes the request body.
func EncodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteInt16(req.Acks)
	w.WriteInt32(req.Timeout)
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteBytes(p.Records)
		}
	}
	return w.Bytes(), nil
}

// DecodeProduceResponse parses the response body.
func DecodeProduceResponse(version int16, body []byte) (*ProduceResponse, error) {
	r := NewReader(body)
	resp := &ProduceResponse{Version: version}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]ProduceResponseTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t ProduceResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]ProduceResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p ProduceResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			if p.BaseOffset, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version >= 2 {
				if p.LogAppendTime, err = r.ReadInt64(); err != nil {
					return nil, err
				}
			}
			if version >= 5 {
				if _, err = r.ReadInt64(); err != nil { // log_start_offset
					return nil, err
				}
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Fetch
// ---------------------------------------------------------------------------

// EncodeFetchRequest serializes the request body.
func EncodeFetchRequest(req *FetchRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteInt32(req.ReplicaID)
	w.WriteInt32(req.MaxWaitMillis)
	w.WriteInt32(req.MinBytes)
	if req.Version >= 3 {
		w.WriteInt32(req.MaxBytes)
	}
	if req.Version >= 4 {
		w.WriteInt8(req.IsolationLevel)
	}
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt64(p.FetchOffset)
			if req.Version >= 5 {
				w.WriteInt64(p.LogStartOffset)
			}
			w.WriteInt32(p.PartitionMaxBytes)
		}
	}
	return w.Bytes(), nil
}

// DecodeFetchResponse parses the response body.
func DecodeFetchResponse(version int16, body []byte) (*FetchResponse, error) {
	r := NewReader(body)
	resp := &FetchResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]FetchResponseTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t FetchResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]FetchResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p FetchResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			if p.HighWatermark, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version >= 4 {
				if p.LastStableOffset, err = r.ReadInt64(); err != nil {
					return nil, err
				}
			}
			if version >= 5 {
				if p.LogStartOffset, err = r.ReadInt64(); err != nil {
					return nil, err
				}
			}
			if version >= 4 {
				if _, err = r.ReadArrayLen(); err != nil { // aborted_transactions
					return nil, err
				}
			}
			if version >= 11 {
				if _, err = r.ReadInt32(); err != nil { // preferred_read_replica
					return nil, err
				}
			}
			if p.Records, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// ListOffsets
// ---------------------------------------------------------------------------

// EncodeListOffsetsRequest serializes the request body.
func EncodeListOffsetsRequest(req *ListOffsetsRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteInt32(req.ReplicaID)
	if req.Version >= 2 {
		w.WriteInt8(req.IsolationLevel)
	}
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			if req.Version >= 4 {
				w.WriteInt32(p.CurrentLeaderEpoch)
			}
			w.WriteInt64(p.Timestamp)
			if req.Version == 0 {
				w.WriteInt32(p.MaxNumOffsets)
			}
		}
	}
	return w.Bytes(), nil
}

// DecodeListOffsetsResponse parses the response body.
func DecodeListOffsetsResponse(version int16, body []byte) (*ListOffsetsResponse, error) {
	r := NewReader(body)
	resp := &ListOffsetsResponse{Version: version}
	var err error
	if version >= 2 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	tn, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]ListOffsetsResponseTopic, 0, tn)
	for i := 0; i < tn; i++ {
		var t ListOffsetsResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]ListOffsetsResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p ListOffsetsResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			if version == 0 {
				if p.OldStyleOffsets, err = readInt64Array(r); err != nil {
					return nil, err
				}
				t.Partitions = append(t.Partitions, p)
				continue
			}
			if version >= 1 {
				if p.Timestamp, err = r.ReadInt64(); err != nil {
					return nil, err
				}
				if p.Offset, err = r.ReadInt64(); err != nil {
					return nil, err
				}
			}
			if version >= 4 {
				if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

// EncodeFindCoordinatorRequest serializes the request body.
func EncodeFindCoordinatorRequest(req *FindCoordinatorRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteString(req.Key)
	if req.Version >= 1 {
		w.WriteInt8(req.KeyType)
	}
	return w.Bytes(), nil
}

// DecodeFindCoordinatorResponse parses the response body.
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

// EncodeJoinGroupRequest serializes the request body.
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

// DecodeJoinGroupResponse parses the response body.
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

// EncodeSyncGroupRequest serializes the request body.
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

// DecodeSyncGroupResponse parses the response body.
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

// EncodeHeartbeatRequest serializes the request body.
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

// DecodeHeartbeatResponse parses the response body.
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

// EncodeOffsetCommitRequest serializes the request body.
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

// DecodeOffsetCommitResponse parses the response body.
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

// EncodeOffsetFetchRequest serializes the request body.
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

// DecodeOffsetFetchResponse parses the response body.
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

func readInt64Array(r *Reader) ([]int64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadInt64()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
