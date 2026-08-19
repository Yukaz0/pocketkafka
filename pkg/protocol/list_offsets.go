package protocol

// ListOffsetsRequestPartition is one partition offset lookup.
type ListOffsetsRequestPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	Timestamp          int64
	MaxNumOffsets      int32
}

// ListOffsetsRequestTopic groups offset lookups by topic.
type ListOffsetsRequestTopic struct {
	Topic      string
	Partitions []ListOffsetsRequestPartition
}

// ListOffsetsRequest (Key 2), decoded at v5.
type ListOffsetsRequest struct {
	Version        int16
	ReplicaID      int32
	IsolationLevel int8
	Topics         []ListOffsetsRequestTopic
}

// DecodeListOffsetsRequest parses the request body for versions up to 5.
func DecodeListOffsetsRequest(version int16, body []byte) (*ListOffsetsRequest, error) {
	r := NewReader(body)
	req := &ListOffsetsRequest{Version: version}
	var err error
	if req.ReplicaID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 2 {
		if req.IsolationLevel, err = r.ReadInt8(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]ListOffsetsRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t ListOffsetsRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]ListOffsetsRequestPartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p ListOffsetsRequestPartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if version >= 4 {
				if p.CurrentLeaderEpoch, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			if p.Timestamp, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version == 0 {
				if p.MaxNumOffsets, err = r.ReadInt32(); err != nil {
					return nil, err
				}
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

// ListOffsetsResponsePartition is the offset result for one partition.
type ListOffsetsResponsePartition struct {
	Partition       int32
	ErrorCode       int16
	OldStyleOffsets []int64
	Timestamp       int64
	Offset          int64
	LeaderEpoch     int32
}

// ListOffsetsResponseTopic groups offset results by topic.
type ListOffsetsResponseTopic struct {
	Topic      string
	Partitions []ListOffsetsResponsePartition
}

// ListOffsetsResponse (Key 2), encoded at v5.
type ListOffsetsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []ListOffsetsResponseTopic
}

// EncodeListOffsetsResponse serializes the response body.
func EncodeListOffsetsResponse(resp *ListOffsetsResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt16(p.ErrorCode)
			if resp.Version == 0 {
				if p.OldStyleOffsets == nil {
					w.WriteArrayLen(-1)
				} else {
					w.WriteArrayLen(len(p.OldStyleOffsets))
					for _, o := range p.OldStyleOffsets {
						w.WriteInt64(o)
					}
				}
				continue
			}
			if resp.Version >= 1 {
				w.WriteInt64(p.Timestamp)
				w.WriteInt64(p.Offset)
			}
			if resp.Version >= 4 {
				w.WriteInt32(p.LeaderEpoch)
			}
		}
	}
	return w.Bytes(), nil
}
