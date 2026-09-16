package protocol

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
