package protocol

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
