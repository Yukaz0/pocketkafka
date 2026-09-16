package protocol

func EncodeProduceRequest(req *ProduceRequest) ([]byte, error) {
	w := NewWriter(128)
	if req.Version >= 3 {
		w.WriteNullableString(nil) // transactional_id
	}
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
