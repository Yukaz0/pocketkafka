package protocol

func EncodeCreateTopicsRequest(req *CreateTopicsRequest) ([]byte, error) {
	w := NewWriter(128)
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteInt32(t.NumPartitions)
		w.WriteInt16(t.ReplicationFactor)
		w.WriteArrayLen(len(t.ReplicaAssignment))
		for _, ra := range t.ReplicaAssignment {
			w.WriteInt32(ra.Partition)
			w.WriteArrayLen(len(ra.Replicas))
			for _, r := range ra.Replicas {
				w.WriteInt32(r)
			}
		}
		w.WriteArrayLen(len(t.Configs))
		for _, c := range t.Configs {
			w.WriteString(c.Name)
			w.WriteNullableString(c.Value)
		}
	}
	w.WriteInt32(req.TimeoutMs)
	if req.Version >= 1 {
		w.WriteBool(req.ValidateOnly)
	}
	return w.Bytes(), nil
}

func DecodeCreateTopicsResponse(version int16, body []byte) (*CreateTopicsResponse, error) {
	r := NewReader(body)
	resp := &CreateTopicsResponse{Version: version}
	var err error
	if version >= 2 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]CreateTopicsResponseTopic, 0, n)
	for i := 0; i < n; i++ {
		var t CreateTopicsResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		if t.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if version >= 1 {
			if t.ErrorMessage, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

func EncodeDeleteTopicsRequest(req *DeleteTopicsRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteArrayLen(len(req.TopicNames))
	for _, n := range req.TopicNames {
		w.WriteString(n)
	}
	w.WriteInt32(req.TimeoutMs)
	return w.Bytes(), nil
}

func DecodeDeleteTopicsResponse(version int16, body []byte) (*DeleteTopicsResponse, error) {
	r := NewReader(body)
	resp := &DeleteTopicsResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]DeleteTopicsResponseTopic, 0, n)
	for i := 0; i < n; i++ {
		var t DeleteTopicsResponseTopic
		if t.Name, err = r.ReadString(); err != nil {
			return nil, err
		}
		if t.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if version >= 5 {
			if t.ErrorMessage, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}
