package protocol

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
		b, err := decodeMetadataBroker(r, version)
		if err != nil {
			return nil, err
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
		t, err := decodeMetadataTopic(r, version)
		if err != nil {
			return nil, err
		}
		resp.Topics = append(resp.Topics, t)
	}

	if version >= 8 && version <= 10 {
		if resp.ClusterAuthorizedOperations, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func decodeMetadataBroker(r *Reader, version int16) (MetadataBroker, error) {
	var b MetadataBroker
	var err error
	if b.NodeID, err = r.ReadInt32(); err != nil {
		return b, err
	}
	if b.Host, err = r.ReadString(); err != nil {
		return b, err
	}
	if b.Port, err = r.ReadInt32(); err != nil {
		return b, err
	}
	if version >= 1 {
		if b.Rack, err = r.ReadNullableString(); err != nil {
			return b, err
		}
	}
	return b, nil
}

func decodeMetadataTopic(r *Reader, version int16) (MetadataTopic, error) {
	var t MetadataTopic
	var err error
	if t.ErrorCode, err = r.ReadInt16(); err != nil {
		return t, err
	}
	if t.Name, err = r.ReadString(); err != nil {
		return t, err
	}
	if version >= 1 {
		if t.IsInternal, err = r.ReadBool(); err != nil {
			return t, err
		}
	}
	pn, err := r.ReadArrayLen()
	if err != nil {
		return t, err
	}
	t.Partitions = make([]MetadataPartition, 0, pn)
	for j := 0; j < pn; j++ {
		p, err := decodeMetadataPartition(r, version)
		if err != nil {
			return t, err
		}
		t.Partitions = append(t.Partitions, p)
	}
	if version >= 8 {
		if t.AuthorizedOperations, err = r.ReadInt32(); err != nil {
			return t, err
		}
	}
	return t, nil
}

func decodeMetadataPartition(r *Reader, version int16) (MetadataPartition, error) {
	var p MetadataPartition
	var err error
	if p.ErrorCode, err = r.ReadInt16(); err != nil {
		return p, err
	}
	if p.Partition, err = r.ReadInt32(); err != nil {
		return p, err
	}
	if p.Leader, err = r.ReadInt32(); err != nil {
		return p, err
	}
	if version >= 7 {
		if p.LeaderEpoch, err = r.ReadInt32(); err != nil {
			return p, err
		}
	}
	if p.Replicas, err = readInt32Array(r); err != nil {
		return p, err
	}
	if p.ISR, err = readInt32Array(r); err != nil {
		return p, err
	}
	if version >= 5 {
		if p.OfflineReplicas, err = readInt32Array(r); err != nil {
			return p, err
		}
	}
	return p, nil
}
