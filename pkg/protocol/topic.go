package protocol

// CreateTopicsRequestTopic describes one topic to create.
type CreateTopicsRequestTopic struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int16
	ReplicaAssignment []CreateTopicsReplicaAssignment
	Configs           []CreateTopicsConfig
}

// CreateTopicsReplicaAssignment manually assigns replicas to a partition.
type CreateTopicsReplicaAssignment struct {
	Partition int32
	Replicas  []int32
}

// CreateTopicsConfig is a topic-level configuration override.
type CreateTopicsConfig struct {
	Name  string
	Value *string
}

// CreateTopicsRequest (Key 19), decoded at v4.
type CreateTopicsRequest struct {
	Version      int16
	Topics       []CreateTopicsRequestTopic
	TimeoutMs    int32
	ValidateOnly bool
}

// DecodeCreateTopicsRequest parses the request body for versions up to 4.
func DecodeCreateTopicsRequest(version int16, body []byte) (*CreateTopicsRequest, error) {
	r := NewReader(body)
	req := &CreateTopicsRequest{Version: version}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]CreateTopicsRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t CreateTopicsRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		if t.NumPartitions, err = r.ReadInt32(); err != nil {
			return nil, err
		}
		if t.ReplicationFactor, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		an, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.ReplicaAssignment = make([]CreateTopicsReplicaAssignment, 0, an)
		for j := 0; j < an; j++ {
			var ra CreateTopicsReplicaAssignment
			if ra.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if ra.Replicas, err = readInt32Array(r); err != nil {
				return nil, err
			}
			t.ReplicaAssignment = append(t.ReplicaAssignment, ra)
		}
		cn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Configs = make([]CreateTopicsConfig, 0, cn)
		for j := 0; j < cn; j++ {
			var c CreateTopicsConfig
			if c.Name, err = r.ReadString(); err != nil {
				return nil, err
			}
			if c.Value, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
			t.Configs = append(t.Configs, c)
		}
		req.Topics = append(req.Topics, t)
	}
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if req.ValidateOnly, err = r.ReadBool(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// CreateTopicsResponseTopic is the result for one topic creation.
type CreateTopicsResponseTopic struct {
	Topic        string
	ErrorCode    int16
	ErrorMessage *string
}

// CreateTopicsResponse (Key 19), encoded at v4.
type CreateTopicsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []CreateTopicsResponseTopic
}

// EncodeCreateTopicsResponse serializes the response body.
func EncodeCreateTopicsResponse(resp *CreateTopicsResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 2 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteInt16(t.ErrorCode)
		if resp.Version >= 1 {
			w.WriteNullableString(t.ErrorMessage)
		}
		if resp.Version >= 5 {
			w.WriteInt32(-1)    // num_partitions
			w.WriteInt16(-1)    // replication_factor
			w.WriteArrayLen(-1) // configs
		}
	}
	return w.Bytes(), nil
}

// DeleteTopicsRequest (Key 20), decoded at v3.
type DeleteTopicsRequest struct {
	Version    int16
	TopicNames []string
	TimeoutMs  int32
}

// DecodeDeleteTopicsRequest parses the request body for versions up to 3.
func DecodeDeleteTopicsRequest(version int16, body []byte) (*DeleteTopicsRequest, error) {
	r := NewReader(body)
	req := &DeleteTopicsRequest{Version: version}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.TopicNames = make([]string, 0, n)
	for i := 0; i < n; i++ {
		t, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		req.TopicNames = append(req.TopicNames, t)
	}
	if req.TimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	return req, nil
}

// DeleteTopicsResponseTopic is the result for one topic deletion.
type DeleteTopicsResponseTopic struct {
	Name         string
	ErrorCode    int16
	ErrorMessage *string
}

// DeleteTopicsResponse (Key 20), encoded at v3.
type DeleteTopicsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []DeleteTopicsResponseTopic
}

// EncodeDeleteTopicsResponse serializes the response body.
func EncodeDeleteTopicsResponse(resp *DeleteTopicsResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteNullableString(&t.Name)
		w.WriteInt16(t.ErrorCode)
		if resp.Version >= 5 {
			w.WriteNullableString(t.ErrorMessage)
		}
	}
	return w.Bytes(), nil
}

func readInt32Array(r *Reader) ([]int32, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	out := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		v, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
