package protocol

// ProduceRequestTopic is one topic in a produce request.
type ProduceRequestTopic struct {
	Topic      string
	Partitions []ProduceRequestPartition
}

// ProduceRequestPartition is one partition's record batch.
type ProduceRequestPartition struct {
	Partition int32
	Records   []byte // raw RecordBatch bytes
}

// ProduceRequest (Key 0), decoded at v2.
type ProduceRequest struct {
	Version int16
	Acks    int16
	Timeout int32
	Topics  []ProduceRequestTopic
}

// DecodeProduceRequest parses the request body for versions up to 2.
func DecodeProduceRequest(version int16, body []byte) (*ProduceRequest, error) {
	r := NewReader(body)
	req := &ProduceRequest{Version: version}
	if version >= 3 {
		if _, err := r.ReadNullableString(); err != nil { // transactional_id
			return nil, err
		}
	}
	var err error
	if req.Acks, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if req.Timeout, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]ProduceRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t ProduceRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]ProduceRequestPartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p ProduceRequestPartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.Records, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

// ProduceResponsePartition is the response for one produced partition.
type ProduceResponsePartition struct {
	Partition     int32
	ErrorCode     int16
	BaseOffset    int64
	LogAppendTime int64
}

// ProduceResponseTopic groups partition responses by topic.
type ProduceResponseTopic struct {
	Topic      string
	Partitions []ProduceResponsePartition
}

// ProduceResponse (Key 0), encoded at v2.
type ProduceResponse struct {
	Version        int16
	Topics         []ProduceResponseTopic
	ThrottleTimeMs int32
}

// EncodeProduceResponse serializes the response body.
func EncodeProduceResponse(resp *ProduceResponse) ([]byte, error) {
	w := NewWriter(128)
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt16(p.ErrorCode)
			w.WriteInt64(p.BaseOffset)
			if resp.Version >= 2 {
				w.WriteInt64(p.LogAppendTime)
			}
			if resp.Version >= 5 {
				w.WriteInt64(-1) // log_start_offset
			}
		}
	}
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	return w.Bytes(), nil
}
