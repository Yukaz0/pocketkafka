package protocol

type FetchRequestPartition struct {
	Partition         int32
	FetchOffset       int64
	LogStartOffset    int64
	PartitionMaxBytes int32
}

type FetchRequestTopic struct {
	Topic      string
	Partitions []FetchRequestPartition
}

type FetchRequest struct {
	Version        int16
	ReplicaID      int32
	MaxWaitMillis  int32
	MinBytes       int32
	MaxBytes       int32
	IsolationLevel int8
	Topics         []FetchRequestTopic
}

func DecodeFetchRequest(version int16, body []byte) (*FetchRequest, error) {
	r := NewReader(body)
	req := &FetchRequest{Version: version}
	var err error
	if req.ReplicaID, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MaxWaitMillis, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if req.MinBytes, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 3 {
		if req.MaxBytes, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	} else {
		req.MaxBytes = 0x7fffffff
	}
	if version >= 4 {
		if req.IsolationLevel, err = r.ReadInt8(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]FetchRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t FetchRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]FetchRequestPartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p FetchRequestPartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.FetchOffset, err = r.ReadInt64(); err != nil {
				return nil, err
			}
			if version >= 5 {
				if p.LogStartOffset, err = r.ReadInt64(); err != nil {
					return nil, err
				}
			}
			if p.PartitionMaxBytes, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

type FetchResponsePartition struct {
	Partition        int32
	ErrorCode        int16
	HighWatermark    int64
	LastStableOffset int64
	LogStartOffset   int64
	Records          []byte
}

type FetchResponseTopic struct {
	Topic      string
	Partitions []FetchResponsePartition
}

type FetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Topics         []FetchResponseTopic
}

func EncodeFetchResponse(resp *FetchResponse) ([]byte, error) {
	w := NewWriter(256)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt16(p.ErrorCode)
			w.WriteInt64(p.HighWatermark)
			if resp.Version >= 4 {
				w.WriteInt64(p.LastStableOffset)
			}
			if resp.Version >= 5 {
				w.WriteInt64(p.LogStartOffset)
			}
			if resp.Version >= 4 {
				w.WriteArrayLen(-1)
			}
			if resp.Version >= 11 {
				w.WriteInt32(-1)
			}

			if len(p.Records) == 0 {
				w.WriteInt32(0)
			} else {
				w.WriteBytes(p.Records)
			}
		}
	}
	return w.Bytes(), nil
}
