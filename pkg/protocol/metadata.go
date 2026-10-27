package protocol

type MetadataRequestTopic struct {
	Topic string
}

type MetadataRequest struct {
	Version                            int16
	Topics                             []MetadataRequestTopic
	AllowAutoTopicCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
}

func DecodeMetadataRequest(version int16, body []byte) (*MetadataRequest, error) {
	r := NewReader(body)
	req := &MetadataRequest{Version: version}

	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if n >= 0 {
		req.Topics = make([]MetadataRequestTopic, 0, n)
		for i := 0; i < n; i++ {
			t, err := r.ReadString()
			if err != nil {
				return nil, err
			}
			req.Topics = append(req.Topics, MetadataRequestTopic{Topic: t})
		}
	}
	if version >= 4 {
		if req.AllowAutoTopicCreation, err = r.ReadBool(); err != nil {
			return nil, err
		}
	}
	if version >= 8 {
		if req.IncludeClusterAuthorizedOperations, err = r.ReadBool(); err != nil {
			return nil, err
		}
		if req.IncludeTopicAuthorizedOperations, err = r.ReadBool(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

type MetadataBroker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   *string
}

type MetadataPartition struct {
	ErrorCode       int16
	Partition       int32
	Leader          int32
	LeaderEpoch     int32
	Replicas        []int32
	ISR             []int32
	OfflineReplicas []int32
}

type MetadataTopic struct {
	ErrorCode            int16
	Name                 string
	IsInternal           bool
	Partitions           []MetadataPartition
	AuthorizedOperations int32
}

type MetadataResponse struct {
	Version                     int16
	ThrottleTimeMs              int32
	Brokers                     []MetadataBroker
	ClusterID                   *string
	ControllerID                int32
	Topics                      []MetadataTopic
	ClusterAuthorizedOperations int32
}

func EncodeMetadataResponse(resp *MetadataResponse) ([]byte, error) {
	w := NewWriter(256)
	if resp.Version >= 3 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Brokers))
	for _, b := range resp.Brokers {
		w.WriteInt32(b.NodeID)
		w.WriteString(b.Host)
		w.WriteInt32(b.Port)
		if resp.Version >= 1 {
			w.WriteNullableString(b.Rack)
		}
	}
	if resp.Version >= 2 {
		w.WriteNullableString(resp.ClusterID)
	}
	if resp.Version >= 1 {
		w.WriteInt32(resp.ControllerID)
	}
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteInt16(t.ErrorCode)
		w.WriteString(t.Name)
		if resp.Version >= 1 {
			w.WriteBool(t.IsInternal)
		}
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt16(p.ErrorCode)
			w.WriteInt32(p.Partition)
			w.WriteInt32(p.Leader)
			if resp.Version >= 7 {
				w.WriteInt32(p.LeaderEpoch)
			}
			writeInt32Array(w, p.Replicas)
			writeInt32Array(w, p.ISR)
			if resp.Version >= 5 {
				writeInt32Array(w, p.OfflineReplicas)
			}
		}
		if resp.Version >= 8 {
			w.WriteInt32(t.AuthorizedOperations)
		}
	}
	if resp.Version >= 8 && resp.Version <= 10 {
		w.WriteInt32(resp.ClusterAuthorizedOperations)
	}
	return w.Bytes(), nil
}

func writeInt32Array(w *Writer, arr []int32) {
	if arr == nil {
		w.WriteArrayLen(-1)
		return
	}
	w.WriteArrayLen(len(arr))
	for _, v := range arr {
		w.WriteInt32(v)
	}
}
