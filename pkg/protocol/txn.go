package protocol

type AddPartitionsToTxnRequestTopic struct {
	Topic      string
	Partitions []int32
}

type AddPartitionsToTxnRequest struct {
	Version         int16
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	Topics          []AddPartitionsToTxnRequestTopic
}

func DecodeAddPartitionsToTxnRequest(version int16, body []byte) (*AddPartitionsToTxnRequest, error) {
	r := NewReader(body)
	req := &AddPartitionsToTxnRequest{Version: version}
	var err error
	if req.TransactionalID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.ProducerID, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if req.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.Topics = make([]AddPartitionsToTxnRequestTopic, 0, n)
	for i := 0; i < n; i++ {
		var t AddPartitionsToTxnRequestTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		if t.Partitions, err = readInt32Array(r); err != nil {
			return nil, err
		}
		req.Topics = append(req.Topics, t)
	}
	return req, nil
}

type AddPartitionsToTxnResponsePartition struct {
	Partition int32
	ErrorCode int16
}

type AddPartitionsToTxnResponseTopic struct {
	Topic      string
	Partitions []AddPartitionsToTxnResponsePartition
}

type AddPartitionsToTxnResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Topics         []AddPartitionsToTxnResponseTopic
}

func EncodeAddPartitionsToTxnResponse(resp *AddPartitionsToTxnResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteArrayLen(len(resp.Topics))
	for _, t := range resp.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p.Partition)
			w.WriteInt16(p.ErrorCode)
		}
	}
	return w.Bytes(), nil
}

func EncodeAddPartitionsToTxnRequest(req *AddPartitionsToTxnRequest) ([]byte, error) {
	w := NewWriter(64)
	w.WriteString(req.TransactionalID)
	w.WriteInt64(req.ProducerID)
	w.WriteInt16(req.ProducerEpoch)
	w.WriteArrayLen(len(req.Topics))
	for _, t := range req.Topics {
		w.WriteString(t.Topic)
		w.WriteArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			w.WriteInt32(p)
		}
	}
	return w.Bytes(), nil
}

func DecodeAddPartitionsToTxnResponse(version int16, body []byte) (*AddPartitionsToTxnResponse, error) {
	r := NewReader(body)
	resp := &AddPartitionsToTxnResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Topics = make([]AddPartitionsToTxnResponseTopic, 0, n)
	for i := 0; i < n; i++ {
		var t AddPartitionsToTxnResponseTopic
		if t.Topic, err = r.ReadString(); err != nil {
			return nil, err
		}
		pn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		t.Partitions = make([]AddPartitionsToTxnResponsePartition, 0, pn)
		for j := 0; j < pn; j++ {
			var p AddPartitionsToTxnResponsePartition
			if p.Partition, err = r.ReadInt32(); err != nil {
				return nil, err
			}
			if p.ErrorCode, err = r.ReadInt16(); err != nil {
				return nil, err
			}
			t.Partitions = append(t.Partitions, p)
		}
		resp.Topics = append(resp.Topics, t)
	}
	return resp, nil
}

type AddOffsetsToTxnRequest struct {
	Version         int16
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	GroupID         string
}

func DecodeAddOffsetsToTxnRequest(version int16, body []byte) (*AddOffsetsToTxnRequest, error) {
	r := NewReader(body)
	req := &AddOffsetsToTxnRequest{Version: version}
	var err error
	if req.TransactionalID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.ProducerID, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if req.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if req.GroupID, err = r.ReadString(); err != nil {
		return nil, err
	}
	return req, nil
}

type AddOffsetsToTxnResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
}

func EncodeAddOffsetsToTxnResponse(resp *AddOffsetsToTxnResponse) ([]byte, error) {
	w := NewWriter(16)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	return w.Bytes(), nil
}

func EncodeAddOffsetsToTxnRequest(req *AddOffsetsToTxnRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteString(req.TransactionalID)
	w.WriteInt64(req.ProducerID)
	w.WriteInt16(req.ProducerEpoch)
	w.WriteString(req.GroupID)
	return w.Bytes(), nil
}

func DecodeAddOffsetsToTxnResponse(version int16, body []byte) (*AddOffsetsToTxnResponse, error) {
	r := NewReader(body)
	resp := &AddOffsetsToTxnResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	return resp, nil
}

type EndTxnRequest struct {
	Version         int16
	TransactionalID string
	ProducerID      int64
	ProducerEpoch   int16
	Committed       bool
}

func DecodeEndTxnRequest(version int16, body []byte) (*EndTxnRequest, error) {
	r := NewReader(body)
	req := &EndTxnRequest{Version: version}
	var err error
	if req.TransactionalID, err = r.ReadString(); err != nil {
		return nil, err
	}
	if req.ProducerID, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if req.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if req.Committed, err = r.ReadBool(); err != nil {
		return nil, err
	}
	return req, nil
}

type EndTxnResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
}

func EncodeEndTxnResponse(resp *EndTxnResponse) ([]byte, error) {
	w := NewWriter(16)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	return w.Bytes(), nil
}

func EncodeEndTxnRequest(req *EndTxnRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteString(req.TransactionalID)
	w.WriteInt64(req.ProducerID)
	w.WriteInt16(req.ProducerEpoch)
	w.WriteBool(req.Committed)
	return w.Bytes(), nil
}

func DecodeEndTxnResponse(version int16, body []byte) (*EndTxnResponse, error) {
	r := NewReader(body)
	resp := &EndTxnResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	return resp, nil
}
