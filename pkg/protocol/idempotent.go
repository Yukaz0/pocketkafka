package protocol

type InitProducerIdRequest struct {
	Version              int16
	TransactionalID      *string
	TransactionTimeoutMs int32
	ProducerID           int64
	ProducerEpoch        int16
}

func DecodeInitProducerIdRequest(version int16, body []byte) (*InitProducerIdRequest, error) {
	r := NewReader(body)
	req := &InitProducerIdRequest{Version: version}
	var err error
	if req.TransactionalID, err = r.ReadNullableString(); err != nil {
		return nil, err
	}
	if req.TransactionTimeoutMs, err = r.ReadInt32(); err != nil {
		return nil, err
	}
	if version >= 2 {
		if req.ProducerID, err = r.ReadInt64(); err != nil {
			return nil, err
		}
		if req.ProducerEpoch, err = r.ReadInt16(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

type InitProducerIdResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	ProducerID     int64
	ProducerEpoch  int16
}

func EncodeInitProducerIdResponse(resp *InitProducerIdResponse) ([]byte, error) {
	w := NewWriter(32)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteInt64(resp.ProducerID)
	w.WriteInt16(resp.ProducerEpoch)
	return w.Bytes(), nil
}

func EncodeInitProducerIdRequest(req *InitProducerIdRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteNullableString(req.TransactionalID)
	w.WriteInt32(req.TransactionTimeoutMs)
	if req.Version >= 2 {
		w.WriteInt64(req.ProducerID)
		w.WriteInt16(req.ProducerEpoch)
	}
	return w.Bytes(), nil
}

func DecodeInitProducerIdResponse(version int16, body []byte) (*InitProducerIdResponse, error) {
	r := NewReader(body)
	resp := &InitProducerIdResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if resp.ProducerID, err = r.ReadInt64(); err != nil {
		return nil, err
	}
	if resp.ProducerEpoch, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	return resp, nil
}
