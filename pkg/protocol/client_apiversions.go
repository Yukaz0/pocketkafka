package protocol

func EncodeApiVersionsRequest(version int16, req *ApiVersionsRequest) ([]byte, error) {
	return nil, nil
}

func DecodeApiVersionsResponse(version int16, body []byte) (*ApiVersionsResponse, error) {
	r := NewReader(body)
	resp := &ApiVersionsResponse{Version: version}
	var err error
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.ApiKeys = make([]ApiKeySupport, 0, n)
	for i := 0; i < n; i++ {
		var k ApiKeySupport
		if k.ApiKey, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if k.MinVersion, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if k.MaxVersion, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		resp.ApiKeys = append(resp.ApiKeys, k)
	}
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}
