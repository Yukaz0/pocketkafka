package protocol

type ApiVersionsRequest struct {
	Version int16
}

type ApiKeySupport struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsResponse struct {
	Version        int16
	ErrorCode      int16
	ApiKeys        []ApiKeySupport
	ThrottleTimeMs int32
}

func DecodeApiVersionsRequest(version int16, body []byte) (*ApiVersionsRequest, error) {
	req := &ApiVersionsRequest{Version: version}

	return req, nil
}

func EncodeApiVersionsResponse(resp *ApiVersionsResponse) ([]byte, error) {
	if resp.Version >= 3 {
		w := NewWriter(96)
		w.WriteInt16(resp.ErrorCode)
		w.WriteCompactArrayLen(len(resp.ApiKeys))
		for _, k := range resp.ApiKeys {
			w.WriteInt16(k.ApiKey)
			w.WriteInt16(k.MinVersion)
			w.WriteInt16(k.MaxVersion)
			w.WriteTaggedFields()
		}
		w.WriteInt32(resp.ThrottleTimeMs)
		w.WriteTaggedFields()
		return w.Bytes(), nil
	}
	w := NewWriter(64)
	w.WriteInt16(resp.ErrorCode)
	w.WriteArrayLen(len(resp.ApiKeys))
	for _, k := range resp.ApiKeys {
		w.WriteInt16(k.ApiKey)
		w.WriteInt16(k.MinVersion)
		w.WriteInt16(k.MaxVersion)
	}
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	return w.Bytes(), nil
}
