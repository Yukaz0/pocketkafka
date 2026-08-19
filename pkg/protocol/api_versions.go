package protocol

// ApiVersionsRequest (Key 18). Versions 0-2 carry no request body fields, so
// this struct is a placeholder for the decoder.
type ApiVersionsRequest struct {
	Version int16
}

// ApiKeySupport describes the supported version range of one API key.
type ApiKeySupport struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
}

// ApiVersionsResponse (Key 18).
type ApiVersionsResponse struct {
	Version        int16
	ErrorCode      int16
	ApiKeys        []ApiKeySupport
	ThrottleTimeMs int32
}

// DecodeApiVersionsRequest parses the (empty) request body for versions 0-2.
func DecodeApiVersionsRequest(version int16, body []byte) (*ApiVersionsRequest, error) {
	req := &ApiVersionsRequest{Version: version}
	// For v0-v2 there is no body to read. Accept any trailing bytes silently.
	return req, nil
}

// EncodeApiVersionsResponse serializes the response body. Version 3 uses the
// flexible (compact) encoding with tagged fields.
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
