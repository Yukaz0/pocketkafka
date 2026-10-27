package protocol

type SASLHandshakeRequest struct {
	Version   int16
	Mechanism string
}

func DecodeSASLHandshakeRequest(version int16, body []byte) (*SASLHandshakeRequest, error) {
	r := NewReader(body)
	m, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &SASLHandshakeRequest{Version: version, Mechanism: m}, nil
}

func EncodeSASLHandshakeRequest(req *SASLHandshakeRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteString(req.Mechanism)
	return w.Bytes(), nil
}

type SASLHandshakeResponse struct {
	Version    int16
	ErrorCode  int16
	Mechanisms []string
}

func EncodeSASLHandshakeResponse(resp *SASLHandshakeResponse) ([]byte, error) {
	w := NewWriter(32)
	w.WriteInt16(resp.ErrorCode)
	if resp.Mechanisms == nil {
		w.WriteArrayLen(-1)
	} else {
		w.WriteArrayLen(len(resp.Mechanisms))
		for _, m := range resp.Mechanisms {
			w.WriteString(m)
		}
	}
	return w.Bytes(), nil
}

func DecodeSASLHandshakeResponse(version int16, body []byte) (*SASLHandshakeResponse, error) {
	r := NewReader(body)
	resp := &SASLHandshakeResponse{Version: version}
	var err error
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Mechanisms = make([]string, 0, n)
	for i := 0; i < n; i++ {
		m, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		resp.Mechanisms = append(resp.Mechanisms, m)
	}
	return resp, nil
}

type SaslAuthenticateRequest struct {
	Version   int16
	AuthBytes []byte
}

func DecodeSaslAuthenticateRequest(version int16, body []byte) (*SaslAuthenticateRequest, error) {
	r := NewReader(body)
	b, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}
	return &SaslAuthenticateRequest{Version: version, AuthBytes: b}, nil
}

func EncodeSaslAuthenticateRequest(req *SaslAuthenticateRequest) ([]byte, error) {
	w := NewWriter(len(req.AuthBytes) + 8)
	w.WriteBytes(req.AuthBytes)
	return w.Bytes(), nil
}

type SaslAuthenticateResponse struct {
	Version           int16
	ErrorCode         int16
	ErrorMessage      *string
	AuthBytes         []byte
	SessionLifetimeMs int64
}

func EncodeSaslAuthenticateResponse(resp *SaslAuthenticateResponse) ([]byte, error) {
	w := NewWriter(64)
	w.WriteInt16(resp.ErrorCode)
	w.WriteNullableString(resp.ErrorMessage)

	if len(resp.AuthBytes) == 0 {
		w.WriteInt32(0)
	} else {
		w.WriteBytes(resp.AuthBytes)
	}
	if resp.Version >= 1 {
		w.WriteInt64(resp.SessionLifetimeMs)
	}
	return w.Bytes(), nil
}

func DecodeSaslAuthenticateResponse(version int16, body []byte) (*SaslAuthenticateResponse, error) {
	r := NewReader(body)
	resp := &SaslAuthenticateResponse{Version: version}
	var err error
	if resp.ErrorCode, err = r.ReadInt16(); err != nil {
		return nil, err
	}
	if resp.ErrorMessage, err = r.ReadNullableString(); err != nil {
		return nil, err
	}
	if resp.AuthBytes, err = r.ReadBytes(); err != nil {
		return nil, err
	}
	if version >= 1 {
		if resp.SessionLifetimeMs, err = r.ReadInt64(); err != nil {
			return nil, err
		}
	}
	return resp, nil
}
