package protocol

// ---------------------------------------------------------------------------
// ListGroups (Key 16), v0-v3
// ---------------------------------------------------------------------------

// ListGroupsRequest lists all consumer groups.
type ListGroupsRequest struct {
	Version int16
}

// DecodeListGroupsRequest parses the request body (empty for v0-v3).
func DecodeListGroupsRequest(version int16, body []byte) (*ListGroupsRequest, error) {
	return &ListGroupsRequest{Version: version}, nil
}

// ListGroupsResponseGroup is one group in the list.
type ListGroupsResponseGroup struct {
	GroupID      string
	ProtocolType string
}

// ListGroupsResponse reports the group IDs known to the broker.
type ListGroupsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	ErrorCode      int16
	Groups         []ListGroupsResponseGroup
}

// EncodeListGroupsResponse serializes the response body.
func EncodeListGroupsResponse(resp *ListGroupsResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteInt16(resp.ErrorCode)
	w.WriteArrayLen(len(resp.Groups))
	for _, g := range resp.Groups {
		w.WriteString(g.GroupID)
		w.WriteString(g.ProtocolType)
	}
	return w.Bytes(), nil
}

// EncodeListGroupsRequest serializes the request body (client side).
func EncodeListGroupsRequest(req *ListGroupsRequest) ([]byte, error) {
	return nil, nil
}

// DecodeListGroupsResponse parses the response body (client side).
func DecodeListGroupsResponse(version int16, body []byte) (*ListGroupsResponse, error) {
	r := NewReader(body)
	resp := &ListGroupsResponse{Version: version}
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
	resp.Groups = make([]ListGroupsResponseGroup, 0, n)
	for i := 0; i < n; i++ {
		var g ListGroupsResponseGroup
		if g.GroupID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if g.ProtocolType, err = r.ReadString(); err != nil {
			return nil, err
		}
		resp.Groups = append(resp.Groups, g)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// DescribeGroups (Key 15), v0-v3
// ---------------------------------------------------------------------------

// DescribeGroupsRequest asks for detail on one or more groups.
type DescribeGroupsRequest struct {
	Version     int16
	GroupIDs    []string
	IncludeAuth bool
}

// DecodeDescribeGroupsRequest parses the request body for versions up to 3.
func DecodeDescribeGroupsRequest(version int16, body []byte) (*DescribeGroupsRequest, error) {
	r := NewReader(body)
	req := &DescribeGroupsRequest{Version: version}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.GroupIDs = make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		req.GroupIDs = append(req.GroupIDs, id)
	}
	if version >= 3 {
		if req.IncludeAuth, err = r.ReadBool(); err != nil {
			return nil, err
		}
	}
	return req, nil
}

// DescribeGroupsResponseGroup is the detail for one group.
type DescribeGroupsResponseGroup struct {
	ErrorCode    int16
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	Members      []DescribeGroupsResponseMember
}

// DescribeGroupsResponseMember is one member of a described group.
type DescribeGroupsResponseMember struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

// DescribeGroupsResponse reports detail for each requested group.
type DescribeGroupsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Groups         []DescribeGroupsResponseGroup
}

// EncodeDescribeGroupsResponse serializes the response body.
func EncodeDescribeGroupsResponse(resp *DescribeGroupsResponse) ([]byte, error) {
	w := NewWriter(128)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Groups))
	for _, g := range resp.Groups {
		w.WriteInt16(g.ErrorCode)
		w.WriteString(g.GroupID)
		w.WriteString(g.State)
		w.WriteString(g.ProtocolType)
		w.WriteString(g.Protocol)
		w.WriteArrayLen(len(g.Members))
		for _, m := range g.Members {
			w.WriteString(m.MemberID)
			w.WriteString(m.ClientID)
			w.WriteString(m.ClientHost)
			// getBytes-based decoders (e.g. sarama) reject a -1 length; write an
			// empty byte array instead of null for empty member fields.
			if len(m.MemberMetadata) == 0 {
				w.WriteInt32(0)
			} else {
				w.WriteBytes(m.MemberMetadata)
			}
			if len(m.MemberAssignment) == 0 {
				w.WriteInt32(0)
			} else {
				w.WriteBytes(m.MemberAssignment)
			}
		}
	}
	return w.Bytes(), nil
}

// EncodeDescribeGroupsRequest serializes the request body (client side).
func EncodeDescribeGroupsRequest(req *DescribeGroupsRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteArrayLen(len(req.GroupIDs))
	for _, id := range req.GroupIDs {
		w.WriteString(id)
	}
	if req.Version >= 3 {
		w.WriteBool(req.IncludeAuth)
	}
	return w.Bytes(), nil
}

// DecodeDescribeGroupsResponse parses the response body (client side).
func DecodeDescribeGroupsResponse(version int16, body []byte) (*DescribeGroupsResponse, error) {
	r := NewReader(body)
	resp := &DescribeGroupsResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Groups = make([]DescribeGroupsResponseGroup, 0, n)
	for i := 0; i < n; i++ {
		var g DescribeGroupsResponseGroup
		if g.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if g.GroupID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if g.State, err = r.ReadString(); err != nil {
			return nil, err
		}
		if g.ProtocolType, err = r.ReadString(); err != nil {
			return nil, err
		}
		if g.Protocol, err = r.ReadString(); err != nil {
			return nil, err
		}
		mn, err := r.ReadArrayLen()
		if err != nil {
			return nil, err
		}
		g.Members = make([]DescribeGroupsResponseMember, 0, mn)
		for j := 0; j < mn; j++ {
			var m DescribeGroupsResponseMember
			if m.MemberID, err = r.ReadString(); err != nil {
				return nil, err
			}
			if m.ClientID, err = r.ReadString(); err != nil {
				return nil, err
			}
			if m.ClientHost, err = r.ReadString(); err != nil {
				return nil, err
			}
			if m.MemberMetadata, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			if m.MemberAssignment, err = r.ReadBytes(); err != nil {
				return nil, err
			}
			g.Members = append(g.Members, m)
		}
		resp.Groups = append(resp.Groups, g)
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// DeleteGroups (Key 42), v0-v1
// ---------------------------------------------------------------------------

// DeleteGroupsRequest asks to delete one or more groups.
type DeleteGroupsRequest struct {
	Version  int16
	GroupIDs []string
}

// DecodeDeleteGroupsRequest parses the request body for versions up to 1.
func DecodeDeleteGroupsRequest(version int16, body []byte) (*DeleteGroupsRequest, error) {
	r := NewReader(body)
	req := &DeleteGroupsRequest{Version: version}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	req.GroupIDs = make([]string, 0, n)
	for i := 0; i < n; i++ {
		id, err := r.ReadString()
		if err != nil {
			return nil, err
		}
		req.GroupIDs = append(req.GroupIDs, id)
	}
	return req, nil
}

// DeleteGroupsResponseGroup is the result for one group deletion.
type DeleteGroupsResponseGroup struct {
	GroupID      string
	ErrorCode    int16
	ErrorMessage *string
}

// DeleteGroupsResponse reports deletion results.
type DeleteGroupsResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Groups         []DeleteGroupsResponseGroup
}

// EncodeDeleteGroupsResponse serializes the response body.
func EncodeDeleteGroupsResponse(resp *DeleteGroupsResponse) ([]byte, error) {
	w := NewWriter(64)
	if resp.Version >= 1 {
		w.WriteInt32(resp.ThrottleTimeMs)
	}
	w.WriteArrayLen(len(resp.Groups))
	for _, g := range resp.Groups {
		w.WriteString(g.GroupID)
		w.WriteInt16(g.ErrorCode)
		if resp.Version >= 1 {
			w.WriteNullableString(g.ErrorMessage)
		}
	}
	return w.Bytes(), nil
}

// EncodeDeleteGroupsRequest serializes the request body (client side).
func EncodeDeleteGroupsRequest(req *DeleteGroupsRequest) ([]byte, error) {
	w := NewWriter(32)
	w.WriteArrayLen(len(req.GroupIDs))
	for _, id := range req.GroupIDs {
		w.WriteString(id)
	}
	return w.Bytes(), nil
}

// DecodeDeleteGroupsResponse parses the response body (client side).
func DecodeDeleteGroupsResponse(version int16, body []byte) (*DeleteGroupsResponse, error) {
	r := NewReader(body)
	resp := &DeleteGroupsResponse{Version: version}
	var err error
	if version >= 1 {
		if resp.ThrottleTimeMs, err = r.ReadInt32(); err != nil {
			return nil, err
		}
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	resp.Groups = make([]DeleteGroupsResponseGroup, 0, n)
	for i := 0; i < n; i++ {
		var g DeleteGroupsResponseGroup
		if g.GroupID, err = r.ReadString(); err != nil {
			return nil, err
		}
		if g.ErrorCode, err = r.ReadInt16(); err != nil {
			return nil, err
		}
		if version >= 1 {
			if g.ErrorMessage, err = r.ReadNullableString(); err != nil {
				return nil, err
			}
		}
		resp.Groups = append(resp.Groups, g)
	}
	return resp, nil
}
