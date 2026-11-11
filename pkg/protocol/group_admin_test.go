package protocol

import (
	"math"
	"testing"
)

func TestDescribeGroupsV3ResponseRoundTrip(t *testing.T) {
	resp := &DescribeGroupsResponse{Version: 3, Groups: []DescribeGroupsResponseGroup{
		{
			GroupID: "alpha", State: "Stable", ProtocolType: "consumer", Protocol: "range",
			AuthorizedOperations: math.MinInt32,
			Members: []DescribeGroupsResponseMember{{
				MemberID: "m-1", ClientID: "client", ClientHost: "/10.0.0.1",
				MemberMetadata: []byte("meta"), MemberAssignment: []byte("assign"),
			}},
		},
		{
			GroupID: "beta", State: "Empty", ProtocolType: "consumer", Protocol: "range",
			AuthorizedOperations: math.MinInt32,
		},
	}}
	body, err := EncodeDescribeGroupsResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeDescribeGroupsResponse(3, body)
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}
	if len(got.Groups) != 2 {
		t.Fatalf("decoded %d groups, want 2", len(got.Groups))
	}
	if got.Groups[1].GroupID != "beta" {
		t.Fatalf("second group = %q, want beta (the reader drifted)", got.Groups[1].GroupID)
	}
	if got.Groups[0].AuthorizedOperations != math.MinInt32 {
		t.Fatalf("authorized operations = %d, want INT32_MIN", got.Groups[0].AuthorizedOperations)
	}
	if got.Groups[0].Members[0].MemberAssignment == nil {
		t.Fatal("member assignment was lost")
	}

	classic := &DescribeGroupsResponse{Version: 2, Groups: resp.Groups}
	body2, err := EncodeDescribeGroupsResponse(classic)
	if err != nil {
		t.Fatal(err)
	}
	if len(body2) >= len(body) {
		t.Fatalf("v2 body (%d) is not shorter than v3 (%d): the version gate is wrong", len(body2), len(body))
	}
	if _, err := DecodeDescribeGroupsResponse(2, body2); err != nil {
		t.Fatalf("decode v2: %v", err)
	}
}

func TestDescribeGroupsRequestV3CarriesTheFlag(t *testing.T) {
	req := &DescribeGroupsRequest{Version: 3, GroupIDs: []string{"g"}, IncludeAuth: true}
	body, err := EncodeDescribeGroupsRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDescribeGroupsRequest(3, body)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.IncludeAuth {
		t.Fatal("include_authorized_operations was dropped")
	}
	plain := &DescribeGroupsRequest{Version: 2, GroupIDs: []string{"g"}}
	body2, err := EncodeDescribeGroupsRequest(plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(body2) >= len(body) {
		t.Fatalf("v2 request (%d) is not shorter than v3 (%d)", len(body2), len(body))
	}
}
