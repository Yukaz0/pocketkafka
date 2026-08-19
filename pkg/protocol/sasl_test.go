package protocol

import "testing"

func TestSASLHandshakeRoundTrip(t *testing.T) {
	resp := &SASLHandshakeResponse{Version: 1, ErrorCode: 0, Mechanisms: []string{"PLAIN"}}
	body, err := EncodeSASLHandshakeResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeSASLHandshakeResponse(1, body)
	if err != nil {
		t.Fatal(err)
	}
	if dec.ErrorCode != 0 || len(dec.Mechanisms) != 1 || dec.Mechanisms[0] != "PLAIN" {
		t.Fatalf("decode mismatch: %+v", dec)
	}
}

func TestSaslAuthenticateRoundTrip(t *testing.T) {
	resp := &SaslAuthenticateResponse{Version: 2, ErrorCode: 0, AuthBytes: []byte("ok")}
	body, err := EncodeSaslAuthenticateResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := DecodeSaslAuthenticateResponse(2, body)
	if err != nil {
		t.Fatal(err)
	}
	if dec.ErrorCode != 0 || string(dec.AuthBytes) != "ok" {
		t.Fatalf("decode mismatch: %+v", dec)
	}
}

func TestSaslAuthenticateRequest(t *testing.T) {
	req, err := DecodeSaslAuthenticateRequest(2, []byte{0, 0, 0, 3, 'a', 'b', 'c'})
	if err != nil {
		t.Fatal(err)
	}
	if string(req.AuthBytes) != "abc" {
		t.Fatalf("auth bytes: %q", req.AuthBytes)
	}
}
