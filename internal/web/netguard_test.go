package web

import (
	"net/url"
	"testing"
)

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q) = %v", raw, err)
	}
	return u
}

func TestPeerHostAllowed(t *testing.T) {
	allow := []string{
		"http://192.168.1.25:9092",
		"http://10.0.0.26:8080",
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
		"http://peer.example.com:8080",
		"http://peer.example.com",
		"https://dash.example.com/path",
	}
	for _, a := range allow {
		if err := peerHostAllowed(mustParse(t, a)); err != nil {
			t.Errorf("peerHostAllowed(%q) = %v, want nil", a, err)
		}
	}
	deny := []string{
		"http://169.254.169.254",
		"http://169.254.169.254:80",
		"http://169.254.170.2:80",
		"http://[fd00:ec2::254]:80",
		"http://100.100.100.200:80",
		"http://169.254.9.9:9092",
		"http://0.0.0.0:8080",
		"http://224.0.0.1:8080",
		"http://user:pass@peer.example.com/",
		"http:///nopath",
	}
	for _, a := range deny {
		if err := peerHostAllowed(mustParse(t, a)); err == nil {
			t.Errorf("peerHostAllowed(%q) = nil, want error", a)
		}
	}
	if err := peerHostAllowed(nil); err == nil {
		t.Error("peerHostAllowed(nil) = nil, want error")
	}
}
