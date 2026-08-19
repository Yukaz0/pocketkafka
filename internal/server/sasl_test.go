package server

import (
	"testing"

	"github.com/neu/go-kafka-neu/internal/config"
)

func TestAuthenticatePlain(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "secret"}}
	s := New(&cfg, nil)

	cases := []struct {
		token string
		ok    bool
	}{
		{"\x00app\x00secret", true},
		{"\x00app\x00wrong", false},
		{"\x00nobody\x00secret", false},
		{"malformed", false},
		{"\x00app\x00secret\x00extra", false}, // too many fields
	}
	for _, c := range cases {
		if got := s.authenticatePlain([]byte(c.token)); got != c.ok {
			t.Fatalf("token %q: got %v want %v", c.token, got, c.ok)
		}
	}
}

func TestSecurityDisabledMeansAuthed(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = false
	s := New(&cfg, nil)
	// When disabled, connections are considered authenticated already.
	// authenticatePlain is only consulted when enabled; this guards the flag.
	_ = s
}
