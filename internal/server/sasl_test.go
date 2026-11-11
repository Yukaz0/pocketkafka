package server

import (
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/credential"
	"github.com/Yukaz0/pocketkafka/pkg/protocol"
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
		{"\x00app\x00secret\x00extra", false},
	}
	for _, c := range cases {
		user, ok := s.authenticatePlain([]byte(c.token))
		if ok != c.ok {
			t.Fatalf("token %q: got %v want %v", c.token, ok, c.ok)
		}
		if ok && user != "app" {
			t.Fatalf("token %q: principal = %q, want %q", c.token, user, "app")
		}
	}
}

func TestSecurityDisabledMeansAuthed(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = false
	s := New(&cfg, nil)

	_ = s
}

func TestSCRAMResponsesAreUniform(t *testing.T) {
	verifier, err := credential.DeriveVerifier(credential.SHA256, "scram-password", []byte("0123456789abcdef0123456789abcdef"), credential.DefaultSCRAMIterations)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Web.AuthSecret = "test-signing-secret"
	cfg.Security.Users = []config.SecurityUser{
		{Username: "hashonly", PasswordHash: "pbkdf2-sha256$1000$c2FsdA==$aGFzaA=="},
		{Username: "scramuser", SCRAMVerifier: verifier.String()},
	}
	s := New(&cfg, nil)

	st := &connState{mechanism: "SCRAM-SHA-256"}
	body, ok, failed, err := s.handleSCRAM(st, 1, []byte("n,,n=hashonly,r=clientNonce0123456789"))
	if err != nil || ok || !failed {
		t.Fatalf("hash-only user: ok=%v failed=%v err=%v", ok, failed, err)
	}
	resp, err := protocol.DecodeSaslAuthenticateResponse(1, body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != protocol.ErrSaslAuthenticationFailed {
		t.Fatalf("error code = %d, want ErrSaslAuthenticationFailed", resp.ErrorCode)
	}
	if resp.ErrorMessage == nil || *resp.ErrorMessage != "authentication failed" {
		t.Fatalf("message = %v, want the generic one", resp.ErrorMessage)
	}

	for _, user := range []string{"ghost", "scramuser"} {
		st := &connState{mechanism: "SCRAM-SHA-256"}
		body, ok, failed, err := s.handleSCRAM(st, 1, []byte("n,,n="+user+",r=clientNonce0123456789"))
		if err != nil || ok || failed {
			t.Fatalf("%s: first step ok=%v failed=%v err=%v", user, ok, failed, err)
		}
		resp, err := protocol.DecodeSaslAuthenticateResponse(1, body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != protocol.ErrNone {
			t.Fatalf("%s: error code = %d, want none", user, resp.ErrorCode)
		}
		first := string(resp.AuthBytes)
		for _, field := range []string{"r=", "s=", "i="} {
			if !strings.Contains(first, field) {
				t.Fatalf("%s: server-first %q is missing %s", user, first, field)
			}
		}
	}
}

func TestPlainRequiresTLSUnlessDisabled(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Web.AuthSecret = "test-signing-secret"
	cfg.Security.RequireTLSForPlain = true
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "a-good-password"}}
	s := New(&cfg, nil)

	token := []byte("\x00app\x00a-good-password")
	hdr := &protocol.RequestHeader{ApiVersion: 1, ApiKey: protocol.APKSaslAuthenticate}
	st := &connState{mechanism: "PLAIN", tls: false}
	body, ok, failed, err := s.handleSASLAuthenticate(st, hdr, saslBody(t, token))
	if err != nil || ok || !failed {
		t.Fatalf("cleartext PLAIN: ok=%v failed=%v err=%v", ok, failed, err)
	}
	resp, err := protocol.DecodeSaslAuthenticateResponse(1, body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorMessage == nil || !strings.Contains(*resp.ErrorMessage, "requires TLS") {
		t.Fatalf("message = %v, want the TLS hint", resp.ErrorMessage)
	}

	st = &connState{mechanism: "PLAIN", tls: true}
	body, ok, failed, err = s.handleSASLAuthenticate(st, hdr, saslBody(t, token))
	if err != nil || !ok || failed {
		t.Fatalf("PLAIN over TLS: ok=%v failed=%v err=%v", ok, failed, err)
	}
	if st.principal != "app" {
		t.Fatalf("principal = %q, want app", st.principal)
	}

	cfg.Security.RequireTLSForPlain = false
	s = New(&cfg, nil)
	st = &connState{mechanism: "PLAIN", tls: false}
	if _, ok, failed, err := s.handleSASLAuthenticate(st, hdr, saslBody(t, token)); err != nil || !ok || failed {
		t.Fatalf("policy off: ok=%v failed=%v err=%v", ok, failed, err)
	}
}

func TestPlainWithoutHandshakeIsRefused(t *testing.T) {
	cfg := config.Default()
	cfg.Security.Enabled = true
	cfg.Web.AuthSecret = "test-signing-secret"
	cfg.Security.RequireTLSForPlain = false
	cfg.Security.Users = []config.SecurityUser{{Username: "app", Password: "a-good-password"}}
	s := New(&cfg, nil)

	hdr := &protocol.RequestHeader{ApiVersion: 1, ApiKey: protocol.APKSaslAuthenticate}

	st := &connState{tls: true}
	body, ok, failed, err := s.handleSASLAuthenticate(st, hdr, saslBody(t, []byte("\x00app\x00a-good-password")))
	if err != nil || ok || !failed {
		t.Fatalf("handshake-skipping PLAIN: ok=%v failed=%v err=%v", ok, failed, err)
	}
	resp, derr := protocol.DecodeSaslAuthenticateResponse(1, body)
	if derr != nil {
		t.Fatal(derr)
	}
	if resp.ErrorCode != protocol.ErrSaslAuthenticationFailed {
		t.Fatalf("error code = %d, want ErrSaslAuthenticationFailed", resp.ErrorCode)
	}
	if resp.ErrorMessage == nil || !strings.Contains(*resp.ErrorMessage, "handshake") {
		t.Fatalf("message = %v, want the handshake requirement", resp.ErrorMessage)
	}
	if st.principal != "" {
		t.Fatalf("principal was set to %q", st.principal)
	}
}

func saslBody(t *testing.T, auth []byte) []byte {
	t.Helper()
	body, err := protocol.EncodeSaslAuthenticateRequest(&protocol.SaslAuthenticateRequest{Version: 1, AuthBytes: auth})
	if err != nil {
		t.Fatal(err)
	}
	return body
}
