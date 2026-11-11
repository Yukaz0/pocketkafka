package credential

import (
	"strings"
	"testing"
)

func TestHashPasswordRoundTrip(t *testing.T) {
	hash, err := HashPasswordWith("correct horse battery staple", []byte("0123456789abcdef"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "pbkdf2-sha256$1000$") {
		t.Fatalf("unexpected hash format %q", hash)
	}
	if !LooksLikeHash(hash) {
		t.Fatalf("%q should be recognised as a stored hash", hash)
	}
	if !VerifyPassword(hash, "correct horse battery staple") {
		t.Fatal("correct password rejected")
	}
	if VerifyPassword(hash, "correct horse battery stapl") {
		t.Fatal("wrong password accepted")
	}
	if VerifyPassword("pbkdf2-sha256$1000$not-base64$also-not", "x") {
		t.Fatal("malformed verifier accepted")
	}
	if VerifyPassword("plaintext", "plaintext") {
		t.Fatal("a plaintext value must not pass as a stored verifier")
	}
}

func TestMatchesHandlesBothForms(t *testing.T) {
	hash, err := HashPasswordWith("s3cret-password", []byte("saltsaltsaltsalt"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		stored, presented string
		want              bool
	}{
		{hash, "s3cret-password", true},
		{hash, "s3cret-passwore", false},
		{"plain-value", "plain-value", true},
		{"plain-value", "plain-valu", false},
		{"", "", true},
		{hash, "", false},
	}
	for _, tc := range cases {
		if got := Matches(tc.stored, tc.presented); got != tc.want {
			t.Fatalf("Matches(%q, %q) = %v, want %v", tc.stored, tc.presented, got, tc.want)
		}
	}
}

func TestHashIterationBounds(t *testing.T) {
	if _, err := HashPasswordWith("p", []byte("salt"), 0); err == nil {
		t.Fatal("zero iterations accepted")
	}
	if _, err := HashPasswordWith("p", []byte("salt"), MaxIterations+1); err == nil {
		t.Fatal("iteration count above the cap accepted")
	}
}

func TestSCRAMVerifierRoundTrip(t *testing.T) {
	salt := []byte("0123456789abcdef0123456789abcdef")
	v, err := DeriveVerifier(SHA256, "scram-password", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	text := v.String()
	if !strings.HasPrefix(text, "scram-sha-256$4096$") {
		t.Fatalf("unexpected verifier format %q", text)
	}
	parsed, err := ParseVerifier(text)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Iterations != v.Iterations || parsed.Mechanism.Name != v.Mechanism.Name {
		t.Fatalf("parsed %+v, want %+v", parsed, v)
	}
	if len(parsed.StoredKey) != SHA256.KeySize || len(parsed.ServerKey) != SHA256.KeySize {
		t.Fatal("parsed key sizes are wrong")
	}
	if string(parsed.StoredKey) != string(v.StoredKey) || string(parsed.ServerKey) != string(v.ServerKey) {
		t.Fatal("parsed keys differ from the derived ones")
	}

	again, err := DeriveVerifier(SHA256, "scram-password", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if string(again.StoredKey) != string(v.StoredKey) {
		t.Fatal("derivation is not deterministic")
	}
	other, err := DeriveVerifier(SHA256, "scram-passwore", salt, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if string(other.StoredKey) == string(v.StoredKey) {
		t.Fatal("a different password produced the same stored key")
	}
}

func TestParseVerifierRejectsGarbage(t *testing.T) {
	cases := []string{
		"",
		"scram-sha-256$4096$c2FsdA==$a2V5$a2V5",
		"scram-sha-1$4096$c2FsdA==$a2V5$a2V5",
		"scram-sha-256$0$c2FsdA==$a2V5$a2V5",
		"scram-sha-256$4096$$a2V5$a2V5",
		"scram-sha-256$4096$c2FsdA==",
		"pbkdf2-sha256$1000$c2FsdA==$aGFzaA==",
	}
	for _, c := range cases {
		if _, err := ParseVerifier(c); err == nil {
			t.Fatalf("%q parsed as a valid verifier", c)
		}
	}
}

func TestMechanismByName(t *testing.T) {
	for _, name := range []string{"SCRAM-SHA-256", "scram-sha-512", "Scram-Sha-256"} {
		if _, ok := MechanismByName(name); !ok {
			t.Fatalf("%q not recognised", name)
		}
	}
	if _, ok := MechanismByName("PLAIN"); ok {
		t.Fatal("PLAIN is not a SCRAM mechanism")
	}
}
