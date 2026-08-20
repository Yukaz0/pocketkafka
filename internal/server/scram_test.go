package server

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
)

// TestSCRAMHandshake simulates a full SCRAM-SHA-256 exchange from the client
// side, following RFC 5802, and verifies the server computes a valid
// ServerSignature.
func TestSCRAMHandshake(t *testing.T) {
	username := "app"
	password := "changeme"
	clientNonce := "clientNonce123456"

	// Server side: build a credential with a known salt so the client can
	// reproduce the derivation.
	salt := []byte("0123456789abcdef")
	mech := scramSHA256
	cred := newScramCredential(mech, password, salt, mech.iterations)

	// Client-first-message: n,,n=app,r=clientNonce123456
	clientFirst := "n,," + "n=" + username + ",r=" + clientNonce
	session, serverFirst, err := newScramSession(mech, cred, clientFirst)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}

	// Parse ServerFirst: r=...,s=...,i=...
	if !strings.Contains(serverFirst, "r="+clientNonce) {
		t.Fatalf("server first must include client nonce: %s", serverFirst)
	}
	var serverNonce, saltB64 string
	var iter int
	for _, part := range strings.Split(serverFirst, ",") {
		switch {
		case strings.HasPrefix(part, "r="):
			serverNonce = strings.TrimPrefix(part, "r=")
		case strings.HasPrefix(part, "s="):
			saltB64 = strings.TrimPrefix(part, "s=")
		case strings.HasPrefix(part, "i="):
			iter = atoi(strings.TrimPrefix(part, "i="))
		}
	}
	if saltB64 == "" || iter == 0 {
		t.Fatalf("bad server first: %s", serverFirst)
	}
	serverSalt, _ := base64.StdEncoding.DecodeString(saltB64)

	// Client computes ClientKey, then ClientProof.
	saltedPassword := pbkdf2Key(mech.newHash, []byte(password), serverSalt, iter, mech.keySize)
	clientKey := hmacSum(mech.newHash, saltedPassword, []byte("Client Key"))
	withoutProof := "c=biws,r=" + serverNonce
	authMessage := clientFirst + "," + serverFirst + "," + withoutProof
	clientSignature := hmacSum(mech.newHash, sha256Sum(clientKey), []byte(authMessage))
	clientProof := make([]byte, len(clientKey))
	for i := range clientKey {
		clientProof[i] = clientKey[i] ^ clientSignature[i]
	}

	clientFinal := withoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof)
	serverFinal, err := session.finish(clientFinal)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !strings.HasPrefix(string(serverFinal), "v=") {
		t.Fatalf("expected server signature, got %q", serverFinal)
	}

	// Verify the ServerSignature the way the client would.
	expected := hmacSum(mech.newHash, hmacSum(mech.newHash, saltedPassword, []byte("Server Key")), []byte(authMessage))
	got, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(string(serverFinal), "v="))
	if !hmac.Equal(expected, got) {
		t.Fatalf("server signature mismatch: got %x want %x", got, expected)
	}
}

func TestSCRAMWrongPassword(t *testing.T) {
	mech := scramSHA256
	cred := newScramCredential(mech, "right-password", []byte("salt"), mech.iterations)
	session, serverFirst, err := newScramSession(mech, cred, "n,,n=app,r=clientNonce")
	if err != nil {
		t.Fatal(err)
	}
	// Extract the full nonce from the server first message.
	var fullNonce string
	for _, part := range strings.Split(serverFirst, ",") {
		if strings.HasPrefix(part, "r=") {
			fullNonce = strings.TrimPrefix(part, "r=")
		}
	}
	// A client with the wrong password produces a bogus proof.
	clientFinal := "c=biws,r=" + fullNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, mech.keySize))
	if _, err := session.finish(clientFinal); err == nil {
		t.Fatal("expected authentication failure for wrong password")
	}
}

func sha256Sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

func atoi(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}
