package server

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"hash"
	"strings"
	"testing"

	"github.com/Yukaz0/pocketkafka/internal/credential"
)

func TestSCRAMHandshake(t *testing.T) {
	username := "app"
	password := "changeme"
	clientNonce := "clientNonce123456"

	salt := []byte("0123456789abcdef")
	mech := credential.SHA256
	cred, err := credential.DeriveVerifier(mech, password, salt, credential.DefaultSCRAMIterations)
	if err != nil {
		t.Fatalf("derive verifier: %v", err)
	}

	clientFirst := "n,," + "n=" + username + ",r=" + clientNonce
	session, serverFirst, err := newScramSession(mech, cred, clientFirst)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}

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

	saltedPassword, err := pbkdf2.Key(sha256.New, password, serverSalt, iter, mech.KeySize)
	if err != nil {
		t.Fatalf("pbkdf2: %v", err)
	}
	clientKey := hmacSum(sha256.New, saltedPassword, []byte("Client Key"))
	withoutProof := "c=biws,r=" + serverNonce

	authMessage := scramBare(clientFirst) + "," + serverFirst + "," + withoutProof
	clientSignature := hmacSum(sha256.New, sha256Sum(clientKey), []byte(authMessage))
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

	expected := hmacSum(sha256.New, hmacSum(sha256.New, saltedPassword, []byte("Server Key")), []byte(authMessage))
	got, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(string(serverFinal), "v="))
	if !hmac.Equal(expected, got) {
		t.Fatalf("server signature mismatch: got %x want %x", got, expected)
	}
}

func TestSCRAMWrongPassword(t *testing.T) {
	mech := credential.SHA256
	cred, err := credential.DeriveVerifier(mech, "right-password", []byte("salt"), credential.DefaultSCRAMIterations)
	if err != nil {
		t.Fatalf("derive verifier: %v", err)
	}
	session, serverFirst, err := newScramSession(mech, cred, "n,,n=app,r=clientNonce")
	if err != nil {
		t.Fatal(err)
	}

	var fullNonce string
	for _, part := range strings.Split(serverFirst, ",") {
		if strings.HasPrefix(part, "r=") {
			fullNonce = strings.TrimPrefix(part, "r=")
		}
	}

	clientFinal := "c=biws,r=" + fullNonce + ",p=" + base64.StdEncoding.EncodeToString(make([]byte, mech.KeySize))
	if _, err := session.finish(clientFinal); err == nil {
		t.Fatal("expected authentication failure for wrong password")
	}
}

func sha256Sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

func hmacSum(newHash func() hash.Hash, key, msg []byte) []byte {
	mac := hmac.New(newHash, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func atoi(s string) int {
	n := 0
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			break
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}
