// SASL/SCRAM-SHA-256 and SCRAM-SHA-512 server implementation (RFC 5802).
package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/Yukaz0/pocketkafka/internal/credential"
)

// scramSession drives the server side of one SCRAM authentication exchange.
// It holds a verifier, never the password.
type scramSession struct {
	mech        credential.Mechanism
	verifier    credential.Verifier
	clientFirst string
	clientNonce string
	serverNonce string
	authMessage string
	finished    bool
}

// newScramSession starts a session and returns the ServerFirstMessage ready to
// send to the client.
func newScramSession(m credential.Mechanism, v credential.Verifier, clientFirst string) (*scramSession, string, error) {
	clientNonce := scramNonce(clientFirst)
	if clientNonce == "" {
		return nil, "", fmt.Errorf("missing client nonce")
	}
	serverNonce, err := randomNonce()
	if err != nil {
		return nil, "", err
	}
	s := &scramSession{
		mech:        m,
		verifier:    v,
		clientFirst: clientFirst,
		clientNonce: clientNonce,
		serverNonce: serverNonce,
	}
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", clientNonce+serverNonce,
		base64.StdEncoding.EncodeToString(v.Salt), v.Iterations)
	// RFC 5802: AuthMessage is built from client-first-message-bare, so the gs2
	// header ("n,," or "n,a=<authzid>,") is excluded. Including it would make
	// this broker's signatures disagree with every compliant client.
	s.authMessage = scramBare(clientFirst) + "," + serverFirst + ","
	return s, serverFirst, nil
}

// finish verifies the ClientFinalMessage and returns the ServerFinalMessage.
// The final message is either "v=<serverSignature>" (success) or an
// "e=<error>" payload (failure).
func (s *scramSession) finish(clientFinal string) ([]byte, error) {
	// client-final: c=<base64>,r=<fullNonce>,p=<clientProof>
	parts := strings.Split(clientFinal, ",")
	if len(parts) < 3 {
		return nil, fmt.Errorf("malformed client-final-message")
	}
	var proofB64, fullNonce string
	for _, p := range parts {
		switch {
		case strings.HasPrefix(p, "p="):
			proofB64 = strings.TrimPrefix(p, "p=")
		case strings.HasPrefix(p, "r="):
			fullNonce = strings.TrimPrefix(p, "r=")
		}
	}
	if proofB64 == "" {
		return nil, fmt.Errorf("missing client proof")
	}
	if fullNonce != s.clientNonce+s.serverNonce {
		return nil, fmt.Errorf("nonce mismatch")
	}

	idx := strings.LastIndex(clientFinal, ",p=")
	if idx < 0 {
		return nil, fmt.Errorf("malformed client-final-message")
	}
	authMessage := s.authMessage + clientFinal[:idx]
	clientSignature := s.verifier.Signature(s.verifier.StoredKey, []byte(authMessage))
	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return nil, fmt.Errorf("bad proof encoding")
	}
	if len(clientProof) != s.mech.KeySize {
		return nil, fmt.Errorf("proof size mismatch")
	}
	// ClientKey = ClientProof XOR ClientSignature; StoredKey = H(ClientKey).
	clientKey := make([]byte, len(clientProof))
	for i := range clientProof {
		clientKey[i] = clientProof[i] ^ clientSignature[i]
	}
	h := s.mech.NewHash()
	h.Write(clientKey)
	if subtle.ConstantTimeCompare(h.Sum(nil), s.verifier.StoredKey) != 1 {
		return nil, fmt.Errorf("authentication failed")
	}

	serverSignature := s.verifier.Signature(s.verifier.ServerKey, []byte(authMessage))
	s.finished = true
	return []byte("v=" + base64.StdEncoding.EncodeToString(serverSignature)), nil
}

func randomNonce() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// scramBare strips the gs2 header from a client-first message: "n,," or
// "n,a=<authzid>," leaves the bare "n=<user>,r=<nonce>[,extensions]".
func scramBare(clientFirst string) string {
	if idx := strings.Index(clientFirst, ",,"); idx >= 0 {
		return clientFirst[idx+2:]
	}
	if idx := strings.Index(clientFirst, ",a="); idx >= 0 {
		if next := strings.Index(clientFirst[idx+3:], ","); next >= 0 {
			return clientFirst[idx+3+next+1:]
		}
	}
	return clientFirst
}

// scramNonce extracts the client nonce (r=) from a client-first message.
func scramNonce(clientFirst string) string {
	for _, part := range strings.Split(scramBare(clientFirst), ",") {
		if strings.HasPrefix(part, "r=") {
			return strings.TrimPrefix(part, "r=")
		}
	}
	return ""
}

// unescapeSCRAMName decodes the RFC 5802 "=2C"/"=3D" escapes in a username.
func unescapeSCRAMName(name string) string {
	if !strings.Contains(name, "=") {
		return name
	}
	out := strings.Builder{}
	out.Grow(len(name))
	for i := 0; i < len(name); i++ {
		if name[i] == '=' && i+2 < len(name) {
			switch name[i+1 : i+3] {
			case "2C":
				out.WriteByte(',')
				i += 2
				continue
			case "3D":
				out.WriteByte('=')
				i += 2
				continue
			}
		}
		out.WriteByte(name[i])
	}
	return out.String()
}
