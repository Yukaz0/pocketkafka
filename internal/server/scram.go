// SASL/SCRAM-SHA-256 and SCRAM-SHA-512 server implementation (RFC 5802).
package server

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"strings"
)

// scramMechanism is one SCRAM hash family.
type scramMechanism struct {
	name       string
	newHash    func() hash.Hash
	keySize    int // StoredKey/ServerKey size in bytes
	iterations int
}

var (
	scramSHA256 = scramMechanism{name: "SCRAM-SHA-256", newHash: sha256.New, keySize: sha256.Size, iterations: 4096}
	scramSHA512 = scramMechanism{name: "SCRAM-SHA-512", newHash: sha512.New, keySize: sha512.Size, iterations: 4096}
)

// pbkdf2Key derives a salted password using PBKDF2-HMAC (RFC 2898).
func pbkdf2Key(h func() hash.Hash, password, salt []byte, iter, keyLen int) []byte {
	prf := hmac.New(h, password)
	hashLen := prf.Size()
	numBlocks := (keyLen + hashLen - 1) / hashLen

	var buf [4]byte
	dk := make([]byte, 0, numBlocks*hashLen)
	u := make([]byte, hashLen)
	for block := 1; block <= numBlocks; block++ {
		prf.Reset()
		prf.Write(salt)
		binary.BigEndian.PutUint32(buf[:], uint32(block))
		prf.Write(buf[:])
		dk = prf.Sum(dk)
		t := dk[len(dk)-hashLen:]
		copy(u, t)
		for n := 2; n <= iter; n++ {
			prf.Reset()
			prf.Write(u)
			u = u[:0]
			u = prf.Sum(u)
			for x := range u {
				t[x] ^= u[x]
			}
		}
	}
	return dk[:keyLen]
}

// scramCredential holds the server-side SCRAM secrets for one user.
type scramCredential struct {
	salt       []byte
	iterations int
	storedKey  []byte
	serverKey  []byte
}

// newScramCredential derives a credential from a plaintext password.
func newScramCredential(m scramMechanism, password string, salt []byte, iterations int) *scramCredential {
	salted := pbkdf2Key(m.newHash, []byte(password), salt, iterations, m.keySize)
	clientKey := hmacSum(m.newHash, salted, []byte("Client Key"))
	h := m.newHash()
	h.Write(clientKey)
	storedKey := h.Sum(nil) // SHA-256/512 of ClientKey
	serverKey := hmacSum(m.newHash, salted, []byte("Server Key"))
	return &scramCredential{salt: salt, iterations: iterations, storedKey: storedKey, serverKey: serverKey}
}

func hmacSum(h func() hash.Hash, key, msg []byte) []byte {
	m := hmac.New(h, key)
	m.Write(msg)
	return m.Sum(nil)
}

// scramSession drives the server side of one SCRAM authentication exchange.
type scramSession struct {
	mechanism   scramMechanism
	cred        *scramCredential
	clientFirst string
	clientNonce string
	serverNonce string
	authMessage string
	finished    bool
}

// newScramSession starts a session and returns the ServerFirstMessage ready to
// send to the client.
func newScramSession(m scramMechanism, cred *scramCredential, clientFirst string) (*scramSession, string, error) {
	// client-first: n,,n=<user>,r=<clientNonce>[,extensions]
	rest := clientFirst
	if idx := strings.Index(clientFirst, ",,"); idx >= 0 {
		rest = clientFirst[idx+2:]
	}
	clientNonce := ""
	for _, part := range strings.Split(rest, ",") {
		if strings.HasPrefix(part, "r=") {
			clientNonce = strings.TrimPrefix(part, "r=")
		}
	}
	if clientNonce == "" {
		return nil, "", fmt.Errorf("missing client nonce")
	}

	serverNonce, err := randomNonce()
	if err != nil {
		return nil, "", err
	}
	s := &scramSession{
		mechanism:   m,
		cred:        cred,
		clientFirst: clientFirst,
		clientNonce: clientNonce,
		serverNonce: serverNonce,
	}
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", clientNonce+serverNonce, base64.StdEncoding.EncodeToString(cred.salt), cred.iterations)
	s.authMessage = clientFirst + "," + serverFirst + ","
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
	var proofB64 string
	for _, p := range parts {
		if strings.HasPrefix(p, "p=") {
			proofB64 = strings.TrimPrefix(p, "p=")
		}
	}
	if proofB64 == "" {
		return nil, fmt.Errorf("missing client proof")
	}
	fullNonce := ""
	for _, p := range parts {
		if strings.HasPrefix(p, "r=") {
			fullNonce = strings.TrimPrefix(p, "r=")
		}
	}
	if fullNonce != s.clientNonce+s.serverNonce {
		return nil, fmt.Errorf("nonce mismatch")
	}

	withoutProof := clientFinal[:strings.LastIndex(clientFinal, ",p=")]
	authMessage := s.authMessage + withoutProof
	clientSignature := hmacSum(s.mechanism.newHash, s.cred.storedKey, []byte(authMessage))
	clientProof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return nil, fmt.Errorf("bad proof encoding")
	}
	// ClientKey = ClientProof XOR ClientSignature
	clientKey := make([]byte, len(clientProof))
	for i := range clientProof {
		clientKey[i] = clientProof[i] ^ clientSignature[i]
	}
	if len(clientKey) != s.mechanism.keySize {
		return nil, fmt.Errorf("proof size mismatch")
	}
	// Recompute StoredKey = H(ClientKey) and compare.
	h := s.mechanism.newHash()
	h.Write(clientKey)
	recomputed := h.Sum(nil)
	if subtle.ConstantTimeCompare(recomputed, s.cred.storedKey) != 1 {
		return nil, fmt.Errorf("authentication failed")
	}

	serverSignature := hmacSum(s.mechanism.newHash, s.cred.serverKey, []byte(authMessage))
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
