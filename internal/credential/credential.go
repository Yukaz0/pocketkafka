// Package credential stores and verifies broker credentials without requiring
// the plaintext password to live in the configuration file.
//
// Two forms are supported:
//
//   - a PBKDF2 password hash ("pbkdf2-sha256$…"), accepted by SASL/PLAIN and by
//     HTTP Basic, and usable for a client that presents the password itself;
//   - a SCRAM verifier ("scram-sha-256$…"), the salt/iterations/StoredKey/
//     ServerKey tuple an RFC 5802 server needs, which never contains anything
//     that lets the broker (or a reader of the file) recover the password.
//
// Every comparison goes through Matches or VerifySignature, both constant-time,
// so no path leaks a password byte by byte.
package credential

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"strings"
)

const (
	// passwordHashIterations follows the OWASP recommendation for
	// PBKDF2-HMAC-SHA256.
	passwordHashIterations = 210000
	// DefaultSCRAMIterations is well above the RFC 5802 minimum of 4096: the
	// iteration count is also the cost a client pays per connection, and a low
	// value is a pre-authentication CPU amplification vector.
	DefaultSCRAMIterations = 15000
	// MaxIterations bounds a verifier that a config file asks us to honour, so
	// a hostile file cannot turn authentication into a CPU denial of service.
	MaxIterations = 1_000_000
)

const (
	passwordPrefix = "pbkdf2-sha256"
	sha512Prefix   = "pbkdf2-sha512"
	scramPrefix    = "scram-"
)

// Mechanism describes one SCRAM hash family.
type Mechanism struct {
	Name    string
	KeySize int
	NewHash func() hash.Hash
}

// Supported SCRAM mechanisms.
var (
	SHA256 = Mechanism{Name: "SCRAM-SHA-256", KeySize: sha256.Size, NewHash: sha256.New}
	SHA512 = Mechanism{Name: "SCRAM-SHA-512", KeySize: sha512.Size, NewHash: sha512.New}
)

// MechanismByName maps a SASL mechanism name to its hash family.
func MechanismByName(name string) (Mechanism, bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SCRAM-SHA-256":
		return SHA256, true
	case "SCRAM-SHA-512":
		return SHA512, true
	}
	return Mechanism{}, false
}

// RandomSalt returns n cryptographically random bytes.
func RandomSalt(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// HashPassword derives a PBKDF2-HMAC-SHA256 verifier:
//
//	pbkdf2-sha256$<iterations>$<base64 salt>$<base64 key>
func HashPassword(password string) (string, error) {
	salt, err := RandomSalt(16)
	if err != nil {
		return "", err
	}
	return HashPasswordWith(password, salt, passwordHashIterations)
}

// HashPasswordWith derives a verifier with an explicit salt and iteration
// count, so a caller can reproduce a stable hash (tests, tooling).
func HashPasswordWith(password string, salt []byte, iterations int) (string, error) {
	if iterations < 1 || iterations > MaxIterations {
		return "", fmt.Errorf("credential: iteration count %d out of range", iterations)
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s$%d$%s$%s", passwordPrefix, iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(key)), nil
}

// LooksLikeHash reports whether v is one of the stored credential forms rather
// than a plaintext password.
func LooksLikeHash(v string) bool {
	return strings.HasPrefix(v, passwordPrefix+"$") ||
		strings.HasPrefix(v, sha512Prefix+"$") ||
		strings.HasPrefix(v, scramPrefix)
}

// Matches checks a presented password against a stored credential. The stored
// value may be a PBKDF2 hash or, for configurations that predate hashing, the
// plaintext itself. Both paths are constant-time.
func Matches(stored, presented string) bool {
	if LooksLikeHash(stored) {
		return VerifyPassword(stored, presented)
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(presented)) == 1
}

// VerifyPassword checks a plaintext password against a stored PBKDF2 verifier.
// A malformed verifier is a non-match, never an error the caller could mistake
// for success.
func VerifyPassword(stored, password string) bool {
	parts := strings.Split(stored, "$")
	if len(parts) != 4 {
		return false
	}
	var newHash func() hash.Hash
	switch parts[0] {
	case passwordPrefix:
		newHash = sha256.New
	case sha512Prefix:
		newHash = sha512.New
	default:
		return false
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 1 || iterations > MaxIterations {
		return false
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) == 0 {
		return false
	}
	want, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil || len(want) == 0 {
		return false
	}
	got, err := pbkdf2.Key(newHash, password, salt, iterations, len(want))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(got, want) == 1
}

// Verifier is the server side of one SCRAM credential: everything RFC 5802
// needs to answer a challenge without ever holding the password.
type Verifier struct {
	Mechanism  Mechanism
	Iterations int
	Salt       []byte
	StoredKey  []byte
	ServerKey  []byte
}

// DeriveVerifier computes the SCRAM verifier for a plaintext password.
func DeriveVerifier(m Mechanism, password string, salt []byte, iterations int) (Verifier, error) {
	if iterations <= 0 {
		iterations = DefaultSCRAMIterations
	}
	if iterations > MaxIterations {
		return Verifier{}, fmt.Errorf("credential: %d iterations exceeds the maximum of %d", iterations, MaxIterations)
	}
	salted, err := pbkdf2.Key(m.NewHash, password, salt, iterations, m.KeySize)
	if err != nil {
		return Verifier{}, err
	}
	clientKey := hmacSum(m, salted, []byte("Client Key"))
	h := m.NewHash()
	h.Write(clientKey)
	storedKey := h.Sum(nil)
	return Verifier{
		Mechanism:  m,
		Iterations: iterations,
		Salt:       append([]byte(nil), salt...),
		StoredKey:  storedKey,
		ServerKey:  hmacSum(m, salted, []byte("Server Key")),
	}, nil
}

// Signature computes HMAC(key, message) with the verifier's hash family; SCRAM
// uses it for both the client and the server signature.
func (v Verifier) Signature(key, message []byte) []byte {
	return hmacSum(v.Mechanism, key, message)
}

// String renders the verifier in the configuration format:
//
//	scram-sha-256$<iterations>$<base64 salt>$<base64 stored key>$<base64 server key>
func (v Verifier) String() string {
	name := "sha-256"
	if v.Mechanism.KeySize == sha512.Size {
		name = "sha-512"
	}
	return fmt.Sprintf("%s%s$%d$%s$%s$%s", scramPrefix, name, v.Iterations,
		base64.StdEncoding.EncodeToString(v.Salt),
		base64.StdEncoding.EncodeToString(v.StoredKey),
		base64.StdEncoding.EncodeToString(v.ServerKey))
}

// ParseVerifier parses the string form produced by Verifier.String.
func ParseVerifier(s string) (Verifier, error) {
	parts := strings.Split(strings.TrimSpace(s), "$")
	if len(parts) != 5 || !strings.HasPrefix(parts[0], scramPrefix) {
		return Verifier{}, errors.New("credential: not a SCRAM verifier")
	}
	m, ok := MechanismByName("SCRAM-" + strings.ToUpper(strings.Replace(parts[0], scramPrefix, "", 1)))
	if !ok {
		return Verifier{}, fmt.Errorf("credential: unsupported SCRAM verifier %q", parts[0])
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 1 || iterations > MaxIterations {
		return Verifier{}, fmt.Errorf("credential: invalid SCRAM iteration count %q", parts[1])
	}
	salt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) == 0 {
		return Verifier{}, errors.New("credential: invalid SCRAM salt")
	}
	storedKey, err := base64.StdEncoding.DecodeString(parts[3])
	if err != nil || len(storedKey) != m.KeySize {
		return Verifier{}, errors.New("credential: invalid SCRAM stored key")
	}
	serverKey, err := base64.StdEncoding.DecodeString(parts[4])
	if err != nil || len(serverKey) != m.KeySize {
		return Verifier{}, errors.New("credential: invalid SCRAM server key")
	}
	return Verifier{Mechanism: m, Iterations: iterations, Salt: salt, StoredKey: storedKey, ServerKey: serverKey}, nil
}

func hmacSum(m Mechanism, key, msg []byte) []byte {
	mac := hmac.New(m.NewHash, key)
	mac.Write(msg)
	return mac.Sum(nil)
}
