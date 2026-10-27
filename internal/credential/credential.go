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
	passwordHashIterations = 210000

	DefaultSCRAMIterations = 15000

	MaxIterations = 1_000_000
)

const (
	passwordPrefix = "pbkdf2-sha256"
	sha512Prefix   = "pbkdf2-sha512"
	scramPrefix    = "scram-"
)

type Mechanism struct {
	Name    string
	KeySize int
	NewHash func() hash.Hash
}

var (
	SHA256 = Mechanism{Name: "SCRAM-SHA-256", KeySize: sha256.Size, NewHash: sha256.New}
	SHA512 = Mechanism{Name: "SCRAM-SHA-512", KeySize: sha512.Size, NewHash: sha512.New}
)

func MechanismByName(name string) (Mechanism, bool) {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "SCRAM-SHA-256":
		return SHA256, true
	case "SCRAM-SHA-512":
		return SHA512, true
	}
	return Mechanism{}, false
}

func RandomSalt(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

func HashPassword(password string) (string, error) {
	salt, err := RandomSalt(16)
	if err != nil {
		return "", err
	}
	return HashPasswordWith(password, salt, passwordHashIterations)
}

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

func LooksLikeHash(v string) bool {
	return strings.HasPrefix(v, passwordPrefix+"$") ||
		strings.HasPrefix(v, sha512Prefix+"$") ||
		strings.HasPrefix(v, scramPrefix)
}

func Matches(stored, presented string) bool {
	if LooksLikeHash(stored) {
		return VerifyPassword(stored, presented)
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(presented)) == 1
}

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

type Verifier struct {
	Mechanism  Mechanism
	Iterations int
	Salt       []byte
	StoredKey  []byte
	ServerKey  []byte
}

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

func (v Verifier) Signature(key, message []byte) []byte {
	return hmacSum(v.Mechanism, key, message)
}

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
