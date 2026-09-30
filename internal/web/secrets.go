package web

// A per-cluster bearer token is a credential for another broker, so it is sealed
// with AES-256-GCM rather than left in the data directory. The key comes from
// web.secrets_key (or KAFKA_SECRETS_KEY) or a 0600 key file created once beside
// the registry; sealed values carry an "enc:v1:" prefix, so a file written before
// encryption existed still loads and is re-sealed on the next persist.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	secretPrefix  = "enc:v1:"
	secretsKeyEnv = "KAFKA_SECRETS_KEY"
)

type secretBox struct {
	aead cipher.AEAD
	// generated tells the caller this process created the key file so it can ask
	// the operator to back it up: losing it makes stored tokens unreadable.
	generated bool
}

// newSecretBox returns nil when there is nothing to protect, and a nil box with
// an error when a key was asked for but could not be used. Callers must read a
// non-nil error as "no secrets can be stored", never as "store them unencrypted".
func newSecretBox(key, keyFile string) (*secretBox, error) {
	if key == "" {
		raw, err := os.ReadFile(keyFile)
		switch {
		case errors.Is(err, os.ErrNotExist):
			buf := make([]byte, 32)
			if _, err := rand.Read(buf); err != nil {
				return nil, err
			}
			if err := os.MkdirAll(filepath.Dir(keyFile), 0o700); err != nil {
				return nil, err
			}
			key = hex.EncodeToString(buf)
			if err := os.WriteFile(keyFile, []byte(key), 0o600); err != nil {
				return nil, err
			}
			b := mustBox(key)
			b.generated = true
			return b, nil
		case err != nil:
			return nil, err
		default:
			key = strings.TrimSpace(string(raw))
		}
	}
	if key == "" {
		return nil, errors.New("secrets key is empty")
	}
	return mustBox(key), nil
}

// mustBox derives a 32-byte key from the passphrase; GCM authenticates the
// ciphertext, so a wrong key fails to open rather than returning garbage.
func mustBox(key string) *secretBox {
	sum := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		panic("web: aes: " + err.Error())
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic("web: gcm: " + err.Error())
	}
	return &secretBox{aead: aead}
}

// seal encrypts a secret. An empty secret stays empty: "not set" must not look
// like "set".
func (b *secretBox) seal(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return secretPrefix + base64.StdEncoding.EncodeToString(b.aead.Seal(nonce, nonce, []byte(plain), nil)), nil
}

// open decrypts a secret. wasPlain reports a value that was stored before
// encryption existed, which the caller rewrites sealed on the next persist.
func (b *secretBox) open(value string) (plain string, wasPlain bool, err error) {
	if !strings.HasPrefix(value, secretPrefix) {
		return value, value != "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, secretPrefix))
	if err != nil {
		return "", false, err
	}
	n := b.aead.NonceSize()
	if len(raw) < n {
		return "", false, errors.New("sealed secret is truncated")
	}
	dec, err := b.aead.Open(nil, raw[:n], raw[n:], nil)
	if err != nil {
		return "", false, fmt.Errorf("open sealed secret (wrong secrets key?): %w", err)
	}
	return string(dec), false, nil
}
