package fleet

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Agent key sealing follows the KDF and AES-GCM pattern in internal/web/secrets.go.
const (
	fleetSecretPrefix  = "enc:v1:"
	minMasterKeyChars  = 16
	fleetKDFSalt       = "pocketkafka-fleet-secret-key-v1"
	fleetKDFIterations = 200000
)

type secretBox struct {
	aead cipher.AEAD
}

func newSecretBox(masterKey string) (*secretBox, error) {
	if len(masterKey) < minMasterKeyChars {
		return nil, fmt.Errorf("fleet: master key is %d characters; at least %d are required", len(masterKey), minMasterKeyChars)
	}
	derived, err := pbkdf2.Key(sha256.New, masterKey, []byte(fleetKDFSalt), fleetKDFIterations, 32)
	if err != nil {
		return nil, fmt.Errorf("fleet: derive master key: %w", err)
	}
	block, err := aes.NewCipher(derived)
	if err != nil {
		return nil, fmt.Errorf("fleet: create cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("fleet: create gcm: %w", err)
	}
	return &secretBox{aead: aead}, nil
}

func (b *secretBox) seal(plain string) (string, error) {
	if plain == "" {
		return "", nil
	}
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", fmt.Errorf("fleet: generate nonce: %w", err)
	}
	sealed := b.aead.Seal(nonce, nonce, []byte(plain), nil)
	return fleetSecretPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

func (b *secretBox) open(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, fleetSecretPrefix) {
		return "", errors.New("fleet: sealed key has invalid prefix")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, fleetSecretPrefix))
	if err != nil {
		return "", fmt.Errorf("fleet: decode sealed key: %w", err)
	}
	nonceSize := b.aead.NonceSize()
	if len(raw) < nonceSize {
		return "", errors.New("fleet: sealed key is truncated")
	}
	nonce, ciphertext := raw[:nonceSize], raw[nonceSize:]
	dec, err := b.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("fleet: open sealed key (wrong master key?): %w", err)
	}
	return string(dec), nil
}
