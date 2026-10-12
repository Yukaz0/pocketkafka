package web

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	secretPrefix         = "enc:v2:"
	legacyPrefix         = "enc:v1:"
	secretsKeyEnv        = "KAFKA_SECRETS_KEY"
	minSecretsKeyChars   = 16
	secretsKDFSalt       = "pocketkafka-secrets-key-v2"
	secretsKDFIterations = 200000
)

type secretBox struct {
	aead      cipher.AEAD
	legacy    cipher.AEAD
	generated bool
}

func newSecretBox(key, keyFile string) (*secretBox, error) {
	fromFile := false
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
			b, err := mustBox(key)
			if err != nil {
				return nil, err
			}
			b.generated = true
			return b, nil
		case err != nil:
			return nil, err
		default:
			key = strings.TrimSpace(string(raw))
			fromFile = true
		}
	}
	if key == "" {
		return nil, errors.New("secrets key is empty")
	}
	if len(key) < minSecretsKeyChars {
		return nil, fmt.Errorf("secrets key from the environment or key file is %d characters; at least %d are required to seal stored credentials", len(key), minSecretsKeyChars)
	}
	if fromFile {
		log.Printf("web: sealing cluster credentials with the key file %s; set web.secrets_key or %s from a secret store to keep the key off the data volume", keyFile, secretsKeyEnv)
	}
	return mustBox(key)
}

func mustBox(key string) (*secretBox, error) {
	derived, err := pbkdf2.Key(sha256.New, key, []byte(secretsKDFSalt), secretsKDFIterations, 32)
	if err != nil {
		return nil, fmt.Errorf("derive secrets key: %w", err)
	}
	aead, err := newAEAD(derived)
	if err != nil {
		return nil, err
	}
	legacySum := sha256.Sum256([]byte(key))
	legacy, err := newAEAD(legacySum[:])
	if err != nil {
		return nil, err
	}
	return &secretBox{aead: aead, legacy: legacy}, nil
}

func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

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

func (b *secretBox) open(value string) (plain string, wasPlain bool, err error) {
	switch {
	case strings.HasPrefix(value, secretPrefix):
		return b.openWith(b.aead, strings.TrimPrefix(value, secretPrefix))
	case strings.HasPrefix(value, legacyPrefix):
		plain, _, err := b.openWith(b.legacy, strings.TrimPrefix(value, legacyPrefix))
		return plain, true, err
	default:
		return value, value != "", nil
	}
}

func (b *secretBox) openWith(aead cipher.AEAD, encoded string) (string, bool, error) {
	if aead == nil {
		return "", false, errors.New("sealed secret uses a derivation this broker cannot read")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", false, err
	}
	n := aead.NonceSize()
	if len(raw) < n {
		return "", false, errors.New("sealed secret is truncated")
	}
	dec, err := aead.Open(nil, raw[:n], raw[n:], nil)
	if err != nil {
		return "", false, fmt.Errorf("open sealed secret (wrong secrets key?): %w", err)
	}
	return string(dec), false, nil
}
