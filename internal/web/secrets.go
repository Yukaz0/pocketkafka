package web

// A per-cluster bearer token is a credential for another broker, so it is sealed
// with AES-256-GCM rather than left in the data directory.
//
// The key comes from web.secrets_key (or KAFKA_SECRETS_KEY) or from a 0600 key
// file created once beside the registry. A passphrase is stretched with
// PBKDF2-HMAC-SHA256 before it is used as an AES key (the first version used a
// bare SHA-256, which makes a weak passphrase cheap to attack offline). Sealed
// values carry an "enc:v2:" prefix; "enc:v1:" values written by an older broker
// still open and are re-sealed with the new derivation on the next persist.

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
	secretPrefix  = "enc:v2:"
	legacyPrefix  = "enc:v1:"
	secretsKeyEnv = "KAFKA_SECRETS_KEY"
	// minSecretsKeyChars mirrors the floor web.secrets_key has to meet in
	// config validation, so the environment and key-file paths cannot be weaker.
	minSecretsKeyChars = 16
	// secretsKDFSalt is a fixed context string. The passphrase is the entropy and
	// the salt only separates purposes, so two installations with the same
	// passphrase derive the same key; that is required for a restart (or a
	// restored backup) to read what it sealed. The consequence is that the
	// passphrase must carry real entropy on its own: PBKDF2 raises the cost of
	// guessing it, it does not add any.
	secretsKDFSalt       = "pocketkafka-secrets-key-v2"
	secretsKDFIterations = 200000
)

type secretBox struct {
	// aead is the current derivation; legacy opens values sealed before the
	// key-derivation upgrade.
	aead   cipher.AEAD
	legacy cipher.AEAD
	// generated tells the caller this process created the key file so it can ask
	// the operator to back it up: losing it makes stored tokens unreadable.
	generated bool
}

// newSecretBox returns nil when there is nothing to protect, and a nil box with
// an error when a key was asked for but could not be used. Callers must read a
// non-nil error as "no secrets can be stored", never as "store them unencrypted".
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
	// The floor applies to the *effective* key, whatever its source: a short
	// passphrase from the environment or from a key file is exactly as weak as a
	// short one in the config, and config validation only sees the latter.
	if len(key) < minSecretsKeyChars {
		return nil, fmt.Errorf("secrets key from the environment or key file is %d characters; at least %d are required to seal stored credentials", len(key), minSecretsKeyChars)
	}
	if fromFile {
		// The key file lives beside the data it protects, so a leaked volume
		// leaks both. Say so once, with the way out.
		log.Printf("web: sealing cluster credentials with the key file %s; set web.secrets_key or %s from a secret store to keep the key off the data volume", keyFile, secretsKeyEnv)
	}
	return mustBox(key)
}

// mustBox builds the AEAD from a passphrase. The passphrase is stretched with
// PBKDF2, so a short operator-chosen value still costs an attacker real work;
// GCM authenticates the ciphertext, so a wrong key fails to open rather than
// returning garbage.
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
	switch {
	case strings.HasPrefix(value, secretPrefix):
		return b.openWith(b.aead, strings.TrimPrefix(value, secretPrefix))
	case strings.HasPrefix(value, legacyPrefix):
		// A value sealed with the pre-PBKDF2 derivation is readable; reporting
		// it as "plain" makes the caller rewrite it with the current one.
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
