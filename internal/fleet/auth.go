package fleet

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	DirectionAgentToControl = "agent→control"
	DirectionControlToAgent = "control→agent"
)

var (
	ErrNilEnvelope      = errors.New("fleet: envelope is nil")
	ErrExpired          = errors.New("fleet: envelope expired")
	ErrReplay           = errors.New("fleet: message replay detected")
	ErrInvalidHMAC      = errors.New("fleet: invalid hmac")
	ErrInvalidBodyHash  = errors.New("fleet: invalid body hash")
	ErrInvalidTimestamp = errors.New("fleet: invalid timestamp")
	ErrCrossEndpoint    = errors.New("fleet: cross-endpoint mismatch")
)

func PayloadSHA256(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func ComputeHMAC(key, direction, op, bodySHA256 string, issuedAt int64, messageID string) string {
	mac := hmac.New(sha256.New, []byte(key))
	data := direction + "\n" + op + "\n" + bodySHA256 + "\n" + strconv.FormatInt(issuedAt, 10) + "\n" + messageID
	mac.Write([]byte(data))
	return hex.EncodeToString(mac.Sum(nil))
}

func Sign(key, direction, op string, payload []byte, ttl time.Duration) (*Envelope, error) {
	if key == "" {
		return nil, errors.New("fleet: key cannot be empty")
	}
	now := time.Now()
	msgID := generateMessageID()
	bodyHash := PayloadSHA256(payload)
	issuedAt := now.Unix()
	expiry := now.Add(ttl).Unix()
	sig := ComputeHMAC(key, direction, op, bodyHash, issuedAt, msgID)

	return &Envelope{
		ProtocolVersion: 1,
		MessageID:       msgID,
		Direction:       direction,
		IssuedAt:        issuedAt,
		Expiry:          expiry,
		Op:              op,
		BodySHA256:      bodyHash,
		HMAC:            sig,
		Payload:         payload,
	}, nil
}

type Verifier struct {
	key       string
	window    time.Duration
	direction string
	registry  *Registry

	mu   sync.Mutex
	seen map[string]int64
}

func NewVerifier(key string, window time.Duration, direction ...string) *Verifier {
	dir := ""
	if len(direction) > 0 {
		dir = direction[0]
	}
	return &Verifier{
		key:       key,
		window:    window,
		direction: dir,
		seen:      make(map[string]int64),
	}
}

func (v *Verifier) WithRegistry(r *Registry) *Verifier {
	v.registry = r
	return v
}

func (v *Verifier) Verify(env *Envelope) error {
	if env == nil {
		return ErrNilEnvelope
	}
	if env.ProtocolVersion != 0 && env.ProtocolVersion != 1 {
		return fmt.Errorf("fleet: unsupported protocol version %d", env.ProtocolVersion)
	}
	if v.direction != "" && env.Direction != "" && env.Direction != v.direction {
		return fmt.Errorf("fleet: direction mismatch %q, expected %q: %w", env.Direction, v.direction, ErrCrossEndpoint)
	}

	now := time.Now().Unix()

	if env.Expiry > 0 && now > env.Expiry {
		return fmt.Errorf("fleet: envelope expired at %d, now %d: %w", env.Expiry, now, ErrExpired)
	}
	if env.IssuedAt > 0 {
		if env.IssuedAt > now+30 {
			return fmt.Errorf("fleet: envelope issued in future: %w", ErrInvalidTimestamp)
		}
		if v.window > 0 && now-env.IssuedAt > int64(v.window.Seconds()) {
			return fmt.Errorf("fleet: envelope issued outside window: %w", ErrExpired)
		}
	}

	expectedBodyHash := PayloadSHA256(env.Payload)
	if env.BodySHA256 != "" && env.BodySHA256 != expectedBodyHash {
		return fmt.Errorf("fleet: body hash mismatch: %w", ErrInvalidBodyHash)
	}

	verificationKey := v.key
	if verificationKey == "" && v.registry != nil && env.AgentID != "" {
		decKey, err := v.registry.agentKey(env.AgentID)
		if err != nil {
			return fmt.Errorf("fleet: agent key unavailable: %w", err)
		}
		verificationKey = decKey
	}

	expectedHMAC := ComputeHMAC(verificationKey, env.Direction, env.Op, expectedBodyHash, env.IssuedAt, env.MessageID)
	if !hmac.Equal([]byte(env.HMAC), []byte(expectedHMAC)) {
		return fmt.Errorf("fleet: invalid hmac: %w", ErrInvalidHMAC)
	}

	if v.registry != nil && env.AgentID != "" {
		if _, err := v.registry.AuthorizeStream(env.AgentID); err != nil {
			return fmt.Errorf("fleet: agent unauthorized: %w", err)
		}
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	for id, exp := range v.seen {
		if now > exp {
			delete(v.seen, id)
		}
	}

	if _, ok := v.seen[env.MessageID]; ok {
		return fmt.Errorf("fleet: message replay detected for %q: %w", env.MessageID, ErrReplay)
	}

	expiry := env.Expiry
	if expiry <= 0 {
		expiry = now + int64(v.window.Seconds())
	}
	v.seen[env.MessageID] = expiry

	return nil
}

func generateMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(b[:])
}
