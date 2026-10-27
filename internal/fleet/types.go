package fleet

import (
	"time"
)

type Server struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Host      string    `json:"host,omitempty"`
	Status    string    `json:"status"`
	AgentID   string    `json:"agent_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Agent struct {
	ID         string    `json:"id"`
	ServerID   string    `json:"server_id"`
	Secret     string    `json:"secret"`
	SealedKey  string    `json:"sealed_key,omitempty"`
	RawSecret  string    `json:"-"`
	Revoked    bool      `json:"revoked"`
	RevokedAt  time.Time `json:"revoked_at,omitempty"`
	EnrolledAt time.Time `json:"enrolled_at"`
	LastSeenAt time.Time `json:"last_seen_at,omitempty"`
}

type Enrollment struct {
	Token     string     `json:"token"`
	ServerID  string     `json:"server_id"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Used      bool       `json:"used"`
	UsedAt    time.Time  `json:"used_at,omitempty"`
}

type JobStatus string

const (
	JobStatusQueued    JobStatus = "queued"
	JobStatusAccepted  JobStatus = "accepted"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
)

type Job struct {
	ID             string    `json:"id"`
	ServerID       string    `json:"server_id"`
	Action         string    `json:"action"`
	Target         string    `json:"target,omitempty"`
	Actor          string    `json:"actor,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Force          bool      `json:"force,omitempty"`
	Result         string    `json:"result,omitempty"`
	TailLines      int       `json:"tail_lines,omitempty"`
	Status         JobStatus `json:"status"`
	Error          string    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	StartedAt      time.Time `json:"started_at,omitempty"`
	CompletedAt    time.Time `json:"completed_at,omitempty"`
}

type AuditEvent struct {
	ID            string    `json:"id"`
	Actor         string    `json:"actor"`
	Target        string    `json:"target"`
	Action        string    `json:"action"`
	Time          time.Time `json:"time"`
	Result        string    `json:"result"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Detail        string    `json:"detail,omitempty"`
}

type Envelope struct {
	ProtocolVersion int    `json:"protocol_version"`
	MessageID       string `json:"message_id"`
	AgentID         string `json:"agent_id,omitempty"`
	Direction       string `json:"direction,omitempty"`
	IssuedAt        int64  `json:"issued_at"`
	Expiry          int64  `json:"expiry"`
	Op              string `json:"op"`
	BodySHA256      string `json:"body_sha256"`
	HMAC            string `json:"hmac"`
	Payload         []byte `json:"payload,omitempty"`
}

type HMACEnvelope = Envelope

type ErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
	ErrorText string `json:"error"`
}

const (
	ErrCodeBadRequest              = "ERR_BAD_REQUEST"
	ErrCodeUnauthorized            = "ERR_UNAUTHORIZED"
	ErrCodeForbidden               = "ERR_FORBIDDEN"
	ErrCodeNotFound                = "ERR_NOT_FOUND"
	ErrCodePayloadTooLarge         = "ERR_PAYLOAD_TOO_LARGE"
	ErrCodeRateLimited             = "ERR_RATE_LIMITED"
	ErrCodeLimitExceeded           = "ERR_LIMIT_EXCEEDED"
	ErrCodeConnectionLimitExceeded = "ERR_CONNECTION_LIMIT_EXCEEDED"
	ErrCodeInternal                = "ERR_INTERNAL"
)
