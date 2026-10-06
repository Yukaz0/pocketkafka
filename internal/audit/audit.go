// Package audit writes the broker's security-relevant trail: who did what, to
// which resource, and whether it was allowed. Every record goes to the process
// log as a structured line and, when a path is configured, is appended to a
// 0600 JSON-lines file.
//
// The trail is deliberately independent of the dashboard's in-memory ring: a
// restart, a full ring, or an attacker flooding the UI must not erase it.
package audit

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// Result values.
const (
	ResultOK    = "ok"
	ResultAllow = "allow"
	ResultDeny  = "deny"
	ResultError = "error"
)

// Anonymous is the actor recorded for an event with no verified identity.
const Anonymous = "anonymous"

const (
	// maxFileBytes rotates the trail file before it grows past this size.
	maxFileBytes = 8 << 20
	// maxFieldBytes truncates a field so one oversized topic name or error
	// string cannot dominate (or wedge) the trail.
	maxFieldBytes = 512
)

// Event is one audit record. Callers must never put a password, token, or other
// secret into Detail or Resource.
type Event struct {
	Time       int64  `json:"time"`
	Actor      string `json:"actor"`
	Action     string `json:"action"`
	Resource   string `json:"resource"`
	Result     string `json:"result"`
	Detail     string `json:"detail,omitempty"`
	RemoteAddr string `json:"remoteAddr,omitempty"`
	RequestID  string `json:"requestId,omitempty"`
}

var (
	mu     sync.Mutex
	file   *os.File
	path   string
	closed bool

	authFailures atomic.Int64
	authzDenials atomic.Int64
)

// authFailureActions are the events that count as a refused credential (as
// opposed to a refused permission). Keeping the classification here means every
// ingress that already reports to the trail feeds the metric for free.
var authFailureActions = map[string]bool{
	"auth.login":  true,
	"auth.bearer": true,
	"auth.sasl":   true,
	"auth.mqtt":   true,
	"auth.basic":  true,
}

// authzDenialActions are the events that count as a permission refusal.
var authzDenialActions = map[string]bool{
	"auth.request":  true,
	"auth.list":     true,
	"auth.route":    true,
	"kafka.request": true,
}

// AuthFailures reports refused credentials since start.
func AuthFailures() int64 { return authFailures.Load() }

// AuthzDenials reports refused permissions since start.
func AuthzDenials() int64 { return authzDenials.Load() }

// Init points the trail at a file. An empty path keeps the trail in the process
// log only. Callers must call it once, before serving.
func Init(logPath string) error {
	mu.Lock()
	defer mu.Unlock()
	if file != nil {
		file.Close()
		file = nil
	}
	path = ""
	closed = false
	if logPath == "" {
		return nil
	}
	f, err := openAppend(logPath)
	if err != nil {
		return err
	}
	file = f
	path = logPath
	return nil
}

// Enabled reports whether the trail has a file sink.
func Enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return file != nil
}

// Close flushes and closes the file sink.
func Close() error {
	mu.Lock()
	defer mu.Unlock()
	closed = true
	if file == nil {
		return nil
	}
	err := file.Close()
	file = nil
	return err
}

// Log records one event. It never fails the caller: a trail that cannot be
// written reports itself in the process log rather than breaking the request
// that triggered it.
func Log(e Event) {
	if e.Time == 0 {
		e.Time = time.Now().UnixMilli()
	}
	e.Actor = sanitize(e.Actor)
	e.Action = sanitize(e.Action)
	e.Resource = sanitize(e.Resource)
	e.Result = sanitize(e.Result)
	e.Detail = sanitize(e.Detail)
	e.RemoteAddr = sanitize(e.RemoteAddr)

	if e.Result == ResultDeny {
		switch {
		case authFailureActions[e.Action]:
			authFailures.Add(1)
		case authzDenialActions[e.Action]:
			authzDenials.Add(1)
		}
	}

	// The structured process-log line is always emitted: it is what a log
	// shipper or journald already collects.
	attrs := []any{
		"actor", e.Actor,
		"action", e.Action,
		"resource", e.Resource,
		"result", e.Result,
	}
	if e.Detail != "" {
		attrs = append(attrs, "detail", e.Detail)
	}
	if e.RemoteAddr != "" {
		attrs = append(attrs, "remote_addr", e.RemoteAddr)
	}
	if e.RequestID != "" {
		attrs = append(attrs, "request_id", e.RequestID)
	}
	slog.Info("audit", attrs...)

	mu.Lock()
	defer mu.Unlock()
	if file == nil || closed {
		return
	}
	line, err := json.Marshal(e)
	if err != nil {
		slog.Error("audit: encode", "error", err)
		return
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		slog.Error("audit: write", "error", err, "path", path)
		return
	}
	rotateLocked()
}

// rotateLocked rolls the file over to <path>.1 once it exceeds maxFileBytes.
func rotateLocked() {
	info, err := file.Stat()
	if err != nil || info.Size() < maxFileBytes {
		return
	}
	file.Close()
	file = nil
	if err := os.Rename(path, path+".1"); err != nil {
		slog.Error("audit: rotate", "error", err, "path", path)
	}
	f, err := openAppend(path)
	if err != nil {
		slog.Error("audit: reopen after rotate", "error", err, "path", path)
		return
	}
	file = f
}

// openAppend opens the trail with 0600 permissions, creating it when missing.
func openAppend(p string) (*os.File, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("audit: open %s: %w", p, err)
	}
	return f, nil
}

// sanitize strips control characters (a forged newline would otherwise let a
// caller inject whole lines into the trail) and bounds the length.
func sanitize(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if unicode.IsControl(r) {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(r)
	}
	out := b.String()
	if len(out) > maxFieldBytes {
		out = out[:maxFieldBytes] + "…"
	}
	return out
}
