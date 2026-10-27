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

const (
	ResultOK    = "ok"
	ResultAllow = "allow"
	ResultDeny  = "deny"
	ResultError = "error"
)

const Anonymous = "anonymous"

const (
	maxFileBytes = 8 << 20

	maxFieldBytes = 512
)

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

var authFailureActions = map[string]bool{
	"auth.login":  true,
	"auth.bearer": true,
	"auth.sasl":   true,
	"auth.mqtt":   true,
	"auth.basic":  true,
}

var authzDenialActions = map[string]bool{
	"auth.request":  true,
	"auth.list":     true,
	"auth.route":    true,
	"kafka.request": true,
}

func AuthFailures() int64 { return authFailures.Load() }

func AuthzDenials() int64 { return authzDenials.Load() }

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

func Enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return file != nil
}

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

func openAppend(p string) (*os.File, error) {
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("audit: open %s: %w", p, err)
	}
	return f, nil
}

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
