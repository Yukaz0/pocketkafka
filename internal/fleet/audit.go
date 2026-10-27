package fleet

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Yukaz0/pocketkafka/internal/atomicfile"
)

var (
	reKeyValSecret = regexp.MustCompile(`(?i)\b([a-z0-9_-]*(?:secret[-_]?key|api[-_]?key|private[-_]?key|access[-_]?key|auth[-_]?key|master[-_]?key|raw[-_]?secret|secret|token|password|passwd)|pass|pwd|key|auth|authorization|credential|credentials)\b(\s*[:=]\s*)(["']?)([^"'\s,;]+)(["']?)`)
	reBearerSecret = regexp.MustCompile(`(?i)\bBearer\s+([A-Za-z0-9._~+/-]+=*)`)
	reJSONSecret   = regexp.MustCompile(`(?i)("(?:[a-z0-9_-]*(?:secret[-_]?key|api[-_]?key|private[-_]?key|access[-_]?key|auth[-_]?key|master[-_]?key|raw[-_]?secret|secret|token|password|passwd)|pass|pwd|key|auth|authorization|credential|credentials)")\s*:\s*"([^"]+)"`)
)

type Audit struct {
	mu               sync.RWMutex
	filePath         string
	events           []*AuditEvent
	retentionDays    int
	retentionEntries int
}

func NewAudit(dirOrPath string) *Audit {
	a := &Audit{
		retentionDays:    30,
		retentionEntries: 10000,
	}

	if dirOrPath == "" {
		return a
	}

	if fi, err := os.Stat(dirOrPath); err == nil && !fi.IsDir() {
		a.filePath = dirOrPath
	} else if strings.HasSuffix(dirOrPath, ".jsonl") {
		a.filePath = dirOrPath
	} else {
		a.filePath = filepath.Join(dirOrPath, "fleet_audit.jsonl")
	}

	_ = a.load()
	return a
}

func (a *Audit) SetRetention(days int, entries int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if days > 0 {
		a.retentionDays = days
	}
	if entries > 0 {
		a.retentionEntries = entries
	}
}

func (a *Audit) Log(e AuditEvent) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if e.ID == "" {
		e.ID = generateAuditID()
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}

	e.Actor = sanitizeAuditString(e.Actor)
	e.Target = sanitizeAuditString(e.Target)
	e.Action = sanitizeAuditString(e.Action)
	e.Result = sanitizeAuditString(e.Result)
	e.CorrelationID = sanitizeAuditString(e.CorrelationID)
	e.Detail = sanitizeAuditString(e.Detail)

	eventCopy := copyAuditEvent(&e)
	a.events = append(a.events, eventCopy)

	if err := a.appendLocked(eventCopy); err != nil {
		slog.Error("fleet: audit write", "error", err, "path", a.filePath,
			"actor", eventCopy.Actor, "action", eventCopy.Action)
	} else {
		slog.Info("fleet: audit", "actor", eventCopy.Actor, "action", eventCopy.Action,
			"target", eventCopy.Target, "result", eventCopy.Result)
	}
}

func (a *Audit) List() []*AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]*AuditEvent, 0, len(a.events))
	for _, e := range a.events {
		out = append(out, copyAuditEvent(e))
	}
	return out
}

func (a *Audit) Query(limit int) []*AuditEvent {
	a.mu.RLock()
	defer a.mu.RUnlock()

	total := len(a.events)
	if limit <= 0 || limit > total {
		limit = total
	}

	out := make([]*AuditEvent, 0, limit)
	for i := range limit {
		idx := total - 1 - i
		out = append(out, copyAuditEvent(a.events[idx]))
	}
	return out
}

func (a *Audit) Prune() (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoff := time.Time{}
	if a.retentionDays > 0 {
		cutoff = time.Now().UTC().Add(-time.Duration(a.retentionDays) * 24 * time.Hour)
	}

	pruned := 0
	var kept []*AuditEvent

	for _, e := range a.events {
		if !cutoff.IsZero() && e.Time.Before(cutoff) {
			pruned++
			continue
		}
		kept = append(kept, e)
	}

	if a.retentionEntries > 0 && len(kept) > a.retentionEntries {
		excess := len(kept) - a.retentionEntries
		pruned += excess
		kept = kept[excess:]
	}

	a.events = kept

	if a.filePath != "" {
		if err := a.rewriteFileLocked(); err != nil {
			return pruned, err
		}
	}

	return pruned, nil
}

func (a *Audit) Close() error {
	return nil
}

func (a *Audit) appendLocked(e *AuditEvent) error {
	if a.filePath == "" {
		return nil
	}

	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("fleet: marshal audit event: %w", err)
	}

	dir := filepath.Dir(a.filePath)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("fleet: mkdir %s: %w", dir, err)
	}

	f, err := os.OpenFile(a.filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("fleet: open %s: %w", a.filePath, err)
	}
	defer f.Close()

	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("fleet: write %s: %w", a.filePath, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fleet: sync %s: %w", a.filePath, err)
	}

	return nil
}

func (a *Audit) rewriteFileLocked() error {
	if a.filePath == "" {
		return nil
	}

	var buf strings.Builder
	for _, e := range a.events {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	return atomicfile.Write(a.filePath, []byte(buf.String()), 0o600)
}

func (a *Audit) load() error {
	if a.filePath == "" {
		return nil
	}

	f, err := os.Open(a.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 10<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var e AuditEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			continue
		}
		a.events = append(a.events, &e)
	}

	return scanner.Err()
}

func sanitizeAuditString(s string) string {
	if s == "" {
		return ""
	}

	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r == '\n' || r == '\r' || unicode.IsControl(r) {
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(r)
	}
	out := strings.TrimSpace(b.String())

	out = reKeyValSecret.ReplaceAllString(out, "${1}${2}${3}[REDACTED]${5}")
	out = reBearerSecret.ReplaceAllString(out, "Bearer [REDACTED]")
	out = reJSONSecret.ReplaceAllString(out, `${1}: "[REDACTED]"`)

	return out
}

func generateAuditID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("audit-%d-%s", time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}

func copyAuditEvent(e *AuditEvent) *AuditEvent {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}
