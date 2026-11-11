package fleet

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuditSanitizesSecret(t *testing.T) {
	dir := t.TempDir()
	a := NewAudit(dir)
	a.Log(AuditEvent{Actor: "op", Action: "enroll", Detail: "token=abc123secret"})
	raw, err := os.ReadFile(filepath.Join(dir, "fleet_audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("audit log file empty")
	}
	if strings.Contains(string(raw), "abc123secret") {
		t.Fatal("secret bocor ke audit log")
	}
}

func TestAuditSanitizesMultipleSecretFormats(t *testing.T) {
	dir := t.TempDir()
	a := NewAudit(dir)

	a.Log(AuditEvent{
		Actor:         "op",
		Action:        "auth",
		Target:        "srv-1",
		Detail:        `password="p@ssword123" secret=super_secret_key Bearer eyJhbGciOi token:my-tok`,
		CorrelationID: "corr-1",
	})

	raw, err := os.ReadFile(filepath.Join(dir, "fleet_audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	s := string(raw)
	for _, leaked := range []string{"p@ssword123", "super_secret_key", "eyJhbGciOi", "my-tok"} {
		if strings.Contains(s, leaked) {
			t.Fatalf("secret %q leaked into audit log", leaked)
		}
	}
}

func TestAuditSanitizesControlCharacters(t *testing.T) {
	dir := t.TempDir()
	a := NewAudit(dir)
	a.Log(AuditEvent{
		Actor:  "op\ninjected",
		Action: "run\r\nline2",
		Detail: "bad\nline",
	})
	raw, err := os.ReadFile(filepath.Join(dir, "fleet_audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("audit file lines = %d, want exactly 1 line (newlines must be sanitized)", len(lines))
	}
}

func TestAuditRetentionAndPrune(t *testing.T) {
	dir := t.TempDir()
	a := NewAudit(dir)
	a.SetRetention(1, 3)

	for i := range 5 {
		a.Log(AuditEvent{
			Actor:  "op",
			Action: "action",
			Detail: "event",
			Time:   time.Now().UTC().Add(time.Duration(i) * time.Second),
		})
	}

	pruned, err := a.Prune()
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 2 {
		t.Fatalf("pruned = %d, want 2", pruned)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "fleet_audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 3 {
		t.Fatalf("remaining lines = %d, want 3", len(lines))
	}

	events := a.List()
	if len(events) != 3 {
		t.Fatalf("events in-memory = %d, want 3", len(events))
	}
}

func TestAuditSanitizesCamelCaseSecrets(t *testing.T) {
	dir := t.TempDir()
	a := NewAudit(dir)

	secrets := []struct {
		detail string
		leak   string
	}{
		{"apiKey=my_api_key_val_1", "my_api_key_val_1"},
		{"secretKey=my_secret_key_val_2", "my_secret_key_val_2"},
		{"apikey=my_apikey_val_3", "my_apikey_val_3"},
		{"SECRET_KEY=my_secret_key_val_4", "my_secret_key_val_4"},
		{`{"apiKey": "json_api_key_5"}`, "json_api_key_5"},
		{`{"secretKey": "json_secret_key_6"}`, "json_secret_key_6"},
		{`{"SECRET_KEY": "json_secret_key_7"}`, "json_secret_key_7"},
		{`{"rawSecret": "json_raw_secret_8"}`, "json_raw_secret_8"},
		{`{"accessToken": "json_access_token_9"}`, "json_access_token_9"},
		{`{"clientSecret": "json_client_secret_10"}`, "json_client_secret_10"},
	}

	for _, s := range secrets {
		a.Log(AuditEvent{
			Actor:  "op",
			Action: "test",
			Detail: s.detail,
		})
	}

	raw, err := os.ReadFile(filepath.Join(dir, "fleet_audit.jsonl"))
	if err != nil {
		t.Fatalf("read audit file: %v", err)
	}
	content := string(raw)
	for _, s := range secrets {
		if strings.Contains(content, s.leak) {
			t.Fatalf("secret leaked: %s in detail %s", s.leak, s.detail)
		}
	}
}

func TestAuditLargeLineLoad(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "fleet_audit.jsonl")
	largeDetail := strings.Repeat("B", 128*1024)

	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create file: %v", err)
	}
	line := fmt.Sprintf(`{"id":"audit-large-1","actor":"op","action":"test","detail":"%s","time":"2026-10-26T00:00:00Z"}`+"\n", largeDetail)
	if _, err := f.WriteString(line); err != nil {
		t.Fatalf("write line: %v", err)
	}
	f.Close()

	a := NewAudit(dir)
	events := a.List()
	if len(events) != 1 {
		t.Fatalf("events loaded = %d, want 1", len(events))
	}
	if len(events[0].Detail) != 128*1024 {
		t.Fatalf("loaded detail length = %d, want %d", len(events[0].Detail), 128*1024)
	}
}
