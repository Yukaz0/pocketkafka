package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLogWritesJSONLinesAndCountsCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	if err := Init(path); err != nil {
		t.Fatal(err)
	}
	defer Close()

	if !Enabled() {
		t.Fatal("a configured path should enable the file sink")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("trail permissions = %o, want 600", perm)
	}

	before := AuthFailures()
	Log(Event{Actor: "app", Action: "auth.login", Resource: "dashboard", Result: ResultDeny, Detail: "invalid credentials"})
	Log(Event{Actor: "admin", Action: "auth.login", Resource: "dashboard", Result: ResultOK})
	Log(Event{Actor: "reader", Action: "auth.request", Resource: "/api/v1/acls", Result: ResultDeny})
	if got := AuthFailures() - before; got != 1 {
		t.Fatalf("auth failures delta = %d, want 1", got)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 3 {
		t.Fatalf("wrote %d lines, want 3:\n%s", len(lines), raw)
	}
	var first Event
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line is not JSON: %v (%s)", err, lines[0])
	}
	if first.Time == 0 || first.Actor != "app" || first.Action != "auth.login" || first.Result != ResultDeny {
		t.Fatalf("unexpected record %+v", first)
	}
}

func TestSanitizeStripsControlCharactersAndBoundsLength(t *testing.T) {
	injected := "orders\n{\"actor\":\"root\"}"
	if got := sanitize(injected); strings.Contains(got, "\n") {
		t.Fatalf("newline survived sanitising: %q", got)
	}
	long := strings.Repeat("x", maxFieldBytes*3)
	got := sanitize(long)
	if len(got) > maxFieldBytes+4 {
		t.Fatalf("sanitised length = %d, want <= %d", len(got), maxFieldBytes+4)
	}
	if sanitize("") != "" {
		t.Fatal("an empty field must stay empty")
	}
}
