package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestSplitBrokers(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"localhost:9092", []string{"localhost:9092"}},
		{"a:1,b:2,c:3", []string{"a:1", "b:2", "c:3"}},
		{"", nil},
		{"a:1,", []string{"a:1"}},
		{",a:1", []string{"a:1"}},
	}
	for _, c := range cases {
		got := splitBrokers(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("splitBrokers(%q) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("splitBrokers(%q) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}

func TestURLEncode(t *testing.T) {
	if got := urlEncode("a/b/c"); got != "a%2Fb%2Fc" {
		t.Fatalf("urlEncode = %q, want a%%2Fb%%2Fc", got)
	}
	if got := urlEncode("plain"); got != "plain" {
		t.Fatalf("urlEncode = %q, want plain", got)
	}
}

// captureStdout runs fn with os.Stdout redirected and returns what it wrote.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stdout = old
	return <-done
}

func TestRenderTableJSON(t *testing.T) {
	old := outputFormat
	defer func() { outputFormat = old }()
	outputFormat = "json"

	out := captureStdout(t, func() {
		renderTable([]string{"Topic", "Log End Offset"}, [][]any{{"orders", 7}})
	})
	var decoded []map[string]any
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	if len(decoded) != 1 || decoded[0]["topic"] != "orders" {
		t.Fatalf("decoded = %v", decoded)
	}
}

func TestRenderTableDefaultsToTable(t *testing.T) {
	old := outputFormat
	defer func() { outputFormat = old }()
	outputFormat = ""

	out := captureStdout(t, func() {
		renderTable([]string{"a", "b"}, [][]any{{1, 2}})
	})
	if !strings.Contains(out, "a") || !strings.Contains(out, "1") {
		t.Fatalf("table output missing expected content: %q", out)
	}
	if strings.HasPrefix(strings.TrimSpace(out), "[") {
		t.Fatalf("default output should not be JSON: %q", out)
	}
}

// TestUsageListsSubcommands guards the CLI contract: every documented
// subcommand stays in the help text.
func TestUsageListsSubcommands(t *testing.T) {
	out := captureStderr(t, usage)
	for _, cmd := range []string{"cluster", "topics", "create", "delete", "produce", "consume", "groups", "offset", "schema"} {
		if !strings.Contains(out, cmd) {
			t.Errorf("usage text missing %q", cmd)
		}
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	w.Close()
	os.Stderr = old
	return <-done
}
