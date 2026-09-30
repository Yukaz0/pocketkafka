package main

import (
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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

// TestSubcommandsRejectMissingArgs guards a bug that made the CLI useless for
// data operations: the arity guards read fs.Args(), which only holds flags
// written AFTER the subcommand, so it was empty for the documented flags-first
// form and produce/create/delete/consume exited 1 with a usage message even
// when their arguments were complete.
//
// The check now runs before the client is created, which is what makes this
// test bite: pointed at a dead address the old code reported a connection error
// for an incomplete command, while the fixed code names the missing argument.
func TestSubcommandsRejectMissingArgs(t *testing.T) {
	bin := buildCLI(t)
	const dead = "127.0.0.1:1"

	missing := []struct {
		name string
		args []string
		want string
	}{
		{"produce without value", []string{"-b", dead, "produce", "orders"}, "usage: pkctl produce"},
		{"produce without topic or value", []string{"-b", dead, "produce"}, "usage: pkctl produce"},
		{"create without topic", []string{"-b", dead, "create"}, "usage: pkctl create"},
		{"delete without topic", []string{"-b", dead, "delete"}, "usage: pkctl delete"},
		{"consume without topic", []string{"-b", dead, "consume"}, "usage: pkctl consume"},
	}
	for _, tc := range missing {
		out, _ := exec.Command(bin, tc.args...).CombinedOutput()
		if !strings.Contains(string(out), tc.want) {
			t.Errorf("%s: want %q, got: %s", tc.name, tc.want, strings.TrimSpace(string(out)))
		}
	}

	// A complete invocation gets past the guard: with no broker listening the
	// next failure is the connection, never a usage message. Both flag orders
	// are exercised because the help text documents the flags-first form.
	for _, args := range [][]string{
		{"-b", dead, "produce", "orders", `{"n":1}`},
		{"produce", "orders", `{"n":1}`, "-b", dead},
		{"-b", dead, "create", "orders"},
		{"-b", dead, "delete", "orders"},
		{"-b", dead, "consume", "orders"},
	} {
		out, _ := exec.Command(bin, args...).CombinedOutput()
		if strings.Contains(string(out), "usage: pkctl") {
			t.Errorf("%v: complete invocation rejected: %s", args, strings.TrimSpace(string(out)))
		}
	}
}

func TestCheckArity(t *testing.T) {
	for _, rest := range [][]string{
		{"produce", "orders", `{"n":1}`}, {"create", "orders"}, {"delete", "orders"},
		{"consume", "orders"}, {"topics"}, {"cluster"}, {"groups"}, {"offset", "reset"},
	} {
		if err := checkArity(rest); err != nil {
			t.Errorf("checkArity(%v) = %v, want nil", rest, err)
		}
	}
	for _, rest := range [][]string{
		{"produce"}, {"produce", "orders"}, {"create"}, {"delete"}, {"consume"}, {},
	} {
		if err := checkArity(rest); err == nil {
			t.Errorf("checkArity(%v) = nil, want an error", rest)
		}
	}
}

func buildCLI(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "pkctl")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/kctl")
	cmd.Dir = "../.."
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pkctl: %v\n%s", err, out)
	}
	return bin
}
