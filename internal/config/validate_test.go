package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDefaultConfigValid ensures the shipped defaults pass validation; a
// failure here means every broker start would abort.
func TestDefaultConfigValid(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Default().Validate() = %v, want nil", err)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string // substring the error must mention
	}{
		{
			name:   "zero max connections",
			mutate: func(c *Config) { c.Network.MaxConnections = 0 },
			want:   "network.max_connections",
		},
		{
			name:   "negative read buffer",
			mutate: func(c *Config) { c.Network.ReadBufferBytes = -1 },
			want:   "network.read_buffer_bytes",
		},
		{
			name:   "negative write buffer",
			mutate: func(c *Config) { c.Network.WriteBufferBytes = -1 },
			want:   "network.write_buffer_bytes",
		},
		{
			name:   "zero max request size",
			mutate: func(c *Config) { c.Network.MaxRequestSizeBytes = 0 },
			want:   "network.max_request_size_bytes",
		},
		{
			name:   "negative read timeout",
			mutate: func(c *Config) { c.Network.ReadTimeoutMs = -1 },
			want:   "network.read_timeout_ms",
		},
		{
			name:   "negative idle timeout",
			mutate: func(c *Config) { c.Network.IdleTimeoutMs = -5 },
			want:   "network.idle_timeout_ms",
		},
		{
			name:   "empty listener address",
			mutate: func(c *Config) { c.Listeners = map[string]string{"plain": ""} },
			want:   "listeners.plain",
		},
		{
			name:   "invalid listener address",
			mutate: func(c *Config) { c.Listeners = map[string]string{"plain": "not-an-addr"} },
			want:   "listeners.plain",
		},
		{
			name: "duplicate listener addresses",
			mutate: func(c *Config) {
				c.Listeners = map[string]string{"a": "127.0.0.1:9092", "b": "127.0.0.1:9092"}
			},
			want: "duplicate address",
		},
		{
			name: "no listeners",
			mutate: func(c *Config) {
				c.Listeners = map[string]string{}
			},
			want: "at least one listener",
		},
		{
			name: "unknown storage backend",
			mutate: func(c *Config) {
				c.Coordinator.StorageBackend = "rocksdb"
			},
			want: "coordinator.storage_backend",
		},
		{
			name: "unknown flush policy",
			mutate: func(c *Config) {
				c.Storage.FlushPolicy = "sometimes"
			},
			want: "storage.flush_policy",
		},
		{
			name: "interval flush without interval",
			mutate: func(c *Config) {
				c.Storage.FlushPolicy = FlushPolicyInterval
				c.Storage.FlushIntervalMs = 0
			},
			want: "storage.flush_interval_ms",
		},
		{
			name: "web auth with default secret",
			mutate: func(c *Config) {
				c.Web.Enabled = true
				c.Web.Auth = true
				c.Web.AuthSecret = "pocketkafka-web-secret"
			},
			want: "web.auth_secret",
		},
		{
			name: "web auth with empty secret",
			mutate: func(c *Config) {
				c.Web.Enabled = true
				c.Web.Auth = true
				c.Web.AuthSecret = ""
			},
			want: "web.auth_secret",
		},
		{
			name: "security enabled without users",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Security.Users = nil
			},
			want: "security.users",
		},
		{
			name: "tls without cert",
			mutate: func(c *Config) {
				c.Security.TLS.Enabled = true
			},
			want: "security.tls.cert_file",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mutate(&cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() = nil, want error mentioning %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

func TestValidateAccepts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"legacy pebble backend", func(c *Config) { c.Coordinator.StorageBackend = BackendPebbleLegacy }},
		{"inmemory backend", func(c *Config) { c.Coordinator.StorageBackend = BackendInMemory }},
		{"flush none", func(c *Config) { c.Storage.FlushPolicy = FlushPolicyNone }},
		{"flush request", func(c *Config) { c.Storage.FlushPolicy = FlushPolicyRequest }},
		{"web auth with strong secret", func(c *Config) {
			c.Web.Enabled = true
			c.Web.Auth = true
			c.Web.AuthSecret = "a-very-long-random-secret"
		}},
		{"security enabled with a user", func(c *Config) {
			c.Security.Enabled = true
			c.Security.Users = []SecurityUser{{Username: "app", Password: "s3cret"}}
			// Security enabled also enables web login, which needs a real secret.
			c.Web.AuthSecret = "a-strong-signing-secret"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mutate(&cfg)
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

// TestNormalizePebbleAlias verifies the legacy backend name is rewritten to the
// canonical one and that the operator is warned exactly once.
func TestNormalizePebbleAlias(t *testing.T) {
	var warnings []string
	origWarn := configWarnf
	configWarnf = func(format string, args ...interface{}) {
		warnings = append(warnings, format)
	}
	t.Cleanup(func() { configWarnf = origWarn })

	cfg := Default()
	cfg.Coordinator.StorageBackend = BackendPebbleLegacy
	cfg.Normalize()
	if cfg.Coordinator.StorageBackend != BackendFile {
		t.Fatalf("backend = %q, want %q", cfg.Coordinator.StorageBackend, BackendFile)
	}
	if len(warnings) != 1 {
		t.Fatalf("warnings = %d, want 1", len(warnings))
	}
}

// TestShippedConfigLoads ensures the checked-in sample config still passes
// normalization and validation, so `pocketkafka -config config/config.yaml`
// keeps working out of the box.
func TestShippedConfigLoads(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "config", "config.yaml")); err != nil {
		t.Fatalf("shipped config/config.yaml does not load: %v", err)
	}
}

// TestLoadValidates proves a bad on-disk config stops Load instead of returning
// a config that would open an unsafe listener.
func TestLoadValidates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "network:\n  max_connections: 0\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("Load() = nil error, want validation failure")
	}
}

// TestLoadNormalizesPebble confirms end-to-end that a legacy file on disk is
// normalized before validation.
func TestLoadNormalizesPebble(t *testing.T) {
	origWarn := configWarnf
	configWarnf = func(string, ...interface{}) {}
	t.Cleanup(func() { configWarnf = origWarn })

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := "coordinator:\n  storage_backend: \"pebble\"\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Coordinator.StorageBackend != BackendFile {
		t.Fatalf("backend = %q, want %q", cfg.Coordinator.StorageBackend, BackendFile)
	}
}
