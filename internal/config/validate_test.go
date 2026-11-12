package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		want   string
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
		{
			name: "security enabled with a default password",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Web.AuthSecret = "a-strong-signing-secret"
				c.Security.Users = []SecurityUser{{Username: "app", Password: "changeme"}}
			},
			want: "password is a default/too-short value",
		},
		{
			name: "security enabled with an unset environment reference",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Web.AuthSecret = "a-strong-signing-secret"
				c.Security.Users = []SecurityUser{{Username: "app", Password: "${KAFKA_TEST_UNSET_PASSWORD}"}}
			},
			want: "unset environment variable",
		},
		{
			name: "security enabled with two credential sources",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Web.AuthSecret = "a-strong-signing-secret"
				c.Security.Users = []SecurityUser{{Username: "app", Password: "a-good-password", PasswordHash: "pbkdf2-sha256$1$c2FsdA==$aGFzaA=="}}
			},
			want: "exactly one of",
		},
		{
			name: "super user that is not a configured user",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Web.AuthSecret = "a-strong-signing-secret"
				c.Security.Users = []SecurityUser{{Username: "app", Password: "a-good-password"}}
				c.Security.SuperUsers = []string{"root"}
			},
			want: "security.super_users",
		},
		{
			name: "web auth secret left as an unset environment reference",
			mutate: func(c *Config) {
				c.Web.Enabled = true
				c.Web.Auth = true
				c.Web.AuthSecret = "${KAFKA_TEST_SECRET_THAT_IS_NOT_SET}"
			},
			want: "references an unset environment variable",
		},
		{
			name: "security enabled with an unset auth secret reference",
			mutate: func(c *Config) {
				c.Security.Enabled = true
				c.Security.Users = []SecurityUser{{Username: "app", Password: "a-good-password"}}
				c.Web.AuthSecret = "${KAFKA_TEST_SECRET_THAT_IS_NOT_SET}"
			},
			want: "web.auth_secret",
		},
		{
			name: "cluster token left as an unset environment reference",
			mutate: func(c *Config) {
				c.Web.ClusterToken = "${KAFKA_TEST_TOKEN_THAT_IS_NOT_SET}"
			},
			want: "web.cluster_token",
		},
		{
			name: "secrets key left as an unset environment reference",
			mutate: func(c *Config) {
				c.Web.SecretsKey = "${KAFKA_TEST_KEY_THAT_IS_NOT_SET}"
			},
			want: "web.secrets_key",
		},
		{
			name: "open broker on a routable address",
			mutate: func(c *Config) {
				c.Listeners["plain"] = "0.0.0.0:9092"
			},
			want: "security.allow_insecure_public=true",
		},
		{
			name: "open REST proxy on a routable address",
			mutate: func(c *Config) {
				c.Gateway.Listen = "0.0.0.0:8082"
			},
			want: "gateway.listen",
		},
		{
			name: "plaintext tiered endpoint with the default secret",
			mutate: func(c *Config) {
				c.Storage.Tiered.Enabled = true
				c.Storage.Tiered.Endpoint = "http://minio.example.internal:9000"
			},
			want: "storage.tiered.endpoint",
		},
		{
			name: "pre-auth frame cap above the request cap",
			mutate: func(c *Config) {
				c.Network.PreAuthMaxRequestBytes = c.Network.MaxRequestSizeBytes + 1
			},
			want: "network.pre_auth_max_request_bytes",
		},
		{
			name: "unknown acl operation in a monitored cluster url",
			mutate: func(c *Config) {
				c.Web.Clusters = []WebCluster{{Name: "peer", URL: "ftp://peer:8080"}}
			},
			want: "scheme must be http or https",
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
			c.Security.Users = []SecurityUser{{Username: "app", Password: "a-good-password"}}

			c.Web.AuthSecret = "a-strong-signing-secret"
		}},
		{"security enabled with a scram verifier only", func(c *Config) {
			c.Security.Enabled = true
			c.Security.Users = []SecurityUser{{
				Username:      "app",
				SCRAMVerifier: "scram-sha-256$15000$MDEyMzQ1Njc4OWFiY2RlZg==$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=$MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
			}}
			c.Web.AuthSecret = "a-strong-signing-secret"
		}},
		{"password containing a literal dollar-brace without a closing brace", func(c *Config) {
			c.Security.Enabled = true
			c.Web.AuthSecret = "a-strong-signing-secret"

			c.Security.Users = []SecurityUser{{Username: "app", Password: "a-good${password"}}
		}},
		{"super user listed in users", func(c *Config) {
			c.Security.Enabled = true
			c.Security.Users = []SecurityUser{{Username: "admin", Password: "a-good-password"}}
			c.Security.SuperUsers = []string{"admin"}
			c.Web.AuthSecret = "a-strong-signing-secret"
		}},
		{"disabled side surfaces on a routable address", func(c *Config) {
			c.SchemaRegistry.Enabled, c.SchemaRegistry.Listen = false, "0.0.0.0:8081"
			c.Gateway.Enabled, c.Gateway.Listen = false, "0.0.0.0:8082"
			c.MQTT.Enabled, c.MQTT.Listen = false, "0.0.0.0:1883"
		}},
		{"explicit opt-in to an open broker", func(c *Config) {
			c.Listeners["plain"] = "0.0.0.0:9092"
			c.Security.AllowInsecurePublic = true
		}},
		{"tls for the http surfaces", func(c *Config) {
			c.Security.TLS.HTTP = true
			c.Security.TLS.CertFile = "/tmp/cert.pem"
			c.Security.TLS.KeyFile = "/tmp/key.pem"
			c.Security.TLS.MinVersion = "1.3"
		}},
		{"https tiered endpoint", func(c *Config) {
			c.Storage.Tiered.Enabled = true
			c.Storage.Tiered.Endpoint = "https://minio.example.internal:9000"
			c.Storage.Tiered.AccessKey = "tiered-access-key"
			c.Storage.Tiered.SecretKey = "a-strong-tiered-secret"
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

func TestShippedConfigLoads(t *testing.T) {
	if _, err := Load(filepath.Join("..", "..", "config", "config.yaml")); err != nil {
		t.Fatalf("shipped config/config.yaml does not load: %v", err)
	}
}

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
