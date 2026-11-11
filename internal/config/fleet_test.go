package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFleetDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Fleet.Enabled {
		t.Fatal("fleet must default disabled")
	}
	if cfg.Fleet.Listen != "127.0.0.1:18090" {
		t.Fatalf("listen = %q", cfg.Fleet.Listen)
	}
}

func TestFleetYAML(t *testing.T) {
	yamlContent := `
fleet:
  enabled: true
  listen: "127.0.0.1:19090"
  heartbeat_timeout: 45
  retention_days: 14
  retention_entries: 5000
  secret_key: "yaml-master-key-16-bytes"
  tls:
    cert_file: "/etc/fleet/cert.pem"
    key_file: "/etc/fleet/key.pem"
`
	t.Setenv("KAFKA_FLEET_SECRET_KEY", "")
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(yamlContent), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Fleet.Enabled {
		t.Fatal("fleet.enabled should be true")
	}
	if cfg.Fleet.Listen != "127.0.0.1:19090" {
		t.Fatalf("listen = %q", cfg.Fleet.Listen)
	}
	if cfg.Fleet.HeartbeatTimeout != 45 {
		t.Fatalf("heartbeat_timeout = %d", cfg.Fleet.HeartbeatTimeout)
	}
	if cfg.Fleet.RetentionDays != 14 {
		t.Fatalf("retention_days = %d", cfg.Fleet.RetentionDays)
	}
	if cfg.Fleet.RetentionEntries != 5000 {
		t.Fatalf("retention_entries = %d", cfg.Fleet.RetentionEntries)
	}
	if cfg.Fleet.SecretKey != "yaml-master-key-16-bytes" {
		t.Fatalf("secret_key = %q", cfg.Fleet.SecretKey)
	}
	if cfg.Fleet.TLS.CertFile != "/etc/fleet/cert.pem" {
		t.Fatalf("tls.cert_file = %q", cfg.Fleet.TLS.CertFile)
	}
	if cfg.Fleet.TLS.KeyFile != "/etc/fleet/key.pem" {
		t.Fatalf("tls.key_file = %q", cfg.Fleet.TLS.KeyFile)
	}
}

func TestFleetEnv(t *testing.T) {
	t.Setenv("KAFKA_FLEET_ENABLED", "true")
	t.Setenv("KAFKA_FLEET_LISTEN", "127.0.0.1:19099")
	t.Setenv("KAFKA_FLEET_TLS_CERT_FILE", "/env/cert.pem")
	t.Setenv("KAFKA_FLEET_TLS_KEY_FILE", "/env/key.pem")
	t.Setenv("KAFKA_FLEET_HEARTBEAT_TIMEOUT", "50")
	t.Setenv("KAFKA_FLEET_RETENTION_DAYS", "7")
	t.Setenv("KAFKA_FLEET_RETENTION_ENTRIES", "2000")
	t.Setenv("KAFKA_FLEET_SECRET_KEY", "env-master-key-16-bytes")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Fleet.Enabled {
		t.Fatal("KAFKA_FLEET_ENABLED was not applied")
	}
	if cfg.Fleet.Listen != "127.0.0.1:19099" {
		t.Fatalf("listen = %q", cfg.Fleet.Listen)
	}
	if cfg.Fleet.TLS.CertFile != "/env/cert.pem" {
		t.Fatalf("cert_file = %q", cfg.Fleet.TLS.CertFile)
	}
	if cfg.Fleet.TLS.KeyFile != "/env/key.pem" {
		t.Fatalf("key_file = %q", cfg.Fleet.TLS.KeyFile)
	}
	if cfg.Fleet.HeartbeatTimeout != 50 {
		t.Fatalf("heartbeat_timeout = %d", cfg.Fleet.HeartbeatTimeout)
	}
	if cfg.Fleet.RetentionDays != 7 {
		t.Fatalf("retention_days = %d", cfg.Fleet.RetentionDays)
	}
	if cfg.Fleet.RetentionEntries != 2000 {
		t.Fatalf("retention_entries = %d", cfg.Fleet.RetentionEntries)
	}
	if cfg.Fleet.SecretKey != "env-master-key-16-bytes" {
		t.Fatalf("secret_key = %q", cfg.Fleet.SecretKey)
	}
}

func TestFleetValidatePublicRequiresTLSAndAuth(t *testing.T) {

	cfg := Default()
	cfg.Fleet.Enabled = false
	cfg.Fleet.Listen = "0.0.0.0:18090"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled fleet on public should pass: %v", err)
	}

	cfg = Default()
	cfg.Fleet.Enabled = true
	cfg.Fleet.SecretKey = "valid-secret-key-16-bytes"
	cfg.Fleet.Listen = "127.0.0.1:18090"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("enabled fleet on loopback should pass: %v", err)
	}

	cfg = Default()
	cfg.Fleet.Enabled = true
	cfg.Fleet.SecretKey = "valid-secret-key-16-bytes"
	cfg.Fleet.Listen = "0.0.0.0:18090"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("public fleet listen without auth+TLS should fail")
	}
	if !strings.Contains(err.Error(), "fleet.listen") {
		t.Fatalf("expected fleet.listen error, got: %v", err)
	}

	cfg = Default()
	cfg.Fleet.Enabled = true
	cfg.Fleet.SecretKey = "valid-secret-key-16-bytes"
	cfg.Fleet.Listen = "0.0.0.0:18090"
	cfg.Security.Enabled = true
	cfg.Web.AuthSecret = "custom-auth-secret-for-test"
	cfg.Security.Users = []SecurityUser{
		{Username: "admin", Password: "strong-password-1234"},
	}
	err = cfg.Validate()
	if err == nil {
		t.Fatal("public fleet listen with security but without TLS should fail")
	}
	if !strings.Contains(err.Error(), "requires fleet.tls cert_file and key_file") {
		t.Fatalf("expected TLS error, got: %v", err)
	}

	cfg.Fleet.TLS.CertFile = "/path/to/cert"
	cfg.Fleet.TLS.KeyFile = "/path/to/key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("public fleet listen with auth+TLS should pass: %v", err)
	}

	cfg = Default()
	cfg.Fleet.Enabled = true
	cfg.Fleet.SecretKey = "valid-secret-key-16-bytes"
	cfg.Fleet.Listen = "0.0.0.0:18090"
	cfg.Security.AllowInsecurePublic = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("public fleet listen with allow_insecure_public should pass: %v", err)
	}
}

func TestFleetValidateSecretKey(t *testing.T) {
	cfg := Default()
	cfg.Fleet.Enabled = true
	t.Setenv("KAFKA_FLEET_SECRET_KEY", "")
	cfg.Fleet.SecretKey = ""
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "fleet.secret_key: must be configured") {
		t.Fatalf("expected fleet.secret_key required error, got: %v", err)
	}

	cfg.Fleet.SecretKey = "too-short"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "fleet.secret_key") {
		t.Fatalf("expected fleet.secret_key length error, got: %v", err)
	}

	cfg.Fleet.SecretKey = "valid-secret-key-16-bytes"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid secret key should pass: %v", err)
	}

	t.Setenv("KAFKA_FLEET_SECRET_KEY", "short")
	cfg.Fleet.SecretKey = ""
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "KAFKA_FLEET_SECRET_KEY") {
		t.Fatalf("expected KAFKA_FLEET_SECRET_KEY error, got: %v", err)
	}
}
