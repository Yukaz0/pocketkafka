package config

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestEnvOverridesDriveTheComposeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  enabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("KAFKA_LISTENERS", "plain://0.0.0.0:9092,internal://0.0.0.0:29092")
	t.Setenv("KAFKA_ADVERTISED_LISTENERS", "plain://localhost:9092,internal://broker:29092")
	t.Setenv("KAFKA_WEB_LISTEN", "0.0.0.0:8080")
	t.Setenv("KAFKA_SCHEMA_REGISTRY_LISTEN", "0.0.0.0:8081")
	t.Setenv("KAFKA_GATEWAY_LISTEN", "0.0.0.0:8082")
	t.Setenv("KAFKA_MQTT_LISTEN", "0.0.0.0:1883")
	t.Setenv("KAFKA_ALLOW_INSECURE_PUBLIC", "true")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Listeners["plain"] != "0.0.0.0:9092" || cfg.Listeners["internal"] != "0.0.0.0:29092" {
		t.Fatalf("listeners = %v", cfg.Listeners)
	}
	if cfg.Web.Listen != "0.0.0.0:8080" || cfg.SchemaRegistry.Listen != "0.0.0.0:8081" ||
		cfg.Gateway.Listen != "0.0.0.0:8082" || cfg.MQTT.Listen != "0.0.0.0:1883" {
		t.Fatalf("management listeners = %q %q %q %q",
			cfg.Web.Listen, cfg.SchemaRegistry.Listen, cfg.Gateway.Listen, cfg.MQTT.Listen)
	}
	if !cfg.Security.AllowInsecurePublic {
		t.Fatal("KAFKA_ALLOW_INSECURE_PUBLIC was ignored, so an open broker on 0.0.0.0 was accepted silently")
	}
}

func TestEnvCanDisableTheDashboardAndBeatsYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("web:\n  enabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KAFKA_ENABLE_WEB_UI", "false")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Web.Enabled {
		t.Fatal("KAFKA_ENABLE_WEB_UI=false did not disable the dashboard")
	}
}

func TestLoopbackBindsNeedNoOptIn(t *testing.T) {

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load with defaults: %v", err)
	}
	for name, addr := range cfg.Listeners {
		if !IsLoopbackHost(mustHost(t, addr)) {
			t.Fatalf("default listener %s = %s is not loopback", name, addr)
		}
	}
	if cfg.Security.AllowInsecurePublic {
		t.Fatal("the insecure-public opt-in must be off by default")
	}
}

func mustHost(t *testing.T, addr string) string {
	t.Helper()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("bad host:port %q: %v", addr, err)
	}
	return host
}
