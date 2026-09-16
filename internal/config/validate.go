package config

import (
	"fmt"
	"net"
	"strings"
)

// defaultWebSecrets are placeholder secrets shipped in the defaults and sample
// config. Enabling web auth while one of these is still in place is treated as
// a misconfiguration: it would publish a publicly-known signing key.
var defaultWebSecrets = map[string]bool{
	"":                        true,
	"pocketkafka-web-secret":  true,
	"change-me-in-production": true,
	"changeme":                true,
}

// Validate checks the configuration for values that would make the broker
// unsafe or non-functional at runtime. It reports every problem it finds,
// naming the config field, so startup can fail before any listener is opened.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...interface{}) {
		problems = append(problems, fmt.Sprintf(format, args...))
	}

	validateListeners(c, add)
	validateNetwork(c, add)
	validateStorage(c, add)
	validateSecurity(c, add)

	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %s", strings.Join(problems, "; "))
}

func validateListeners(c *Config, add func(string, ...interface{})) {
	if len(c.Listeners) == 0 {
		add("listeners: at least one listener is required")
	}
	seen := map[string]string{}
	for name, addr := range c.Listeners {
		if strings.TrimSpace(addr) == "" {
			add("listeners.%s: address must not be empty", name)
			continue
		}
		if !validHostPort(addr) {
			add("listeners.%s: %q is not a valid host:port address", name, addr)
			continue
		}
		if prev, dup := seen[addr]; dup {
			add("listeners.%s: duplicate address %q (also used by listener %q)", name, addr, prev)
			continue
		}
		seen[addr] = name
	}
	for name, addr := range c.AdvertisedListeners {
		if strings.TrimSpace(addr) == "" {
			add("advertised_listeners.%s: address must not be empty", name)
			continue
		}
		if !validHostPort(addr) {
			add("advertised_listeners.%s: %q is not a valid host:port address", name, addr)
		}
	}
}

func validateNetwork(c *Config, add func(string, ...interface{})) {
	if c.Network.MaxConnections <= 0 {
		add("network.max_connections: must be > 0 (got %d)", c.Network.MaxConnections)
	}
	if c.Network.ReadBufferBytes < 0 {
		add("network.read_buffer_bytes: must be >= 0 (got %d)", c.Network.ReadBufferBytes)
	}
	if c.Network.WriteBufferBytes < 0 {
		add("network.write_buffer_bytes: must be >= 0 (got %d)", c.Network.WriteBufferBytes)
	}
	if c.Network.MaxRequestSizeBytes <= 0 {
		add("network.max_request_size_bytes: must be > 0 (got %d)", c.Network.MaxRequestSizeBytes)
	}
	if c.Network.ReadTimeoutMs < 0 {
		add("network.read_timeout_ms: must be >= 0 (got %d)", c.Network.ReadTimeoutMs)
	}
	if c.Network.WriteTimeoutMs < 0 {
		add("network.write_timeout_ms: must be >= 0 (got %d)", c.Network.WriteTimeoutMs)
	}
	if c.Network.IdleTimeoutMs < 0 {
		add("network.idle_timeout_ms: must be >= 0 (got %d)", c.Network.IdleTimeoutMs)
	}
}

func validateStorage(c *Config, add func(string, ...interface{})) {
	switch c.Coordinator.StorageBackend {
	case BackendFile, BackendInMemory, BackendPebbleLegacy:
	default:
		add("coordinator.storage_backend: unknown backend %q (want %q or %q)",
			c.Coordinator.StorageBackend, BackendFile, BackendInMemory)
	}
	switch c.Storage.FlushPolicy {
	case FlushPolicyNone, FlushPolicyInterval, FlushPolicyRequest:
	default:
		add("storage.flush_policy: unknown policy %q (want %q, %q, or %q)",
			c.Storage.FlushPolicy, FlushPolicyNone, FlushPolicyInterval, FlushPolicyRequest)
	}
	if c.Storage.FlushPolicy == FlushPolicyInterval && c.Storage.FlushIntervalMs <= 0 {
		add("storage.flush_interval_ms: must be > 0 when flush_policy=%q (got %d)",
			FlushPolicyInterval, c.Storage.FlushIntervalMs)
	}
	if c.Storage.SegmentMaxBytes <= 0 {
		add("storage.segment_max_bytes: must be > 0 (got %d)", c.Storage.SegmentMaxBytes)
	}
}

func validateSecurity(c *Config, add func(string, ...interface{})) {
	// Enabling security also turns on web login (the dashboard must not be an
	// unauthenticated bypass), so the signing secret must be non-default in
	// either case.
	if c.Web.Enabled && (c.Web.Auth || c.Security.Enabled) {
		if defaultWebSecrets[c.Web.AuthSecret] {
			add("web.auth_secret: must be set to a non-default value when web.auth or security.enabled is true")
		}
	}
	if c.Security.Enabled {
		useful := 0
		for _, u := range c.Security.Users {
			if strings.TrimSpace(u.Username) != "" {
				useful++
			}
		}
		if useful == 0 {
			add("security.users: at least one user with a username is required when security.enabled is true")
		}
	}
	if c.Security.TLS.Enabled {
		if c.Security.TLS.CertFile == "" {
			add("security.tls.cert_file: required when security.tls.enabled is true")
		}
		if c.Security.TLS.KeyFile == "" {
			add("security.tls.key_file: required when security.tls.enabled is true")
		}
		if strings.TrimSpace(c.Security.TLS.Listen) == "" {
			add("security.tls.listen: required when security.tls.enabled is true")
		}
	}
}

// validHostPort reports whether addr parses as host:port with a numeric or
// service-name port.
func validHostPort(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return strings.TrimSpace(port) != ""
}
