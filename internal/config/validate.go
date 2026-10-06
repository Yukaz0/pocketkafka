package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
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

// weakPasswords are the values that show up in every sample config and wordlist.
// A security-enabled broker refuses to start on one of them: online guessing
// against a known default is not a hypothetical risk.
var weakPasswords = map[string]bool{
	"changeme":                true,
	"change-me-in-production": true,
	"password":                true,
	"passw0rd":                true,
	"admin":                   true,
	"administrator":           true,
	"root":                    true,
	"secret":                  true,
	"minioadmin":              true,
	"devpassword":             true,
	"pocketkafka":             true,
	"123456":                  true,
	"12345678":                true,
	"qwerty":                  true,
	"letmein":                 true,
}

// MinPasswordLength is the shortest plaintext password a security-enabled
// broker accepts. It applies to config-supplied passwords; verifiers carry
// their own stronger material.
const MinPasswordLength = 8

// IsLoopbackHost reports whether a bind host only accepts local connections.
// An empty host, "0.0.0.0", "::" and any non-loopback name all mean "reachable
// from the network", which is the conservative reading.
func IsLoopbackHost(host string) bool {
	host = strings.TrimSpace(strings.Trim(strings.TrimSpace(host), "[]"))
	switch strings.ToLower(host) {
	case "":
		return false
	case "localhost":
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// bindTargets lists every address the broker would listen on, labelled with the
// config field that produced it.
func bindTargets(c *Config) []bindTarget {
	out := []bindTarget{}
	for name, addr := range c.Listeners {
		out = append(out, bindTarget{"listeners." + name, addr})
	}
	if c.Web.Enabled {
		out = append(out, bindTarget{"web.listen", c.Web.Listen})
	}
	out = append(out, bindTarget{"schema_registry.listen", c.SchemaRegistry.Listen})
	out = append(out, bindTarget{"gateway.listen", c.Gateway.Listen})
	out = append(out, bindTarget{"mqtt.listen", c.MQTT.Listen})
	if c.Security.TLS.Enabled || c.Security.TLS.HTTP || c.Security.TLS.MQTT {
		out = append(out, bindTarget{"security.tls.listen", c.Security.TLS.Listen})
	}
	return out
}

// bindTarget is one configured listen address and the field naming it.
type bindTarget struct {
	field string
	addr  string
}

// ValidateClusterName checks a monitored-cluster name. The charset is
// deliberately narrow: a name ends up in URLs, in log lines, and as an argument
// inside a generated onclick attribute, so allowing punctuation would only
// create escaping problems for no benefit.
func ValidateClusterName(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("must not be empty")
	}
	if len(name) > 64 {
		return errors.New("too long (max 64)")
	}
	for _, r := range name {
		ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') ||
			r == '.' || r == '_' || r == '-'
		if !ok {
			return errors.New("may contain only letters, digits, '.', '_' and '-'")
		}
	}
	return nil
}

// ValidateClusterURL checks a monitored-cluster endpoint: plain http(s) with a
// host and no embedded credentials. Only URLs that pass this ever get fetched,
// which is what keeps the dashboard fan-out from being an SSRF primitive.
func ValidateClusterURL(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("is not valid: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https (got %q)", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("must include a host")
	}
	if u.User != nil {
		return errors.New("must not embed credentials; use the token field instead")
	}
	return nil
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
	validateTiered(c, add)
	validateExposure(c, add)
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
	if c.Network.PreAuthMaxRequestBytes <= 0 {
		add("network.pre_auth_max_request_bytes: must be > 0 (got %d)", c.Network.PreAuthMaxRequestBytes)
	}
	if c.Network.PreAuthMaxRequestBytes > c.Network.MaxRequestSizeBytes {
		add("network.pre_auth_max_request_bytes: must not exceed max_request_size_bytes (%d > %d)",
			c.Network.PreAuthMaxRequestBytes, c.Network.MaxRequestSizeBytes)
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

// validateExposure refuses a broker that is both unauthenticated and reachable
// from the network. Every ingress (Kafka, dashboard, Schema Registry, REST
// proxy, MQTT) is default-deny only when security is enabled, so a non-loopback
// bind with security off means anyone who can route to this host can truncate
// topics and edit ACLs.
func validateExposure(c *Config, add func(string, ...interface{})) {
	if c.Security.Enabled || c.Security.AllowInsecurePublic {
		return
	}
	for _, t := range bindTargets(c) {
		host, _, err := net.SplitHostPort(strings.TrimSpace(t.addr))
		if err != nil {
			continue // shape errors are reported by validateListeners
		}
		if IsLoopbackHost(host) {
			continue
		}
		add("%s: binds %s (not loopback) while security.enabled is false; "+
			"set security.enabled=true, or set security.allow_insecure_public=true to accept "+
			"an open broker on a routable address", t.field, t.addr)
	}
}

// validateTiered rejects a cold-storage configuration that would ship objects
// with a well-known credential over a plaintext link to a remote host.
func validateTiered(c *Config, add func(string, ...interface{})) {
	if !c.Storage.Tiered.Enabled {
		return
	}
	if weakPasswords[strings.ToLower(c.Storage.Tiered.SecretKey)] || c.Storage.Tiered.SecretKey == "" {
		add("storage.tiered.secret_key: must be set to a non-default value while tiering is enabled")
	}
	if strings.TrimSpace(c.Storage.Tiered.AccessKey) == "" {
		add("storage.tiered.access_key: required while tiering is enabled")
	}
	u, err := url.Parse(c.Storage.Tiered.Endpoint)
	if err != nil || u.Host == "" {
		add("storage.tiered.endpoint: must be a URL with a host")
		return
	}
	if u.Scheme == "http" && !IsLoopbackHost(u.Hostname()) {
		add("storage.tiered.endpoint: %s sends objects and credentials in cleartext; use https (http is only accepted for loopback)", c.Storage.Tiered.Endpoint)
	}
}

// unresolvedEnv reports a ${NAME} reference that environment expansion could
// not resolve. It mirrors expandEnv: a "${" with no closing "}" is literal
// text, not a reference, and must not be rejected.
func unresolvedEnv(v string) bool {
	i := strings.Index(v, "${")
	if i < 0 {
		return false
	}
	return strings.Contains(v[i+2:], "}")
}

func validateSecurity(c *Config, add func(string, ...interface{})) {
	// Enabling security also turns on web login (the dashboard must not be an
	// unauthenticated bypass), so the signing secret must be non-default in
	// either case.
	if c.Web.Enabled && (c.Web.Auth || c.Security.Enabled) {
		if defaultWebSecrets[c.Web.AuthSecret] {
			add("web.auth_secret: must be set to a non-default value when web.auth or security.enabled is true")
		}
		if unresolvedEnv(c.Web.AuthSecret) {
			// The literal "${NAME}" is not an empty secret: it is a *predictable*
			// one, and a predictable signing key forges sessions.
			add("web.auth_secret: %q references an unset environment variable", c.Web.AuthSecret)
		}
	}
	// Same reasoning for the two other shared secrets: a literal ${NAME} that was
	// never substituted is guessable, whether or not it is "non-default".
	for _, secret := range []struct{ field, value string }{
		{"web.cluster_token", c.Web.ClusterToken},
		{"web.secrets_key", c.Web.SecretsKey},
	} {
		if unresolvedEnv(secret.value) {
			add("%s: %q references an unset environment variable", secret.field, secret.value)
		}
	}
	// Multi-cluster monitoring: a malformed entry would be persisted and then
	// fail on every fan-out, so reject it at startup instead.
	seenClusters := map[string]bool{}
	for i, cl := range c.Web.Clusters {
		name := strings.TrimSpace(cl.Name)
		if err := ValidateClusterName(name); err != nil {
			add("web.clusters[%d].name: %v", i, err)
			continue
		}
		if seenClusters[name] {
			add("web.clusters[%d].name: duplicate cluster name %q", i, name)
		}
		seenClusters[name] = true
		if err := ValidateClusterURL(cl.URL); err != nil {
			add("web.clusters[%d].url: %v", i, err)
		}
	}
	// The bearer token this broker accepts from an aggregator: short values are
	// guessable, so refuse them rather than shipping a weak shared secret.
	if c.Web.ClusterToken != "" && len(c.Web.ClusterToken) < 16 {
		add("web.cluster_token: must be at least 16 characters when set")
	}
	// The key that seals stored credentials at rest. A short key protects
	// nothing; an unset key is fine because one is generated in the data dir.
	// The environment variable is checked too: it is the effective key whenever
	// it is set, and validating only the config value would leave the shorter
	// path open. (The generated key file is 32 random bytes, checked where it is
	// read.)
	if c.Web.SecretsKey != "" && len(c.Web.SecretsKey) < 16 {
		add("web.secrets_key: must be at least 16 characters when set")
	}
	if env := os.Getenv("KAFKA_SECRETS_KEY"); env != "" && len(env) < 16 {
		add("KAFKA_SECRETS_KEY: must be at least 16 characters when set (it seals stored cluster credentials)")
	}
	if c.Security.Enabled {
		useful := 0
		for i, u := range c.Security.Users {
			if strings.TrimSpace(u.Username) != "" {
				useful++
			}
			validateUserSecret(i, u, add)
		}
		if useful == 0 {
			add("security.users: at least one user with a username is required when security.enabled is true")
		}
		// A super user that is not also a configured user can never present a
		// credential, so the entry would silently grant nothing.
		known := map[string]bool{}
		for _, u := range c.Security.Users {
			known[strings.TrimSpace(u.Username)] = true
		}
		for _, su := range c.Security.SuperUsers {
			if !known[strings.TrimSpace(su)] {
				add("security.super_users: %q is not listed in security.users", su)
			}
		}
	}
	if c.Security.SCRAMIterations < 0 {
		add("security.scram_iterations: must be >= 0 (got %d)", c.Security.SCRAMIterations)
	}
	validateTLS(c, add)
}

// validateUserSecret checks one user's credential source: exactly one method,
// and a plaintext password that is neither a known default nor trivially short.
func validateUserSecret(i int, u SecurityUser, add func(string, ...interface{})) {
	where := fmt.Sprintf("security.users[%d]", i)
	if name := strings.TrimSpace(u.Username); name != "" {
		where += " (" + name + ")"
	}
	methods := 0
	for _, set := range []bool{u.Password != "", u.PasswordHash != "", u.SCRAMVerifier != ""} {
		if set {
			methods++
		}
	}
	switch {
	case methods == 0:
		add("%s: needs one of password, password_hash or scram_verifier", where)
		return
	case methods > 1:
		add("%s: set exactly one of password, password_hash or scram_verifier", where)
		return
	}
	for _, v := range []string{u.Password, u.PasswordHash, u.SCRAMVerifier, u.PasswordFile} {
		if unresolvedEnv(v) {
			add("%s: %q references an unset environment variable", where, v)
		}
	}
	if u.Password != "" {
		if weakPasswords[strings.ToLower(u.Password)] || len(u.Password) < MinPasswordLength {
			add("%s: password is a default/too-short value (min %d characters, no well-known defaults)", where, MinPasswordLength)
		}
	}
	if strings.TrimSpace(u.PasswordFile) != "" && u.Password != "" {
		add("%s: set either password or password_file, not both", where)
	}
}

// validateTLS checks the shared certificate and the surfaces switched over to
// it.
func validateTLS(c *Config, add func(string, ...interface{})) {
	t := c.Security.TLS
	active := t.Enabled || t.HTTP || t.MQTT
	if !active {
		return
	}
	if strings.TrimSpace(t.CertFile) == "" {
		add("security.tls.cert_file: required when any TLS surface is enabled")
	}
	if strings.TrimSpace(t.KeyFile) == "" {
		add("security.tls.key_file: required when any TLS surface is enabled")
	}
	if strings.TrimSpace(t.Listen) == "" {
		add("security.tls.listen: required when security.tls.enabled is true")
	}
	for _, v := range []string{t.CertFile, t.KeyFile, t.ClientCAFile} {
		if unresolvedEnv(v) {
			add("security.tls: %q references an unset environment variable", v)
		}
	}
	switch t.MinVersion {
	case "", "1.2", "1.3":
	default:
		add("security.tls.min_version: want %q or %q (got %q)", "1.2", "1.3", t.MinVersion)
	}
	if t.Enabled && !c.Security.Enabled {
		add("security.tls.enabled: a TLS listener without security.enabled still accepts anonymous clients; enable security")
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
