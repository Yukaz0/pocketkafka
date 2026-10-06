// Package config loads the broker configuration from a YAML file and applies
// environment variable overrides (12-factor style).
package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// configWarnf reports a configuration deprecation. It is a package variable so
// tests can capture the warning instead of polluting the test log.
var configWarnf = func(format string, args ...interface{}) {
	log.Printf("config warning: "+format, args...)
}

// Broker identifies this broker within the cluster.
type Broker struct {
	ID        int    `yaml:"id"`
	ClusterID string `yaml:"cluster_id"`
	Rack      string `yaml:"rack"`
}

// Listener is a single TCP listener definition.
type Listener struct {
	Name string
	Addr string
}

// Retention describes the data retention policy for segments.
type Retention struct {
	CheckIntervalMs int64 `yaml:"check_interval_ms"`
	RetentionHours  int   `yaml:"retention_hours"`
	RetentionBytes  int64 `yaml:"retention_bytes"`
}

// Tiered configures S3/MinIO cold offload.
type Tiered struct {
	Enabled           bool   `yaml:"enabled"`
	Endpoint          string `yaml:"endpoint"`
	Bucket            string `yaml:"bucket"`
	AccessKey         string `yaml:"access_key"`
	SecretKey         string `yaml:"secret_key"`
	Region            string `yaml:"region"`
	Prefix            string `yaml:"prefix"`
	OffloadAfterHours int    `yaml:"offload_after_hours"`
	CheckIntervalMs   int    `yaml:"check_interval_ms"`
}

// Storage configures the commit log.
type Storage struct {
	DataDir            string    `yaml:"data_dir"`
	SegmentMaxBytes    int64     `yaml:"segment_max_bytes"`
	IndexIntervalBytes int64     `yaml:"index_interval_bytes"`
	Retention          Retention `yaml:"retention"`
	Tiered             Tiered    `yaml:"tiered"`
	// FlushPolicy controls when appended records are fsync'd:
	// "none" (never, OS decides), "interval" (periodic FlushIntervalMs plus
	// shutdown), or "request" (before the produce ack is returned).
	FlushPolicy     string `yaml:"flush_policy"`
	FlushIntervalMs int    `yaml:"flush_interval_ms"`
	// SyncOnAcksAll forces a sync for acks=-1 produce requests regardless of
	// FlushPolicy, so a full-ack carries the durable-storage promise.
	SyncOnAcksAll bool `yaml:"sync_on_acks_all"`
}

// Topics configures topic auto creation.
type Topics struct {
	AutoCreate               bool `yaml:"auto_create"`
	DefaultPartitions        int  `yaml:"default_partitions"`
	DefaultReplicationFactor int  `yaml:"default_replication_factor"`
}

// Coordinator configures the consumer group coordinator.
type Coordinator struct {
	SessionTimeoutMs        int    `yaml:"session_timeout_ms"`
	RebalanceTimeoutMs      int    `yaml:"rebalance_timeout_ms"`
	HeartbeatIntervalMs     int    `yaml:"heartbeat_interval_ms"`
	OffsetsRetentionMinutes int    `yaml:"offsets_retention_minutes"`
	StorageBackend          string `yaml:"storage_backend"`
}

// Network configures the network layer.
type Network struct {
	MaxConnections      int   `yaml:"max_connections"`
	ReadBufferBytes     int   `yaml:"read_buffer_bytes"`
	WriteBufferBytes    int   `yaml:"write_buffer_bytes"`
	MaxRequestSizeBytes int64 `yaml:"max_request_size_bytes"`
	// PreAuthMaxRequestBytes caps a request frame accepted before the
	// connection has authenticated. SASL exchanges are a few hundred bytes, so
	// a small bound keeps an anonymous client from forcing a 100 MB allocation
	// per connection.
	PreAuthMaxRequestBytes int64 `yaml:"pre_auth_max_request_bytes"`
	ReadTimeoutMs          int   `yaml:"read_timeout_ms"`
	WriteTimeoutMs         int   `yaml:"write_timeout_ms"`
	IdleTimeoutMs          int   `yaml:"idle_timeout_ms"`
}

// Logging configures the logger.
type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// WebCluster is one remote broker the dashboard may monitor. It carries no
// secret: a per-cluster bearer token is stored separately in the data dir.
type WebCluster struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

// Web configures the embedded dashboard server.
type Web struct {
	Listen  string `yaml:"listen"`
	Enabled bool   `yaml:"enabled"`
	// Auth enables web UI login using the security.users credentials. The
	// cookie is HMAC-signed with the secret below.
	Auth       bool   `yaml:"auth"`
	AuthSecret string `yaml:"auth_secret"`
	// Clusters seeds the list of monitored clusters; the UI can edit the list
	// afterwards and it persists in <data_dir>/__clusters.json.
	Clusters []WebCluster `yaml:"clusters"`
	// ClusterToken is the bearer token THIS broker accepts from another
	// broker's dashboard aggregating its health report. Empty disables the
	// bearer path entirely.
	ClusterToken string `yaml:"cluster_token"`
	// SecretsKey protects stored credentials (per-cluster bearer tokens) at
	// rest. Empty uses KAFKA_SECRETS_KEY, and failing that a key file is
	// generated once inside the data dir with 0600.
	SecretsKey string `yaml:"secrets_key"`
	// TrustedProxies lists proxy addresses whose X-Forwarded-For/Proto headers
	// are believed. Only then is "the client asked over https" taken from a
	// header instead of the connection, so the session cookie can be marked
	// Secure behind a TLS-terminating reverse proxy.
	TrustedProxies []string `yaml:"trusted_proxies"`
	// AllowedHosts lists extra Host header values the dashboard accepts for the
	// WebSocket live tail. By default only IP literals and "localhost" are
	// accepted, which is what blocks DNS rebinding; a dashboard reached under a
	// name must list that name here.
	AllowedHosts []string `yaml:"allowed_hosts"`
	// SessionTTLMinutes is the absolute lifetime of a dashboard session
	// (default 1440 = 24h). 0 uses the default.
	SessionTTLMinutes int `yaml:"session_ttl_minutes"`
	// SessionIdleMinutes ends a session idle for that long (default 120).
	IdleTrimMinutes int `yaml:"session_idle_minutes"`
	// AuditLog is a file that receives the audit trail as JSON lines (0600,
	// rotated at 8 MiB). Empty keeps the trail in the process log only.
	AuditLog string `yaml:"audit_log"`
}

// DefaultSessionTTLMinutes and DefaultSessionIdleMinutes bound a dashboard
// session when the config leaves them unset.
const (
	DefaultSessionTTLMinutes  = 1440
	DefaultSessionIdleMinutes = 120
)

// SchemaRegistry configures the embedded Confluent-compatible registry.
type SchemaRegistry struct {
	Listen string `yaml:"listen"`
}

// Gateway configures the HTTP REST proxy.
type Gateway struct {
	Listen string `yaml:"listen"`
}

// MQTT configures the MQTT bridge.
type MQTT struct {
	Listen string `yaml:"listen"`
}

// SecurityUser is a SASL credential. Exactly one of Password, PasswordFile,
// PasswordHash or SCRAMVerifier supplies the secret:
//
//   - password       plaintext (legacy; also what SCRAM derives from)
//   - password_file  a file holding the plaintext secret, so it stays out of
//     the YAML
//   - password_hash  a PBKDF2 verifier usable by SASL/PLAIN and HTTP Basic,
//     but not by SCRAM (SCRAM needs the password itself)
//   - scram_verifier a stored SCRAM verifier (salt, iterations, StoredKey,
//     ServerKey), the form that never exposes the password
type SecurityUser struct {
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	PasswordFile  string `yaml:"password_file"`
	PasswordHash  string `yaml:"password_hash"`
	SCRAMVerifier string `yaml:"scram_verifier"`
}

// Security configures SASL/PLAIN authentication.
type Security struct {
	Enabled bool           `yaml:"enabled"`
	Users   []SecurityUser `yaml:"users"`
	TLS     TLS            `yaml:"tls"`
	// AllowInsecurePublic permits a listener bound to a non-loopback address
	// while security.enabled is false. It is an explicit opt-in: an open broker
	// on a routable address is refused at startup otherwise.
	AllowInsecurePublic bool `yaml:"allow_insecure_public"`
	// RequireTLSForPlain refuses SASL/PLAIN on a connection that is not
	// encrypted (MQTT included). Default true: PLAIN puts the password on the
	// wire. SCRAM is unaffected, so a SCRAM-only cluster can stay plaintext.
	RequireTLSForPlain bool `yaml:"require_tls_for_plain"`
	// AllowWildcardAdmin permits an ACL rule that grants Admin to the
	// wildcard principal. It is off by default: such a rule hands every
	// authenticated identity cluster-wide administration.
	AllowWildcardAdmin bool `yaml:"allow_wildcard_admin"`
	// SuperUsers bypass every ACL check. Without one, a fresh security-enabled
	// deployment has nobody who can administer the ACL file.
	SuperUsers []string `yaml:"super_users"`
	// SCRAMIterations is the PBKDF2 iteration count used when the broker must
	// derive a SCRAM credential from a plaintext password. 0 uses the default.
	SCRAMIterations int `yaml:"scram_iterations"`
}

// TLS configures transport encryption. One certificate serves every surface;
// the booleans select which surfaces use it.
type TLS struct {
	// Enabled starts the Kafka SSL:// listener (security.tls.listen).
	Enabled  bool   `yaml:"enabled"`
	Listen   string `yaml:"listen"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	// ClientCAFile turns on mutual TLS: a client certificate signed by this CA
	// is required. The peer's CN becomes its principal.
	ClientCAFile string `yaml:"client_ca_file"`
	// MinVersion is "1.2" (default) or "1.3".
	MinVersion string `yaml:"min_version"`
	// HTTP serves the dashboard, Schema Registry and REST proxy over TLS.
	HTTP bool `yaml:"http"`
	// MQTT serves the MQTT bridge over TLS.
	MQTT bool `yaml:"mqtt"`
}

// Config is the root configuration document.
type Config struct {
	Broker              Broker            `yaml:"broker"`
	Listeners           map[string]string `yaml:"listeners"`
	AdvertisedListeners map[string]string `yaml:"advertised_listeners"`
	Storage             Storage           `yaml:"storage"`
	Topics              Topics            `yaml:"topics"`
	Coordinator         Coordinator       `yaml:"coordinator"`
	Network             Network           `yaml:"network"`
	Logging             Logging           `yaml:"logging"`
	Web                 Web               `yaml:"web"`
	SchemaRegistry      SchemaRegistry    `yaml:"schema_registry"`
	Gateway             Gateway           `yaml:"gateway"`
	MQTT                MQTT              `yaml:"mqtt"`
	Security            Security          `yaml:"security"`
}

// Default returns a Config populated with the documented default values.
func Default() Config {
	return Config{
		Broker: Broker{ID: 0, ClusterID: "pocketkafka-local", Rack: ""},
		Listeners: map[string]string{
			"plain":    "127.0.0.1:9092",
			"internal": "127.0.0.1:29092",
		},
		AdvertisedListeners: map[string]string{
			"plain":    "localhost:9092",
			"internal": "local-kafka:29092",
		},
		Storage: Storage{
			DataDir:            "./data",
			SegmentMaxBytes:    104857600, // 100MB
			IndexIntervalBytes: 4096,      // 4KB
			Retention: Retention{
				CheckIntervalMs: 300000, // 5 minutes
				RetentionHours:  168,    // 7 days
				RetentionBytes:  -1,     // unlimited
			},
			FlushPolicy:     FlushPolicyInterval,
			FlushIntervalMs: 100,
			SyncOnAcksAll:   true,
		},
		Topics: Topics{AutoCreate: true, DefaultPartitions: 1, DefaultReplicationFactor: 1},
		Coordinator: Coordinator{
			SessionTimeoutMs:        45000,
			RebalanceTimeoutMs:      60000,
			HeartbeatIntervalMs:     3000,
			OffsetsRetentionMinutes: 10080,
			StorageBackend:          BackendFile,
		},
		Network: Network{
			MaxConnections:      10000,
			ReadBufferBytes:     65536,
			WriteBufferBytes:    65536,
			MaxRequestSizeBytes: 104857600,
			// 64 KiB: several times a SASL exchange, far below the 100 MB a
			// client may send once authenticated.
			PreAuthMaxRequestBytes: 65536,
			ReadTimeoutMs:          30000,
			WriteTimeoutMs:         30000,
			IdleTimeoutMs:          120000,
		},
		Logging:        Logging{Level: "info", Format: "json"},
		Web:            Web{Listen: "127.0.0.1:8080", Enabled: true, Auth: false, AuthSecret: "pocketkafka-web-secret"},
		SchemaRegistry: SchemaRegistry{Listen: "127.0.0.1:8081"},
		Gateway:        Gateway{Listen: "127.0.0.1:8082"},
		MQTT:           MQTT{Listen: "127.0.0.1:1883"},
		Security: Security{
			RequireTLSForPlain: true,
			TLS:                TLS{Listen: "127.0.0.1:9093"},
		},
	}
}

// Storage backend names. Only BackendFile and BackendInMemory are real
// implementations; BackendPebbleLegacy is accepted as an alias for BackendFile
// so existing configuration keeps working (see Normalize).
const (
	BackendFile         = "file"
	BackendInMemory     = "inmemory"
	BackendPebbleLegacy = "pebble"
)

// Segment flush policies (Storage.FlushPolicy).
const (
	FlushPolicyNone     = "none"
	FlushPolicyInterval = "interval"
	FlushPolicyRequest  = "request"
)

// Load reads the YAML file at path (if it exists), overlays it on top of the
// defaults, then applies environment variable overrides. The result is
// normalized and validated before it is returned, so a caller that gets a nil
// error can start listeners safely.
func Load(path string) (Config, error) {
	cfg := Default()
	if path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			m, perr := parseYAML(data)
			if perr != nil {
				return cfg, fmt.Errorf("parse config %s: %w", path, perr)
			}
			applyYAML(&cfg, m)
		} else if !os.IsNotExist(err) {
			return cfg, fmt.Errorf("read config %s: %w", path, err)
		}
	}
	applyEnv(&cfg)
	if err := cfg.resolvePasswordFiles(); err != nil {
		return cfg, err
	}
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// resolvePasswordFiles reads security.users[].password_file into the plaintext
// password field, so an operator can keep the secret out of the YAML (and out
// of the container image). A file that cannot be read is a startup error: a
// silently empty password would turn into "no credential" instead of "no
// access".
func (c *Config) resolvePasswordFiles() error {
	for i := range c.Security.Users {
		u := &c.Security.Users[i]
		if u.PasswordFile == "" {
			continue
		}
		if u.Password != "" {
			return fmt.Errorf("security.users[%d] (%s): set either password or password_file, not both", i, u.Username)
		}
		data, err := os.ReadFile(u.PasswordFile)
		if err != nil {
			return fmt.Errorf("security.users[%d] (%s): read password_file: %w", i, u.Username, err)
		}
		u.Password = strings.TrimRight(string(data), "\r\n")
		if u.Password == "" {
			return fmt.Errorf("security.users[%d] (%s): password_file %s is empty", i, u.Username, u.PasswordFile)
		}
	}
	return nil
}

// Normalize rewrites legacy or shorthand values into their canonical form so
// downstream code only ever sees the canonical spelling. Legacy "pebble" is an
// alias for the file-backed backend and triggers a one-time warning.
func (c *Config) Normalize() {
	if c.Coordinator.StorageBackend == BackendPebbleLegacy {
		configWarnf("coordinator.storage_backend=%q is a legacy alias for %q (the offset store has always been a gob file, never Pebble)", BackendPebbleLegacy, BackendFile)
		c.Coordinator.StorageBackend = BackendFile
	}
}

func applyEnv(cfg *Config) {
	setInt(&cfg.Broker.ID, os.Getenv("KAFKA_BROKER_ID"))
	if v := os.Getenv("KAFKA_LISTENERS"); v != "" {
		cfg.Listeners = parseListeners(v)
	}
	if v := os.Getenv("KAFKA_ADVERTISED_LISTENERS"); v != "" {
		cfg.AdvertisedListeners = parseListeners(v)
	}
	setStr(&cfg.Storage.DataDir, os.Getenv("KAFKA_DATA_DIR"))
	setInt64(&cfg.Storage.SegmentMaxBytes, os.Getenv("KAFKA_LOG_SEGMENT_BYTES"))
	setInt(&cfg.Storage.Retention.RetentionHours, os.Getenv("KAFKA_LOG_RETENTION_HOURS"))
	setBool(&cfg.Topics.AutoCreate, os.Getenv("KAFKA_AUTO_CREATE_TOPICS"))
	setStr(&cfg.Logging.Level, os.Getenv("KAFKA_LOG_LEVEL"))

	// Management surfaces bind separately from the Kafka listeners, so an
	// operator can expose the dashboard without exposing the broker.
	setStr(&cfg.Web.Listen, os.Getenv("KAFKA_WEB_LISTEN"))
	setStr(&cfg.SchemaRegistry.Listen, os.Getenv("KAFKA_SCHEMA_REGISTRY_LISTEN"))
	setStr(&cfg.Gateway.Listen, os.Getenv("KAFKA_GATEWAY_LISTEN"))
	setStr(&cfg.MQTT.Listen, os.Getenv("KAFKA_MQTT_LISTEN"))

	setBool(&cfg.Web.Enabled, os.Getenv("KAFKA_ENABLE_WEB_UI"))
	setBool(&cfg.Web.Auth, os.Getenv("KAFKA_WEB_AUTH"))
	setStr(&cfg.Web.AuthSecret, os.Getenv("KAFKA_WEB_AUTH_SECRET"))
	setStr(&cfg.Web.ClusterToken, os.Getenv("KAFKA_CLUSTER_TOKEN"))
	setStr(&cfg.Web.AuditLog, os.Getenv("KAFKA_AUDIT_LOG"))

	setBool(&cfg.Security.Enabled, os.Getenv("KAFKA_SECURITY_ENABLED"))
	setBool(&cfg.Security.AllowInsecurePublic, os.Getenv("KAFKA_ALLOW_INSECURE_PUBLIC"))
	setBool(&cfg.Security.RequireTLSForPlain, os.Getenv("KAFKA_REQUIRE_TLS_FOR_PLAIN"))
	setInt(&cfg.Security.SCRAMIterations, os.Getenv("KAFKA_SCRAM_ITERATIONS"))
	setBool(&cfg.Security.AllowWildcardAdmin, os.Getenv("KAFKA_ALLOW_WILDCARD_ADMIN"))
	setList(&cfg.Security.SuperUsers, os.Getenv("KAFKA_SUPER_USERS"))

	setBool(&cfg.Security.TLS.Enabled, os.Getenv("KAFKA_TLS_ENABLED"))
	setStr(&cfg.Security.TLS.Listen, os.Getenv("KAFKA_TLS_LISTEN"))
	setStr(&cfg.Security.TLS.CertFile, os.Getenv("KAFKA_TLS_CERT_FILE"))
	setStr(&cfg.Security.TLS.KeyFile, os.Getenv("KAFKA_TLS_KEY_FILE"))
	setStr(&cfg.Security.TLS.ClientCAFile, os.Getenv("KAFKA_TLS_CLIENT_CA_FILE"))
	setStr(&cfg.Security.TLS.MinVersion, os.Getenv("KAFKA_TLS_MIN_VERSION"))
	setBool(&cfg.Security.TLS.HTTP, os.Getenv("KAFKA_TLS_HTTP"))
	setBool(&cfg.Security.TLS.MQTT, os.Getenv("KAFKA_TLS_MQTT"))
}

// setList splits a comma separated environment value into dst.
func setList(dst *[]string, v string) {
	if strings.TrimSpace(v) == "" {
		return
	}
	*dst = splitList(v)
}

// splitList splits a comma separated value, dropping empty entries.
func splitList(v string) []string {
	out := make([]string, 0, 4)
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// parseListeners parses a comma separated list of NAME://host:port pairs.
func parseListeners(v string) map[string]string {
	out := make(map[string]string)
	for _, part := range strings.Split(v, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, addr, ok := strings.Cut(part, "://")
		if !ok {
			continue
		}
		out[name] = addr
	}
	return out
}

func setInt(dst *int, v string) {
	if v == "" {
		return
	}
	if n, err := strconv.Atoi(v); err == nil {
		*dst = n
	}
}

func setInt64(dst *int64, v string) {
	if v == "" {
		return
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		*dst = n
	}
}

func setBool(dst *bool, v string) {
	if v == "" {
		return
	}
	if b, err := strconv.ParseBool(v); err == nil {
		*dst = b
	}
}

func setStr(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}
