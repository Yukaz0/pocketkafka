package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

var configWarnf = func(format string, args ...interface{}) {
	log.Printf("config warning: "+format, args...)
}

type Broker struct {
	ID        int    `yaml:"id"`
	ClusterID string `yaml:"cluster_id"`
	Rack      string `yaml:"rack"`
}

type Listener struct {
	Name string
	Addr string
}

type Retention struct {
	CheckIntervalMs int64 `yaml:"check_interval_ms"`
	RetentionHours  int   `yaml:"retention_hours"`
	RetentionBytes  int64 `yaml:"retention_bytes"`
}

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

type Storage struct {
	DataDir            string    `yaml:"data_dir"`
	SegmentMaxBytes    int64     `yaml:"segment_max_bytes"`
	IndexIntervalBytes int64     `yaml:"index_interval_bytes"`
	Retention          Retention `yaml:"retention"`
	Tiered             Tiered    `yaml:"tiered"`

	FlushPolicy     string `yaml:"flush_policy"`
	FlushIntervalMs int    `yaml:"flush_interval_ms"`

	SyncOnAcksAll bool `yaml:"sync_on_acks_all"`
}

type Topics struct {
	AutoCreate               bool `yaml:"auto_create"`
	DefaultPartitions        int  `yaml:"default_partitions"`
	DefaultReplicationFactor int  `yaml:"default_replication_factor"`
}

type Coordinator struct {
	SessionTimeoutMs        int    `yaml:"session_timeout_ms"`
	RebalanceTimeoutMs      int    `yaml:"rebalance_timeout_ms"`
	HeartbeatIntervalMs     int    `yaml:"heartbeat_interval_ms"`
	OffsetsRetentionMinutes int    `yaml:"offsets_retention_minutes"`
	StorageBackend          string `yaml:"storage_backend"`
}

type Network struct {
	MaxConnections      int   `yaml:"max_connections"`
	ReadBufferBytes     int   `yaml:"read_buffer_bytes"`
	WriteBufferBytes    int   `yaml:"write_buffer_bytes"`
	MaxRequestSizeBytes int64 `yaml:"max_request_size_bytes"`

	PreAuthMaxRequestBytes int64 `yaml:"pre_auth_max_request_bytes"`
	ReadTimeoutMs          int   `yaml:"read_timeout_ms"`
	WriteTimeoutMs         int   `yaml:"write_timeout_ms"`
	IdleTimeoutMs          int   `yaml:"idle_timeout_ms"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type WebCluster struct {
	Name string `yaml:"name"`
	URL  string `yaml:"url"`
}

type Web struct {
	Listen  string `yaml:"listen"`
	Enabled bool   `yaml:"enabled"`

	Auth       bool   `yaml:"auth"`
	AuthSecret string `yaml:"auth_secret"`

	Clusters []WebCluster `yaml:"clusters"`

	ClusterToken string `yaml:"cluster_token"`

	SecretsKey string `yaml:"secrets_key"`

	TrustedProxies []string `yaml:"trusted_proxies"`

	AllowedHosts []string `yaml:"allowed_hosts"`

	SessionTTLMinutes int `yaml:"session_ttl_minutes"`

	IdleTrimMinutes int `yaml:"session_idle_minutes"`

	AuditLog string `yaml:"audit_log"`
}

const (
	DefaultSessionTTLMinutes  = 1440
	DefaultSessionIdleMinutes = 120
)

type SchemaRegistry struct {
	Listen string `yaml:"listen"`
}

type Gateway struct {
	Listen string `yaml:"listen"`
}

type MQTT struct {
	Listen string `yaml:"listen"`
}

type SecurityUser struct {
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	PasswordFile  string `yaml:"password_file"`
	PasswordHash  string `yaml:"password_hash"`
	SCRAMVerifier string `yaml:"scram_verifier"`
}

type Security struct {
	Enabled bool           `yaml:"enabled"`
	Users   []SecurityUser `yaml:"users"`
	TLS     TLS            `yaml:"tls"`

	AllowInsecurePublic bool `yaml:"allow_insecure_public"`

	RequireTLSForPlain bool `yaml:"require_tls_for_plain"`

	AllowWildcardAdmin bool `yaml:"allow_wildcard_admin"`

	SuperUsers []string `yaml:"super_users"`

	SCRAMIterations int `yaml:"scram_iterations"`
}

type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	Listen   string `yaml:"listen"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	ClientCAFile string `yaml:"client_ca_file"`

	MinVersion string `yaml:"min_version"`

	HTTP bool `yaml:"http"`

	MQTT bool `yaml:"mqtt"`
}

type FleetTLS struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`

	CAFile string `yaml:"ca_file"`

	ServerName string `yaml:"server_name"`
}

type Fleet struct {
	Enabled          bool     `yaml:"enabled"`
	Listen           string   `yaml:"listen"`
	TLS              FleetTLS `yaml:"tls"`
	SecretKey        string   `yaml:"secret_key"`
	HeartbeatTimeout int      `yaml:"heartbeat_timeout"`
	RetentionDays    int      `yaml:"retention_days"`
	RetentionEntries int      `yaml:"retention_entries"`
}

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
	Fleet               Fleet             `yaml:"fleet"`
}

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
			SegmentMaxBytes:    104857600,
			IndexIntervalBytes: 4096,
			Retention: Retention{
				CheckIntervalMs: 300000,
				RetentionHours:  168,
				RetentionBytes:  -1,
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
		Fleet: Fleet{
			Enabled:          false,
			Listen:           "127.0.0.1:18090",
			HeartbeatTimeout: 30,
			RetentionDays:    30,
			RetentionEntries: 10000,
		},
	}
}

const (
	BackendFile         = "file"
	BackendInMemory     = "inmemory"
	BackendPebbleLegacy = "pebble"
)

const (
	FlushPolicyNone     = "none"
	FlushPolicyInterval = "interval"
	FlushPolicyRequest  = "request"
)

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
	setBool(&cfg.Fleet.Enabled, os.Getenv("KAFKA_FLEET_ENABLED"))
	setStr(&cfg.Fleet.Listen, os.Getenv("KAFKA_FLEET_LISTEN"))
	setStr(&cfg.Fleet.TLS.CertFile, os.Getenv("KAFKA_FLEET_TLS_CERT_FILE"))
	setStr(&cfg.Fleet.TLS.KeyFile, os.Getenv("KAFKA_FLEET_TLS_KEY_FILE"))
	setStr(&cfg.Fleet.TLS.CAFile, os.Getenv("KAFKA_FLEET_TLS_CA_FILE"))
	setStr(&cfg.Fleet.TLS.ServerName, os.Getenv("KAFKA_FLEET_TLS_SERVER_NAME"))
	setStr(&cfg.Fleet.SecretKey, os.Getenv("KAFKA_FLEET_SECRET_KEY"))
	setInt(&cfg.Fleet.HeartbeatTimeout, os.Getenv("KAFKA_FLEET_HEARTBEAT_TIMEOUT"))
	setInt(&cfg.Fleet.RetentionDays, os.Getenv("KAFKA_FLEET_RETENTION_DAYS"))
	setInt(&cfg.Fleet.RetentionEntries, os.Getenv("KAFKA_FLEET_RETENTION_ENTRIES"))
}

func setList(dst *[]string, v string) {
	if strings.TrimSpace(v) == "" {
		return
	}
	*dst = splitList(v)
}

func splitList(v string) []string {
	out := make([]string, 0, 4)
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

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
