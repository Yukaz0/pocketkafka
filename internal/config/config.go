// Package config loads the broker configuration from a YAML file and applies
// environment variable overrides (12-factor style) as described in
// docs/CONFIG_SPEC.md.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
}

// Logging configures the logger.
type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Web configures the embedded dashboard server.
type Web struct {
	Listen string `yaml:"listen"`
}

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

// SecurityUser is a SASL/PLAIN credential.
type SecurityUser struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// Security configures SASL/PLAIN authentication.
type Security struct {
	Enabled bool           `yaml:"enabled"`
	Users   []SecurityUser `yaml:"users"`
	TLS     TLS            `yaml:"tls"`
}

// TLS configures an optional TLS (SSL) listener.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	Listen   string `yaml:"listen"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
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
		Broker: Broker{ID: 0, ClusterID: "go-kafka-neu-local", Rack: ""},
		Listeners: map[string]string{
			"plain":    "0.0.0.0:9092",
			"internal": "0.0.0.0:29092",
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
		},
		Topics: Topics{AutoCreate: true, DefaultPartitions: 1, DefaultReplicationFactor: 1},
		Coordinator: Coordinator{
			SessionTimeoutMs:        45000,
			RebalanceTimeoutMs:      60000,
			HeartbeatIntervalMs:     3000,
			OffsetsRetentionMinutes: 10080,
			StorageBackend:          "pebble",
		},
		Network: Network{
			MaxConnections:      10000,
			ReadBufferBytes:     65536,
			WriteBufferBytes:    65536,
			MaxRequestSizeBytes: 104857600,
		},
		Logging:        Logging{Level: "info", Format: "json"},
		Web:            Web{Listen: "0.0.0.0:8080"},
		SchemaRegistry: SchemaRegistry{Listen: "0.0.0.0:8081"},
		Gateway:        Gateway{Listen: "0.0.0.0:8082"},
		MQTT:           MQTT{Listen: "0.0.0.0:1883"},
		Security:       Security{TLS: TLS{Listen: "0.0.0.0:9093"}},
	}
}

// Load reads the YAML file at path (if it exists), overlays it on top of the
// defaults, then applies environment variable overrides.
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
	return cfg, nil
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
