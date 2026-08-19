package config

import (
	"strconv"
	"strings"
)

// parseYAML parses a small YAML subset: nested maps of scalars plus sequences of
// maps (e.g. security.users). Indentation is significant and '#' starts a
// comment. This is enough for config/config.yaml and keeps the module free of
// external dependencies.
func parseYAML(data []byte) (map[string]interface{}, error) {
	root := map[string]interface{}{}

	type yamlLine struct {
		indent  int
		content string
	}
	var lines []yamlLine
	for _, raw := range strings.Split(string(data), "\n") {
		line := stripComment(raw)
		trimmed := strings.TrimRight(line, " \t\r")
		c := strings.TrimSpace(trimmed)
		if c == "" {
			continue
		}
		lines = append(lines, yamlLine{leadingSpaces(trimmed), c})
	}

	// stack of (indent, map) for nested scopes.
	type scope struct {
		indent int
		m      map[string]interface{}
	}
	stack := []scope{{indent: -1, m: root}}

	for i := 0; i < len(lines); i++ {
		ln := lines[i]
		for len(stack) > 1 && ln.indent <= stack[len(stack)-1].indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1].m

		if strings.HasPrefix(ln.content, "- ") {
			// A sequence item; append a fresh element map to the parent's list.
			seqKey := lastSeqKey(parent)
			if seqKey == "" {
				continue
			}
			item := map[string]interface{}{}
			rest := strings.TrimSpace(ln.content[2:])
			k, v, has := splitKeyValue(rest)
			if has {
				item[k] = parseScalar(v)
			} else {
				item[k] = ""
			}
			parent[seqKey] = append(parent[seqKey].([]interface{}), item)
			// Subsequent deeper lines populate this element map.
			stack = append(stack, scope{indent: ln.indent, m: item})
			continue
		}

		key, val, hasVal := splitKeyValue(ln.content)
		if !hasVal {
			child := map[string]interface{}{}
			// A key with no value is a nested map, unless its children are
			// sequence items, in which case it is a list.
			if i+1 < len(lines) && lines[i+1].indent > ln.indent && strings.HasPrefix(lines[i+1].content, "- ") {
				parent[key] = []interface{}{}
				continue
			}
			parent[key] = child
			stack = append(stack, scope{indent: ln.indent, m: child})
			continue
		}
		parent[key] = parseScalar(val)
	}
	return root, nil
}

// lastSeqKey returns a key in m whose value is a list (used to place sequence
// items). Config sections have at most one list, so this is unambiguous here.
func lastSeqKey(m map[string]interface{}) string {
	for k, v := range m {
		if _, ok := v.([]interface{}); ok {
			return k
		}
	}
	return ""
}

func stripComment(line string) string {
	if idx := strings.Index(line, "#"); idx >= 0 {
		return line[:idx]
	}
	return line
}

func leadingSpaces(s string) int {
	n := 0
	for n < len(s) && (s[n] == ' ' || s[n] == '\t') {
		n++
	}
	return n
}

func splitKeyValue(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	colon := strings.IndexByte(s, ':')
	if colon < 0 {
		return s, "", false
	}
	key := strings.TrimSpace(s[:colon])
	val := strings.TrimSpace(s[colon+1:])
	if val == "" {
		// A key with no value begins a nested map.
		return key, "", false
	}
	return key, val, true
}

func parseScalar(s string) interface{} {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	if (strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return strings.Trim(s, "\"'")
	}
	switch s {
	case "true":
		return true
	case "false":
		return false
	case "null", "~":
		return nil
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// asMap returns m[k] as a nested map.
func asMap(m map[string]interface{}, k string) map[string]interface{} {
	if v, ok := m[k].(map[string]interface{}); ok {
		return v
	}
	return nil
}

func getString(m map[string]interface{}, k string, def string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return def
}

func getInt(m map[string]interface{}, k string, def int) int {
	switch v := m[k].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return def
}

func getInt64(m map[string]interface{}, k string, def int64) int64 {
	switch v := m[k].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return def
}

func getBool(m map[string]interface{}, k string, def bool) bool {
	if v, ok := m[k].(bool); ok {
		return v
	}
	return def
}

// applyYAML overlays parsed YAML values onto a Config.
func applyYAML(cfg *Config, m map[string]interface{}) {
	if b := asMap(m, "broker"); b != nil {
		cfg.Broker.ID = getInt(b, "id", cfg.Broker.ID)
		cfg.Broker.ClusterID = getString(b, "cluster_id", cfg.Broker.ClusterID)
		cfg.Broker.Rack = getString(b, "rack", cfg.Broker.Rack)
	}
	if l := asMap(m, "listeners"); l != nil {
		for k, v := range l {
			if s, ok := v.(string); ok {
				cfg.Listeners[k] = s
			}
		}
	}
	if a := asMap(m, "advertised_listeners"); a != nil {
		for k, v := range a {
			if s, ok := v.(string); ok {
				cfg.AdvertisedListeners[k] = s
			}
		}
	}
	if s := asMap(m, "storage"); s != nil {
		cfg.Storage.DataDir = getString(s, "data_dir", cfg.Storage.DataDir)
		cfg.Storage.SegmentMaxBytes = getInt64(s, "segment_max_bytes", cfg.Storage.SegmentMaxBytes)
		cfg.Storage.IndexIntervalBytes = getInt64(s, "index_interval_bytes", cfg.Storage.IndexIntervalBytes)
		if r := asMap(s, "retention"); r != nil {
			cfg.Storage.Retention.CheckIntervalMs = getInt64(r, "check_interval_ms", cfg.Storage.Retention.CheckIntervalMs)
			cfg.Storage.Retention.RetentionHours = getInt(r, "retention_hours", cfg.Storage.Retention.RetentionHours)
			cfg.Storage.Retention.RetentionBytes = getInt64(r, "retention_bytes", cfg.Storage.Retention.RetentionBytes)
		}
	}
	if t := asMap(m, "topics"); t != nil {
		cfg.Topics.AutoCreate = getBool(t, "auto_create", cfg.Topics.AutoCreate)
		cfg.Topics.DefaultPartitions = getInt(t, "default_partitions", cfg.Topics.DefaultPartitions)
		cfg.Topics.DefaultReplicationFactor = getInt(t, "default_replication_factor", cfg.Topics.DefaultReplicationFactor)
	}
	if c := asMap(m, "coordinator"); c != nil {
		cfg.Coordinator.SessionTimeoutMs = getInt(c, "session_timeout_ms", cfg.Coordinator.SessionTimeoutMs)
		cfg.Coordinator.RebalanceTimeoutMs = getInt(c, "rebalance_timeout_ms", cfg.Coordinator.RebalanceTimeoutMs)
		cfg.Coordinator.HeartbeatIntervalMs = getInt(c, "heartbeat_interval_ms", cfg.Coordinator.HeartbeatIntervalMs)
		cfg.Coordinator.OffsetsRetentionMinutes = getInt(c, "offsets_retention_minutes", cfg.Coordinator.OffsetsRetentionMinutes)
		cfg.Coordinator.StorageBackend = getString(c, "storage_backend", cfg.Coordinator.StorageBackend)
	}
	if n := asMap(m, "network"); n != nil {
		cfg.Network.MaxConnections = getInt(n, "max_connections", cfg.Network.MaxConnections)
		cfg.Network.ReadBufferBytes = getInt(n, "read_buffer_bytes", cfg.Network.ReadBufferBytes)
		cfg.Network.WriteBufferBytes = getInt(n, "write_buffer_bytes", cfg.Network.WriteBufferBytes)
		cfg.Network.MaxRequestSizeBytes = getInt64(n, "max_request_size_bytes", cfg.Network.MaxRequestSizeBytes)
	}
	if lg := asMap(m, "logging"); lg != nil {
		cfg.Logging.Level = getString(lg, "level", cfg.Logging.Level)
		cfg.Logging.Format = getString(lg, "format", cfg.Logging.Format)
	}
	if w := asMap(m, "web"); w != nil {
		cfg.Web.Listen = getString(w, "listen", cfg.Web.Listen)
	}
	if sr := asMap(m, "schema_registry"); sr != nil {
		cfg.SchemaRegistry.Listen = getString(sr, "listen", cfg.SchemaRegistry.Listen)
	}
	if gw := asMap(m, "gateway"); gw != nil {
		cfg.Gateway.Listen = getString(gw, "listen", cfg.Gateway.Listen)
	}
	if mq := asMap(m, "mqtt"); mq != nil {
		cfg.MQTT.Listen = getString(mq, "listen", cfg.MQTT.Listen)
	}
	if sec := asMap(m, "security"); sec != nil {
		cfg.Security.Enabled = getBool(sec, "enabled", cfg.Security.Enabled)
		if users, ok := sec["users"].([]interface{}); ok {
			for _, u := range users {
				if um, ok := u.(map[string]interface{}); ok {
					cfg.Security.Users = append(cfg.Security.Users, SecurityUser{
						Username: getString(um, "username", ""),
						Password: getString(um, "password", ""),
					})
				}
			}
		}
		if t := asMap(sec, "tls"); t != nil {
			cfg.Security.TLS.Enabled = getBool(t, "enabled", cfg.Security.TLS.Enabled)
			cfg.Security.TLS.Listen = getString(t, "listen", cfg.Security.TLS.Listen)
			cfg.Security.TLS.CertFile = getString(t, "cert_file", cfg.Security.TLS.CertFile)
			cfg.Security.TLS.KeyFile = getString(t, "key_file", cfg.Security.TLS.KeyFile)
		}
	}
}
