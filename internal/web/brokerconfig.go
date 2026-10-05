package web

import (
	"net/http"
	"strings"
)

// handleConfig serves the effective broker configuration (defaults applied) for
// the read-only "Broker configuration" card.
//
// The response is assembled field by field from an explicit allowlist instead
// of serialising the config struct, so a secret added to config.Config later is
// not exposed until someone lists it here. Names only, never passwords:
// security.users is reduced to usernames, and the tiered endpoint keeps its
// host while any embedded credentials are dropped.
func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfg
	users := make([]string, 0, len(cfg.Security.Users))
	for _, u := range cfg.Security.Users {
		users = append(users, u.Username)
	}
	clusters := make([]map[string]string, 0, len(cfg.Web.Clusters))
	for _, c := range cfg.Web.Clusters {
		clusters = append(clusters, map[string]string{"name": c.Name, "url": c.URL})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"broker": map[string]any{
			"id":        cfg.Broker.ID,
			"clusterId": cfg.Broker.ClusterID,
			"rack":      cfg.Broker.Rack,
		},
		"listeners":           stringMapCopy(cfg.Listeners),
		"advertisedListeners": stringMapCopy(cfg.AdvertisedListeners),
		"storage": map[string]any{
			"dataDir":            cfg.Storage.DataDir,
			"segmentMaxBytes":    cfg.Storage.SegmentMaxBytes,
			"indexIntervalBytes": cfg.Storage.IndexIntervalBytes,
			"flushPolicy":        cfg.Storage.FlushPolicy,
			"flushIntervalMs":    cfg.Storage.FlushIntervalMs,
			"syncOnAcksAll":      cfg.Storage.SyncOnAcksAll,
			"retention": map[string]any{
				"checkIntervalMs": cfg.Storage.Retention.CheckIntervalMs,
				"retentionHours":  cfg.Storage.Retention.RetentionHours,
				"retentionBytes":  cfg.Storage.Retention.RetentionBytes,
			},
			"tiered": map[string]any{
				"enabled":           cfg.Storage.Tiered.Enabled,
				"endpoint":          hostOnly(cfg.Storage.Tiered.Endpoint),
				"bucket":            cfg.Storage.Tiered.Bucket,
				"region":            cfg.Storage.Tiered.Region,
				"prefix":            cfg.Storage.Tiered.Prefix,
				"offloadAfterHours": cfg.Storage.Tiered.OffloadAfterHours,
				"checkIntervalMs":   cfg.Storage.Tiered.CheckIntervalMs,
			},
		},
		"topics": map[string]any{
			"autoCreate":               cfg.Topics.AutoCreate,
			"defaultPartitions":        cfg.Topics.DefaultPartitions,
			"defaultReplicationFactor": cfg.Topics.DefaultReplicationFactor,
		},
		"coordinator": map[string]any{
			"sessionTimeoutMs":        cfg.Coordinator.SessionTimeoutMs,
			"rebalanceTimeoutMs":      cfg.Coordinator.RebalanceTimeoutMs,
			"heartbeatIntervalMs":     cfg.Coordinator.HeartbeatIntervalMs,
			"offsetsRetentionMinutes": cfg.Coordinator.OffsetsRetentionMinutes,
			"storageBackend":          cfg.Coordinator.StorageBackend,
		},
		"network": map[string]any{
			"maxConnections":      cfg.Network.MaxConnections,
			"readBufferBytes":     cfg.Network.ReadBufferBytes,
			"writeBufferBytes":    cfg.Network.WriteBufferBytes,
			"maxRequestSizeBytes": cfg.Network.MaxRequestSizeBytes,
			"readTimeoutMs":       cfg.Network.ReadTimeoutMs,
			"writeTimeoutMs":      cfg.Network.WriteTimeoutMs,
			"idleTimeoutMs":       cfg.Network.IdleTimeoutMs,
		},
		"logging": map[string]any{
			"level":  cfg.Logging.Level,
			"format": cfg.Logging.Format,
		},
		"web": map[string]any{
			"listen":   cfg.Web.Listen,
			"enabled":  cfg.Web.Enabled,
			"auth":     cfg.Web.Auth,
			"clusters": clusters,
		},
		"schemaRegistry": map[string]any{"listen": cfg.SchemaRegistry.Listen},
		"gateway":        map[string]any{"listen": cfg.Gateway.Listen},
		"mqtt":           map[string]any{"listen": cfg.MQTT.Listen},
		"security": map[string]any{
			"enabled": cfg.Security.Enabled,
			"users":   users,
			"tls": map[string]any{
				"enabled":  cfg.Security.TLS.Enabled,
				"listen":   cfg.Security.TLS.Listen,
				"certFile": cfg.Security.TLS.CertFile,
				"keyFile":  cfg.Security.TLS.KeyFile,
			},
		},
	})
}

// stringMapCopy copies a listener map so a response never aliases server state.
func stringMapCopy(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// hostOnly reduces a configured endpoint to host[:port], dropping credentials
// a URL may embed. A bare "host:port" has no scheme, so url.Parse would read
// the host as a scheme; strip the userinfo textually instead.
func hostOnly(endpoint string) string {
	host := endpoint
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	if i := strings.IndexAny(host, "/?"); i >= 0 {
		host = host[:i]
	}
	if i := strings.LastIndex(host, "@"); i >= 0 {
		host = host[i+1:]
	}
	return host
}
