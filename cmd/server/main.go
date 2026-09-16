// Command server runs the pocketkafka broker engine.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof" // registrasi handler /debug/pprof ke DefaultServeMux
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/gateway"
	"github.com/Yukaz0/pocketkafka/internal/handler"
	"github.com/Yukaz0/pocketkafka/internal/httpauth"
	"github.com/Yukaz0/pocketkafka/internal/logger"
	"github.com/Yukaz0/pocketkafka/internal/schemaregistry"
	"github.com/Yukaz0/pocketkafka/internal/server"
	"github.com/Yukaz0/pocketkafka/internal/storage"
	"github.com/Yukaz0/pocketkafka/internal/tier"
	"github.com/Yukaz0/pocketkafka/internal/web"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "config/config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger.InitWithSink(cfg.Logging.Level, cfg.Logging.Format, web.LogRing)

	log.Printf("pocketkafka v%s starting (cluster=%s data_dir=%s)",
		version, cfg.Broker.ClusterID, cfg.Storage.DataDir)

	// Storage engine.
	store, err := storage.NewStore(cfg.Storage.DataDir, cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer store.Close()

	// Durability: the "interval" flush policy fsyncs all partitions
	// periodically; "request" and acks=-1 sync per produce ack (see handler).
	if cfg.Storage.FlushPolicy == config.FlushPolicyInterval {
		stopFlush := store.StartFlush(time.Duration(cfg.Storage.FlushIntervalMs) * time.Millisecond)
		defer stopFlush()
	}

	// Offsets + group coordinator.
	offsetDir := cfg.Storage.DataDir + "/__coordinator"
	offsetStore, err := coordinator.NewOffsetStore(cfg.Coordinator.StorageBackend, offsetDir)
	if err != nil {
		log.Fatalf("init offset store: %v", err)
	}
	advHost, advPort := advertised(cfg)
	gm := coordinator.NewGroupManager(offsetStore, int32(cfg.Broker.ID), advHost, advPort, int32(cfg.Coordinator.SessionTimeoutMs))
	// Flush the offset snapshot/WAL before the process exits, and drop idle
	// commits per offsets_retention_minutes while it runs.
	defer offsetStore.Close()
	defer startOffsetRetention(gm, cfg.Coordinator.OffsetsRetentionMinutes)()

	// Shared ACL store. When security is disabled the broker keeps its
	// historical allow-all behaviour (allow-all authorizer); when it is enabled
	// every ingress uses this store with default-deny.
	aclStore, err := authz.NewStore(filepath.Join(cfg.Storage.DataDir, "__acls.json"))
	if err != nil {
		log.Fatalf("init ACL store: %v", err)
	}

	// Handlers + server.
	h := handler.New(store, gm, &cfg, int32(cfg.Broker.ID), advHost, advPort)
	if cfg.Security.Enabled {
		h.WithAuthorizer(aclStore)
	}
	srv := server.New(&cfg, h)

	if err := srv.Start(); err != nil {
		log.Fatalf("start server: %v", err)
	}

	// pprof (opsional, default off): KAFKA_PPROF_LISTEN=0.0.0.0:6060 untuk
	// profiling CPU/heap broker tanpa deploy ulang.
	if listen := os.Getenv("KAFKA_PPROF_LISTEN"); listen != "" {
		go func() {
			log.Printf("pocketkafka pprof on http://%s/debug/pprof/", listen)
			if err := http.ListenAndServe(listen, nil); err != nil {
				log.Printf("pprof error: %v", err)
			}
		}()
	}

	// Management surfaces require identity/authorization whenever security is
	// enabled, so REST/Schema Registry cannot bypass the ACLs enforced on the
	// Kafka protocol path.
	httpPolicy := httpauth.Policy{Public: []string{"/healthz", "/livez"}}

	// Embedded Schema Registry (port 8081, Confluent-compatible). Schemas are
	// persisted under the data dir so IDs/versions survive a restart.
	sr, err := schemaregistry.Open(filepath.Join(cfg.Storage.DataDir, "__schemas.json"))
	if err != nil {
		log.Fatalf("init schema registry: %v", err)
	}
	srHandler := httpauth.New(cfg.Security.Enabled, cfg.Security.Users, aclStore, httpPolicy).
		Wrap(limitBody(sr.Handler(), cfg.Network.MaxRequestSizeBytes))
	srSrv := newHTTPServer(cfg, cfg.SchemaRegistry.Listen, srHandler, false)
	go func() {
		log.Printf("pocketkafka schema registry on http://%s", cfg.SchemaRegistry.Listen)
		if err := srSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("schema registry error: %v", err)
		}
	}()

	// Embedded Web UI dashboard (port 8080), started only when web.enabled is
	// true. The MQTT bridge handle is wired after this block, so the server
	// exposes it through WithMQTT below.
	ws, webSrv := buildWebServer(cfg, store, gm, sr, aclStore, version)
	if webSrv != nil {
		go func() {
			log.Printf("pocketkafka web UI on http://%s", cfg.Web.Listen)
			if err := webSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("web server error: %v", err)
			}
		}()
	} else {
		log.Printf("pocketkafka web UI disabled (web.enabled=false)")
	}

	// HTTP REST Proxy gateway (port 8082).
	restHandler := httpauth.New(cfg.Security.Enabled, cfg.Security.Users, aclStore, httpPolicy).
		Wrap(limitBody(gateway.NewRESTProxy(store).Handler(), cfg.Network.MaxRequestSizeBytes))
	gwSrv := newHTTPServer(cfg, cfg.Gateway.Listen, restHandler, false)
	go func() {
		log.Printf("pocketkafka REST proxy on http://%s", cfg.Gateway.Listen)
		if err := gwSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("rest proxy error: %v", err)
		}
	}()

	// MQTT Bridge (port 1883) bridging IoT topics to Kafka. When security is on
	// the bridge authenticates CONNECT and authorizes publish/subscribe against
	// the mapped Kafka topic so MQTT is not an authorization bypass.
	mqttBridge := gateway.NewMQTTBridge(store).
		WithSecurity(cfg.Security.Enabled, cfg.Security.Users, aclStore)
	if err := mqttBridge.Start(cfg.MQTT.Listen); err != nil {
		log.Printf("mqtt bridge error: %v", err)
	}
	if ws != nil {
		ws.WithMQTT(mqttBridge, cfg.MQTT.Listen)
	}
	defer mqttBridge.Close()

	// Background retention cleanup.
	retention := storage.Retention{
		CheckInterval:  time.Duration(cfg.Storage.Retention.CheckIntervalMs) * time.Millisecond,
		RetentionTime:  time.Duration(cfg.Storage.Retention.RetentionHours) * time.Hour,
		RetentionBytes: cfg.Storage.Retention.RetentionBytes,
	}
	stopRetention := store.StartRetention(retention)

	// Background log compaction for cleanup.policy=compact topics.
	stopCompaction := store.StartCompaction(60 * time.Second)

	// S3/MinIO tiered (cold) storage.
	if cfg.Storage.Tiered.Enabled {
		client := tier.NewS3Client(
			cfg.Storage.Tiered.Endpoint, cfg.Storage.Tiered.Bucket,
			cfg.Storage.Tiered.AccessKey, cfg.Storage.Tiered.SecretKey,
			cfg.Storage.Tiered.Region, cfg.Storage.Tiered.Prefix,
		)
		interval := time.Duration(cfg.Storage.Tiered.CheckIntervalMs) * time.Millisecond
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		threshold := time.Duration(cfg.Storage.Tiered.OffloadAfterHours) * time.Hour
		stopTiering := store.EnableTiering(client, threshold, interval)
		defer stopTiering()
		log.Printf("pocketkafka tiered storage enabled -> %s/%s", cfg.Storage.Tiered.Endpoint, cfg.Storage.Tiered.Bucket)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	stopRetention()
	stopCompaction()
	shutdownHTTP(webSrv)
	shutdownHTTP(gwSrv)
	shutdownHTTP(srSrv)
	srv.Close()
}

// startOffsetRetention drops committed offsets idle beyond retentionMinutes on
// a background ticker. It returns a stop function; a non-positive retention
// disables the loop (offset retention turned off).
func startOffsetRetention(gm *coordinator.GroupManager, retentionMinutes int) func() {
	if retentionMinutes <= 0 {
		return func() {}
	}
	retention := time.Duration(retentionMinutes) * time.Minute
	interval := retention / 2
	if interval < time.Minute {
		interval = time.Minute
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if n := gm.PruneOffsets(time.Now(), retention); n > 0 {
					log.Printf("offset retention: pruned %d idle committed offsets", n)
				}
			}
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

// buildWebServer constructs the embedded dashboard server. It returns
// (nil, nil) when the web UI is disabled, so callers must treat a nil server as
// "not started" instead of unconditionally shutting it down.
func buildWebServer(cfg config.Config, store *storage.Store, gm *coordinator.GroupManager, sr *schemaregistry.Registry, aclStore *authz.Store, version string) (*web.Server, *http.Server) {
	if !cfg.Web.Enabled {
		return nil, nil
	}
	ws := web.New(store, gm, sr, int32(cfg.Broker.ID), cfg.Broker.ClusterID, version).WithAuth(cfg).
		WithDataDir(cfg.Storage.DataDir).
		WithACLStore(aclStore).
		WithBrokerInfo(listenerAddrs(cfg), advertisedString(cfg), securityModeOf(cfg))
	ws.InstallLogSink()
	srv := newHTTPServer(cfg, cfg.Web.Listen, limitBody(ws.Handler(), cfg.Network.MaxRequestSizeBytes), true)
	return ws, srv
}

// newHTTPServer builds an http.Server with the network timeouts from config.
// allowWebSocket must be true for servers that hijack connections for
// WebSockets: a WriteTimeout deadline would survive the hijack and break the
// long-lived tail stream, so it is left unset for those.
func newHTTPServer(cfg config.Config, addr string, h http.Handler, allowWebSocket bool) *http.Server {
	srv := &http.Server{Addr: addr, Handler: h}
	if ms := cfg.Network.ReadTimeoutMs; ms > 0 {
		d := time.Duration(ms) * time.Millisecond
		srv.ReadTimeout = d
		srv.ReadHeaderTimeout = d
	}
	if ms := cfg.Network.IdleTimeoutMs; ms > 0 {
		srv.IdleTimeout = time.Duration(ms) * time.Millisecond
	}
	if !allowWebSocket {
		if ms := cfg.Network.WriteTimeoutMs; ms > 0 {
			srv.WriteTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	return srv
}

// limitBody bounds the size of any request body a handler will read, so a
// client cannot exhaust memory with an oversized payload.
func limitBody(next http.Handler, max int64) http.Handler {
	if max <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, max)
		}
		next.ServeHTTP(w, r)
	})
}

// shutdownHTTP gracefully stops an HTTP server. It is nil-safe so callers do not
// need to special-case servers that were never started (e.g. a disabled web UI).
func shutdownHTTP(srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

// listenerAddrs returns the sorted listener addresses from config.
func listenerAddrs(cfg config.Config) []string {
	out := make([]string, 0, len(cfg.Listeners))
	for _, v := range cfg.Listeners {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// advertisedString returns the primary advertised address (host:port).
func advertisedString(cfg config.Config) string {
	addr := cfg.AdvertisedListeners["plain"]
	if addr == "" {
		for _, v := range cfg.AdvertisedListeners {
			addr = v
			break
		}
	}
	return addr
}

// securityModeOf reports the auth mode: SASL/PLAIN, SASL/SCRAM, or PLAINTEXT.
func securityModeOf(cfg config.Config) string {
	if !cfg.Security.Enabled {
		return "PLAINTEXT"
	}
	return "SASL"
}

// advertised extracts the host and port of the primary advertised listener.
func advertised(cfg config.Config) (string, int32) {
	addr := cfg.AdvertisedListeners["plain"]
	if addr == "" {
		for _, v := range cfg.AdvertisedListeners {
			addr = v
			break
		}
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "localhost", 9092
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	port, err := strconv.Atoi(strings.TrimSpace(portStr))
	if err != nil {
		port = 9092
	}
	return host, int32(port)
}
