// Command server runs the pocketkafka broker engine.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Yukaz0/pocketkafka/internal/audit"
	"github.com/Yukaz0/pocketkafka/internal/authz"
	"github.com/Yukaz0/pocketkafka/internal/config"
	"github.com/Yukaz0/pocketkafka/internal/coordinator"
	"github.com/Yukaz0/pocketkafka/internal/credential"
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
	// Operational subcommands never load a config or open a listener, so they
	// are dispatched before the flag set below.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			os.Exit(runHealthcheck(os.Args[2:]))
		case "hash-password":
			os.Exit(runHashPassword(os.Args[2:]))
		case "scram-verifier":
			os.Exit(runScramVerifier(os.Args[2:]))
		case "version":
			fmt.Println(version)
			return
		}
	}

	cfgPath := flag.String("config", "config/config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger.InitWithSink(cfg.Logging.Level, cfg.Logging.Format, web.LogRing)
	if err := audit.Init(cfg.Web.AuditLog); err != nil {
		log.Fatalf("init audit log: %v", err)
	}
	defer audit.Close()

	log.Printf("pocketkafka v%s starting (cluster=%s data_dir=%s)",
		version, cfg.Broker.ClusterID, cfg.Storage.DataDir)
	logSecurityPosture(cfg)
	startPprof()

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
	gm := coordinator.NewGroupManager(offsetStore, int32(cfg.Broker.ID), advHost, advPort, int32(cfg.Coordinator.SessionTimeoutMs)).
		WithDefaultRebalanceTimeout(time.Duration(cfg.Coordinator.RebalanceTimeoutMs) * time.Millisecond)
	// Flush the offset snapshot/WAL before the process exits, and drop idle
	// commits per offsets_retention_minutes while it runs.
	defer offsetStore.Close()
	defer startOffsetRetention(gm, cfg.Coordinator.OffsetsRetentionMinutes)()
	defer gm.StartSessionReaper(time.Second)()

	// Shared ACL store. When security is disabled the broker keeps its
	// historical allow-all behaviour (allow-all authorizer); when it is enabled
	// every ingress uses this store with default-deny. security.super_users
	// wraps it so an operator named there can administer a fresh install
	// instead of hand-editing __acls.json.
	aclStore, err := authz.NewStore(filepath.Join(cfg.Storage.DataDir, "__acls.json"))
	if err != nil {
		log.Fatalf("init ACL store: %v", err)
	}
	aclStore.SetAllowWildcardAdmin(cfg.Security.AllowWildcardAdmin)
	for _, rule := range aclStore.WildcardAdminRules() {
		log.Printf("WARNING: ACL rule grants Admin to %q, so every authenticated identity administers the cluster (set security.allow_wildcard_admin=true to silence this)", rule.Principal)
	}
	authorizer := authz.NewSuperUserAuthorizer(aclStore, cfg.Security.SuperUsers)
	if len(cfg.Security.SuperUsers) > 0 {
		log.Printf("pocketkafka super users (bypass every ACL): %s", strings.Join(cfg.Security.SuperUsers, ", "))
	}

	// One certificate serves every surface; the per-surface flags decide which
	// ones speak TLS.
	tlsConf, err := cfg.Security.TLS.ServerConfig()
	if err != nil {
		log.Fatalf("configure TLS: %v", err)
	}

	// Handlers + server.
	h := handler.New(store, gm, &cfg, int32(cfg.Broker.ID), advHost, advPort)
	if cfg.Security.Enabled {
		h.WithAuthorizer(authorizer)
	}
	srv := server.New(&cfg, h)

	if err := srv.Start(); err != nil {
		log.Fatalf("start server: %v", err)
	}

	// Management surfaces require identity/authorization whenever security is
	// enabled, so REST/Schema Registry cannot bypass the ACLs enforced on the
	// Kafka protocol path.
	httpPolicy := httpauth.Policy{
		Public:     []string{"/healthz", "/livez"},
		RequireTLS: cfg.Security.Enabled && cfg.Security.RequireTLSForPlain,
	}

	// Embedded Schema Registry (port 8081, Confluent-compatible). Schemas are
	// persisted under the data dir so IDs/versions survive a restart.
	sr, err := schemaregistry.Open(filepath.Join(cfg.Storage.DataDir, "__schemas.json"))
	if err != nil {
		log.Fatalf("init schema registry: %v", err)
	}
	srHandler := httpauth.New(cfg.Security.Enabled, cfg.Security.Users, authorizer, httpPolicy).
		Wrap(limitBody(sr.Handler(), cfg.Network.MaxRequestSizeBytes))
	srSrv := newHTTPServer(cfg, cfg.SchemaRegistry.Listen, srHandler, false)
	go func() {
		log.Printf("pocketkafka schema registry on %s", httpScheme(cfg.Security.TLS.HTTP)+cfg.SchemaRegistry.Listen)
		if err := serveHTTP(srSrv, tlsConf, cfg.Security.TLS.HTTP); err != nil && err != http.ErrServerClosed {
			log.Printf("schema registry error: %v", err)
		}
	}()

	// Embedded Web UI dashboard (port 8080), started only when web.enabled is
	// true. The MQTT bridge handle is wired after this block, so the server
	// exposes it through WithMQTT below.
	ws, webSrv := buildWebServer(cfg, store, gm, sr, aclStore, authorizer, version)
	if webSrv != nil {
		go func() {
			log.Printf("pocketkafka web UI on %s", httpScheme(cfg.Security.TLS.HTTP)+cfg.Web.Listen)
			if err := serveHTTP(webSrv, tlsConf, cfg.Security.TLS.HTTP); err != nil && err != http.ErrServerClosed {
				log.Printf("web server error: %v", err)
			}
		}()
	} else {
		log.Printf("pocketkafka web UI disabled (web.enabled=false)")
	}

	// HTTP REST Proxy gateway (port 8082).
	restHandler := httpauth.New(cfg.Security.Enabled, cfg.Security.Users, authorizer, httpPolicy).
		Wrap(limitBody(gateway.NewRESTProxy(store).Handler(), cfg.Network.MaxRequestSizeBytes))
	gwSrv := newHTTPServer(cfg, cfg.Gateway.Listen, restHandler, false)
	go func() {
		log.Printf("pocketkafka REST proxy on %s", httpScheme(cfg.Security.TLS.HTTP)+cfg.Gateway.Listen)
		if err := serveHTTP(gwSrv, tlsConf, cfg.Security.TLS.HTTP); err != nil && err != http.ErrServerClosed {
			log.Printf("rest proxy error: %v", err)
		}
	}()

	// MQTT Bridge (port 1883) bridging IoT topics to Kafka. When security is on
	// the bridge authenticates CONNECT and authorizes publish/subscribe against
	// the mapped Kafka topic so MQTT is not an authorization bypass.
	mqttBridge := gateway.NewMQTTBridge(store).
		WithSecurity(cfg.Security.Enabled, cfg.Security.Users, authorizer)
	if cfg.Security.TLS.MQTT {
		mqttBridge.WithTLS(tlsConf, cfg.Security.RequireTLSForPlain)
	} else if cfg.Security.Enabled && cfg.Security.RequireTLSForPlain {
		log.Printf("mqtt: security.require_tls_for_plain is true but security.tls.mqtt is false, so a CONNECT carrying a password is refused; set security.tls.mqtt=true or turn the policy off")
	}
	mqttStartErr := mqttBridge.Start(cfg.MQTT.Listen)
	if mqttStartErr != nil {
		log.Printf("mqtt bridge error: %v", mqttStartErr)
	}
	if ws != nil {
		// The listener counts as running only when it bound: wiring the bridge
		// unconditionally made the Integrations card report LISTENING while the
		// port was in fact held by another process.
		ws.WithMQTT(mqttBridge, cfg.MQTT.Listen, mqttStartErr == nil)
	}

	// Health sampler: keeps a rolling sample ring for /api/v1/health/overview,
	// which the dashboard Health view renders, so history exists even before
	// anyone opens the page.
	if ws != nil {
		samplerCtx, stopSampler := context.WithCancel(context.Background())
		defer stopSampler()
		ws.StartSampler(samplerCtx, store, gm, 5*time.Second)
	}

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
	// Close the MQTT bridge first and explicitly: it must finish before the
	// deferred store.Close/offsetStore.Close run, otherwise a bridge that hangs
	// on a live client would stall the final fsync/snapshot and the broker
	// would not exit cleanly.
	mqttBridge.Close()
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
func buildWebServer(cfg config.Config, store *storage.Store, gm *coordinator.GroupManager, sr *schemaregistry.Registry, aclStore *authz.Store, authorizer authz.Authorizer, version string) (*web.Server, *http.Server) {
	if !cfg.Web.Enabled {
		return nil, nil
	}
	ws := web.New(store, gm, sr, int32(cfg.Broker.ID), cfg.Broker.ClusterID, version).WithAuth(cfg).
		WithDataDir(cfg.Storage.DataDir).
		WithACLStore(aclStore).
		WithAuthorizer(authorizer).
		WithBrokerInfo(listenerAddrs(cfg), advertisedString(cfg), securityModeOf(cfg)).
		WithConfig(cfg).
		WithGateway(cfg.Gateway.Listen).
		WithSchemaRegistryListen(cfg.SchemaRegistry.Listen)
	ws.WithClusterMonitoring(cfg)
	ws.InstallLogSink()
	srv := newHTTPServer(cfg, cfg.Web.Listen, limitBody(ws.Handler(), cfg.Network.MaxRequestSizeBytes), true)
	return ws, srv
}

// serveHTTP starts a management server, over TLS when the surface is switched
// to it. The certificate comes from the shared security.tls block.
func serveHTTP(srv *http.Server, tlsConf *tls.Config, useTLS bool) error {
	if !useTLS || tlsConf == nil {
		return srv.ListenAndServe()
	}
	cp := tlsConf.Clone()
	srv.TLSConfig = cp
	return srv.ListenAndServeTLS("", "")
}

// httpScheme labels a listen address in the startup log.
func httpScheme(useTLS bool) string {
	if useTLS {
		return "https://"
	}
	return "http://"
}

// logSecurityPosture states the exposure the broker is about to accept. A
// security-disabled broker is only reachable from loopback now (config
// validation refuses anything else unless the operator opted in), but the
// operator should still see it in the log.
func logSecurityPosture(cfg config.Config) {
	switch {
	case cfg.Security.Enabled && cfg.Security.TLS.Any():
		log.Printf("security: enabled, TLS on %s", tlsSurfaces(cfg.Security.TLS))
	case cfg.Security.Enabled:
		log.Printf("security: enabled, but no TLS surface is configured (security.tls.*): SASL/PLAIN, HTTP Basic and MQTT passwords are refused while require_tls_for_plain is true")
	default:
		log.Printf("WARNING: security.enabled=false: every interface is unauthenticated and every ACL is bypassed (allow-all)")
	}
	if cfg.Security.RequireTLSForPlain && !cfg.Security.TLS.HTTP && cfg.Security.Enabled {
		log.Printf("security: require_tls_for_plain=true with security.tls.http=false: the REST proxy and Schema Registry refuse Basic credentials (a TLS-terminating proxy needs require_tls_for_plain=false)")
	}
}

// tlsSurfaces names the enabled TLS surfaces for the startup log.
func tlsSurfaces(t config.TLS) string {
	out := []string{}
	if t.Enabled {
		out = append(out, "kafka:"+t.Listen)
	}
	if t.HTTP {
		out = append(out, "http")
	}
	if t.MQTT {
		out = append(out, "mqtt")
	}
	return strings.Join(out, ",")
}

// startPprof serves the Go profiler on a dedicated mux. It is opt-in through
// KAFKA_PPROF_LISTEN and bound to loopback unless KAFKA_PPROF_ALLOW_PUBLIC is
// set: /debug/pprof exposes the command line, the heap and every goroutine
// stack, and the CPU profiler is itself a denial-of-service lever.
func startPprof() {
	listen := strings.TrimSpace(os.Getenv("KAFKA_PPROF_LISTEN"))
	if listen == "" {
		return
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		log.Printf("pprof: %q is not a host:port address; pprof disabled", listen)
		return
	}
	if !config.IsLoopbackHost(host) && !envTrue("KAFKA_PPROF_ALLOW_PUBLIC") {
		log.Printf("pprof: refusing to bind %s (not loopback); set KAFKA_PPROF_ALLOW_PUBLIC=true to accept publishing profiles", listen)
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	// A bare http.ListenAndServe has no timeouts at all: a slow client could
	// hold a connection open indefinitely. The write timeout is generous because
	// /debug/pprof/profile streams for as long as the requested profile run.
	srv := &http.Server{
		Addr:              listen,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("pocketkafka pprof on http://%s/debug/pprof/", listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("pprof error: %v", err)
		}
	}()
}

func envTrue(name string) bool {
	v := strings.TrimSpace(os.Getenv(name))
	b, err := strconv.ParseBool(v)
	return err == nil && b
}

// runHealthcheck dials a listener and exits 0 when it accepts a connection.
// The container HEALTHCHECK uses it, so the runtime image needs no netcat or
// wget.
func runHealthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "host:port of the Kafka listener")
	timeout := fs.Duration("timeout", 3*time.Second, "dial timeout")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	conn, err := net.DialTimeout("tcp", *addr, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "unhealthy:", err)
		return 1
	}
	conn.Close()
	return 0
}

// runHashPassword prints a PBKDF2 verifier for security.users[].password_hash.
// The password is read from the flag or, preferably, from stdin so it stays out
// of the shell history.
func runHashPassword(args []string) int {
	fs := flag.NewFlagSet("hash-password", flag.ContinueOnError)
	password := fs.String("password", "", "password to hash (read from stdin when empty)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	pw, err := resolveSecret(*password)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	hash, err := credential.HashPassword(pw)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(hash)
	return 0
}

// runScramVerifier prints a SCRAM verifier for security.users[].scram_verifier:
// the form that lets the broker authenticate a SCRAM client without ever
// holding the password.
func runScramVerifier(args []string) int {
	fs := flag.NewFlagSet("scram-verifier", flag.ContinueOnError)
	password := fs.String("password", "", "password to derive the verifier from (read from stdin when empty)")
	mechanism := fs.String("mechanism", "SCRAM-SHA-256", "SCRAM-SHA-256 or SCRAM-SHA-512")
	iterations := fs.Int("iterations", credential.DefaultSCRAMIterations, "PBKDF2 iteration count")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	mech, ok := credential.MechanismByName(*mechanism)
	if !ok {
		fmt.Fprintf(os.Stderr, "unsupported mechanism %q\n", *mechanism)
		return 1
	}
	pw, err := resolveSecret(*password)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	salt, err := credential.RandomSalt(32)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	verifier, err := credential.DeriveVerifier(mech, pw, salt, *iterations)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(verifier.String())
	return 0
}

// resolveSecret returns the flag value, or reads one line from stdin when the
// flag is empty.
func resolveSecret(flagValue string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("read secret from stdin: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", fmt.Errorf("no secret provided")
	}
	return line, nil
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
	return primaryAdvertised(cfg)
}

// primaryAdvertised picks the advertised address used when a connection does
// not arrive through a listener with an entry of its own: the "plain" listener
// when there is one, otherwise the first name in sorted order. Iterating the map
// directly would pick a different address on every start.
func primaryAdvertised(cfg config.Config) string {
	if addr := cfg.AdvertisedListeners["plain"]; addr != "" {
		return addr
	}
	names := make([]string, 0, len(cfg.AdvertisedListeners))
	for name := range cfg.AdvertisedListeners {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if addr := cfg.AdvertisedListeners[name]; addr != "" {
			return addr
		}
	}
	return ""
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
	addr := primaryAdvertised(cfg)
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
