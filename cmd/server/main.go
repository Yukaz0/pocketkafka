// Command server runs the go-kafka-neu broker engine.
package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/neu/go-kafka-neu/internal/config"
	"github.com/neu/go-kafka-neu/internal/coordinator"
	"github.com/neu/go-kafka-neu/internal/gateway"
	"github.com/neu/go-kafka-neu/internal/handler"
	"github.com/neu/go-kafka-neu/internal/logger"
	"github.com/neu/go-kafka-neu/internal/schemaregistry"
	"github.com/neu/go-kafka-neu/internal/server"
	"github.com/neu/go-kafka-neu/internal/storage"
	"github.com/neu/go-kafka-neu/internal/tier"
	"github.com/neu/go-kafka-neu/internal/web"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "config/config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger.Init(cfg.Logging.Level, cfg.Logging.Format)

	log.Printf("go-kafka-neu v%s starting (cluster=%s data_dir=%s)",
		version, cfg.Broker.ClusterID, cfg.Storage.DataDir)

	// Storage engine.
	store, err := storage.NewStore(cfg.Storage.DataDir, cfg.Storage.SegmentMaxBytes, cfg.Storage.IndexIntervalBytes)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer store.Close()

	// Offsets + group coordinator.
	offsetDir := cfg.Storage.DataDir + "/__coordinator"
	offsetStore, err := coordinator.NewOffsetStore(cfg.Coordinator.StorageBackend, offsetDir)
	if err != nil {
		log.Fatalf("init offset store: %v", err)
	}
	advHost, advPort := advertised(cfg)
	gm := coordinator.NewGroupManager(offsetStore, int32(cfg.Broker.ID), advHost, advPort, int32(cfg.Coordinator.SessionTimeoutMs))

	// Handlers + server.
	h := handler.New(store, gm, &cfg, int32(cfg.Broker.ID), advHost, advPort)
	srv := server.New(&cfg, h)

	if err := srv.Start(); err != nil {
		log.Fatalf("start server: %v", err)
	}

	// Embedded Schema Registry (port 8081, Confluent-compatible).
	sr := schemaregistry.New()
	srSrv := &http.Server{Addr: cfg.SchemaRegistry.Listen, Handler: sr.Handler()}
	go func() {
		log.Printf("go-kafka-neu schema registry on http://%s", cfg.SchemaRegistry.Listen)
		if err := srSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("schema registry error: %v", err)
		}
	}()

	// Embedded Web UI dashboard (port 8080).
	webSrv := &http.Server{
		Addr:    cfg.Web.Listen,
		Handler: web.New(store, gm, sr, int32(cfg.Broker.ID), cfg.Broker.ClusterID, version).WithAuth(cfg).Handler(),
	}
	go func() {
		log.Printf("go-kafka-neu web UI on http://%s", cfg.Web.Listen)
		if err := webSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("web server error: %v", err)
		}
	}()

	// HTTP REST Proxy gateway (port 8082).
	gwSrv := &http.Server{Addr: cfg.Gateway.Listen, Handler: gateway.NewRESTProxy(store).Handler()}
	go func() {
		log.Printf("go-kafka-neu REST proxy on http://%s", cfg.Gateway.Listen)
		if err := gwSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("rest proxy error: %v", err)
		}
	}()

	// MQTT Bridge (port 1883) bridging IoT topics to Kafka.
	mqttBridge := gateway.NewMQTTBridge(store)
	if err := mqttBridge.Start(cfg.MQTT.Listen); err != nil {
		log.Printf("mqtt bridge error: %v", err)
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
		log.Printf("go-kafka-neu tiered storage enabled -> %s/%s", cfg.Storage.Tiered.Endpoint, cfg.Storage.Tiered.Bucket)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	stopRetention()
	stopCompaction()
	srv.Close()
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
