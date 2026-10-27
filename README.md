<div align="center">

# 🦘 PocketKafka

### *The PocketBase of Event Streaming*

**An ultra-lightweight, all-in-one, 100% pure-Go Kafka streaming platform with zero external dependencies.**

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero-success.svg?style=flat-square)](go.mod)
[![Docker Image Size](https://img.shields.io/badge/Docker%20Size-%3C%2029MB-brightgreen.svg?style=flat-square)](#-quick-start)
[![Idle Memory](https://img.shields.io/badge/Idle%20RAM-%3C%2060MB-purple.svg?style=flat-square)](#-why-pocketkafka)

<p align="center">
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-why-pocketkafka">Why PocketKafka?</a> •
  <a href="#-components--ports">Components & Ports</a> •
  <a href="#-embedded-web-ui">Embedded Web UI</a> •
  <a href="#-client-sdk--multi-language-usage">Client SDK</a> •
  <a href="#-testing">Testing</a>
</p>

---

</div>

PocketKafka is a from-scratch implementation of the Apache Kafka binary wire protocol and a modern commit-log storage engine. It packs a **high-performance Kafka Broker**, an **embedded Control Center UI**, a **Confluent API-compatible Schema Registry subset (AVRO/JSON; PROTOBUF rejected, no compatibility checking — see hardening notes)**, **HTTP REST & MQTT Gateways**, **Log Compaction**, **S3 Tiered Storage**, and **SASL/SCRAM + TLS Security** into a **single static binary under 30MB**.

`go.mod` has **zero third-party dependencies**.

---

## ⚡ Why PocketKafka?

| Feature | 🦘 **PocketKafka** | **Apache Kafka** | **Redpanda** |
| :--- | :---: | :---: | :---: |
| **Language & Runtime** | **Pure Go (Static Binary)** | Java / JVM | C++ (Seastar) |
| **Idle Memory Footprint** | **< 60 MB** | ~1.5 GB+ (JVM) | ~512 MB - 1 GB |
| **Docker Image Size** | **< 29 MB** | ~600 MB+ | ~300 MB |
| **Embedded Web UI** | **Yes (Control Center, :8080)** | ❌ Needs extra container | Optional Console |
| **Built-in Schema Registry** | **Subset (Port 8081)** | ❌ Needs `cp-schema-registry` | Built-in |
| **HTTP REST & MQTT Gateways**| **Yes (:8082 / :1883)** | ❌ Needs separate proxies | ❌ Needs separate proxies |
| **External Dependencies** | **Zero (`go.mod` is clean)** | Java, ZooKeeper/KRaft | C++ libs |
| **CGO / `librdkafka` Required**| **No (Pure Go)** | N/A | N/A |

---

## 🌐 Components & Ports

All services are baked into a single binary and boot in milliseconds:

| Component | Port | Description |
| :--- | :---: | :--- |
| **Kafka Wire Protocol** | `9092` / `29092` | Core broker listener (`PLAINTEXT` / internal network) |
| **Embedded Web UI** | `8080` | Multi-zone SPA (Kafka, Fleet, MQTT) with live tail, inspector & settings |
| **Schema Registry** | `8081` | Confluent API-compatible subset: AVRO/JSON syntax-checked, PROTOBUF rejected |
| **HTTP REST Proxy** | `8082` | Publish and consume messages over standard HTTP POST/GET |
| **MQTT 3.1.1 Bridge** | `1883` | Direct IoT sensor telemetry bridged to Kafka topics |
| **Fleet Controller** | `18090` (proxy `:8080`) | Remote host onboarding & container lifecycle (disabled by default; loopback/proxied via `:8080`) |
| **TLS / SSL** | `9093` | Optional encrypted listener |
| **Prometheus Metrics** | `8080` `/metrics` | Standard Prometheus metric exporter |
| **Health Probes** | `8080` `/healthz` | Kubernetes / Docker liveness and readiness endpoints |
---

## 🚀 Quick Start

### Option 1: Run with Docker Compose (Recommended)

```sh
# Clone & run the minimal single-container stack
docker compose -f docker-compose.minimal.yml up -d --build
```

Access the **PocketKafka Web UI** immediately at: **<http://localhost:8080>**

To stop:
```sh
docker compose -f docker-compose.minimal.yml down
```

---

### Option 2: Build and Run from Source (Go 1.26+)

```sh
# 1. Build binary
go build -o bin/pocketkafka ./cmd/server
go build -o bin/pkctl ./cmd/kctl               # Admin CLI tool
go build -o bin/fleet-agent ./cmd/fleet-agent   # Optional Fleet host agent
go build -o bin/client-example ./cmd/client-example
# 2. Start the PocketKafka broker
./bin/pocketkafka -config config/config.yaml
```

---

## 🛠️ Admin CLI (`pkctl`)

PocketKafka comes with a powerful zero-dependency CLI tool:

```sh
# Topic management
./bin/pkctl -b localhost:9092 topics
./bin/pkctl -b localhost:9092 topics -o json                    # Output JSON for CI/CD
./bin/pkctl -b localhost:9092 create my-topic -p 3
./bin/pkctl -b localhost:9092 delete my-topic

# Produce & Consume
./bin/pkctl -b localhost:9092 produce my-topic "hello pocketkafka"
./bin/pkctl -b localhost:9092 consume my-topic -g worker-group -n 5

# Consumer Groups & Lag Monitoring
./bin/pkctl -b localhost:9092 groups                            # View per-partition lag
./bin/pkctl -b localhost:9092 offset get --group worker-group --topic my-topic
./bin/pkctl -b localhost:9092 offset reset --group worker-group --topic my-topic --to-earliest

# Schema Registry Operations (:8081)
./bin/pkctl schema list
./bin/pkctl schema register --subject orders-value --file ./schemas/order.avsc
```

---

## 💻 Client SDK & Multi-Language Usage

PocketKafka implements the standard Apache Kafka binary wire protocol (compatible with Kafka 0.11 through 2.4, RecordBatch v2 format). Client options across programming languages:
- **Native Kafka Protocol (`localhost:9092`)**: Direct TCP streaming with standard Kafka client libraries (in-tree Go SDK and `librdkafka` legacy mode verified; full external matrix tracked in [`test/interop/compatibility.yaml`](test/interop/compatibility.yaml)).
- **HTTP REST Proxy (`localhost:8082`)**: Universal publish/consume over standard HTTP (`curl`, `fetch`, `requests`) without requiring any Kafka client library.
- **MQTT Bridge (`localhost:1883`)**: Direct sensor and IoT device streaming.

### 1. Go (Native SDK)

```go
package main

import (
    "context"
    "log"
    "github.com/Yukaz0/pocketkafka/pkg/client"
)

func main() {
    // 1. High-throughput batch producer
    producer, err := client.NewProducer(client.ProducerConfig{
        Brokers: []string{"localhost:9092"},
        Acks:    client.AcksAll,
    })
    if err != nil { log.Fatal(err) }
    defer producer.Close()

    offset, _ := producer.SendSync(context.Background(), &client.Message{
        Topic: "orders",
        Key:   []byte("order-101"),
        Value: []byte(`{"item":"laptop","price":1200}`),
    })
    log.Printf("Produced to offset: %d", offset)

    // 2. Rebalancing consumer group
    cg, _ := client.NewConsumerGroup(client.ConsumerGroupConfig{
        Brokers: []string{"localhost:9092"},
        GroupID: "order-processors",
        Topics:  []string{"orders"},
    })
    defer cg.Close()

    cg.Start(context.Background(), func(msg *client.Message) error {
        log.Printf("Received: %s", string(msg.Value))
        return nil // Auto-commit offset
    })
}
```

### 2. Python (`kafka-python` or `confluent-kafka`) — planned, not yet verified

> External-client support is tracked in [`test/interop/compatibility.yaml`](test/interop/compatibility.yaml). Snippets below are the target API; only the Go SDK and REST/MQTT paths are verified today.

```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('orders', b'{"item":"phone","price":800}')
```

### 3. Node.js (`kafkajs`) — planned, not yet verified

```javascript
const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();
await producer.connect();
await producer.send({ topic: 'orders', messages: [{ value: 'hello from node' }] });
```

### 4. HTTP REST Proxy (Any language / cURL)

```sh
# Publish without any Kafka library
curl -X POST http://localhost:8082/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "user-1", "value": {"event": "login"}}'
```

### 5. MQTT 3.1.1 Bridge

An MQTT publish to `sensors/<device>/<metric>` is stored in the Kafka topic
`mqtt-<device-group>`, keyed by `<device>`. The original MQTT topic is preserved
on the record as the `mqtt-topic` header, so subscribers receive the topic they
published to and subscription filters (`+`, `#`) are matched against it instead
of the derived Kafka topic name. QoS 0 and QoS 1 are acknowledged (`PUBACK`);
QoS 2 performs the `PUBREC`/`PUBREL`/`PUBCOMP` handshake. Password checks avoid
timing side channels, and `SUBSCRIBE` never creates a topic for a principal that
only holds Read permission.

---

## 🎨 Embedded Web UI

PocketKafka ships a modern, zero-build, native ES modules Single-Page Application (SPA) at `http://localhost:8080`, served directly from the Go binary via `//go:embed`. No Node.js, no bundler/build step, no npm packages, and no external CDN dependencies.

### Multi-Zone Architecture
The topbar features an instant zone switcher separating the application into three isolated operational domains:
- **`[ Kafka ]` Control Center**: Core broker operations, topics, consumer groups, partition data browser, schemas, metrics, health, producer studio, broker logs, and settings.
- **`[ Fleet ]` Fleet Studio**: Multi-node infrastructure manager. Onboard remote hosts via single-use HMAC enrollment tokens, track container states (Docker), dispatch async lifecycle jobs, and view append-only audit trails.
- **`[ MQTT ]` MQTT Studio**: IoT gateway management. Inspect active client connections with keep-alive trackers, publish test payloads directly to Kafka-bridged topics, test topic wildcard matchers (`+`/`#`), and monitor ingress throughput.

### Key Capabilities
- **Flat 10-Item Sidebar**: Clean, unbloated sidebar in the Kafka zone (*Overview*, *Topics*, *Data Browser*, *Consumer Groups*, *Metrics*, *Health*, *Producer Studio*, *Schemas*, *Broker Logs*, *Settings*) with collapsible rail (56 px collapsed / 224 px expanded).
- **Consolidated Settings**: Settings view organized into clear sub-tabs: *General*, *Clusters*, *Connections* (ingress surfaces & endpoint details), *Storage*, and *Security & ACL*.
- **Data Browser**: WebSocket live-tail with message inspector (collapsible JSON tree, syntax-highlighted), topic explorer drawer, and message diff viewer.
- **Topics & Consumer Groups**: Partition detail, log-end/high-watermark offsets, commit history, and per-partition lag visualization.
- **Isolated Sign-In Screen**: When authentication is enabled, non-authenticated requests render a dedicated, centered login card with zero dashboard chrome or leaking menus.
- **Dynamic Asset Cache-Buster**: Query parameters (`v=${version}`) automatically hash embedded frontend files on build to guarantee fresh browser caches across upgrades.
- **Global Broker Selector**: Switch between local broker and registered Kafka/PocketKafka peer clusters.
- **Dark / Light Theme Toggle**: Native theme switcher persisted in `localStorage`.
- **Responsive**: Full mobile layout with drawer navigation at narrow viewports.
---

## 🚀 Fleet Agent (Host & Container Management)

PocketKafka includes an optional, zero-dependency Fleet Agent daemon (`cmd/fleet-agent`) for monitoring and controlling Docker containers across remote or local nodes directly from the Web UI Fleet zone.

### 1. Run with Docker Compose
Start the agent alongside the broker on a local Docker host:
```sh
# 1. Generate enrollment token from Web UI: Fleet -> Servers -> Enroll Server
# 2. Run with fleet profile:
FLEET_ENROLL_TOKEN="<token>" docker compose --profile fleet up -d fleet-agent
```

### 2. Standalone Container
```sh
docker build -f Dockerfile.fleet-agent -t pocketkafka-fleet-agent:latest .
docker run -d --name fleet-agent --restart always \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v fleet-agent-state:/data \
  -e FLEET_SERVER_URL=http://<controller-host>:8080 \
  -e FLEET_ENROLL_TOKEN="<token>" \
  pocketkafka-fleet-agent:latest
```

### 3. Windows Usage
- **Option A (Docker Desktop WSL 2 - Recommended)**: Run via the Docker container command above.
- **Option B (Native Binary `fleet-agent.exe`)**:
  Compile with `GOOS=windows GOARCH=amd64 go build -o fleet-agent.exe ./cmd/fleet-agent`. Enable *"Expose daemon on tcp://localhost:2375 without TLS"* in Docker Desktop settings, then run:
  ```cmd
  fleet-agent.exe -server http://<controller-host>:8080 -token <token> -docker-socket tcp://127.0.0.1:2375
  ```

Detailed operation, systemd setup, and troubleshooting are available in [`docs/FLEET_RUNBOOK.md`](docs/FLEET_RUNBOOK.md).

---

## 📁 Project Layout

```
pocketkafka/
├── cmd/
│   ├── server/             # All-in-one PocketKafka broker entrypoint
│   ├── kctl/               # Admin CLI management tool (pkctl)
│   ├── fleet-agent/        # Remote host daemon for container management
│   └── client-example/     # Bundled zero-dependency SDK demo
├── pkg/
│   ├── protocol/           # Hand-written Kafka wire protocol codecs (Zero deps)
│   └── client/             # Zero-dependency Go client SDK
├── internal/
│   ├── server/             # TCP socket server, dispatcher, SASL/SCRAM, TLS
│   ├── handler/            # Core Kafka API handlers (0..42) + dispatcher
│   ├── storage/            # Commit log, sparse index, compaction, recovery
│   ├── tier/               # S3 tiered storage engine & object offloader
│   ├── coordinator/        # Consumer group coordinator & offset snapshot/WAL
│   ├── schemaregistry/     # Embedded Schema Registry subset (AVRO/JSON)
│   ├── gateway/            # HTTP REST proxy & MQTT 3.1.1 bridge
│   ├── fleet/              # Fleet controller, token enrollment & job dispatcher
│   ├── authz/              # Shared ACL store & Authorizer engine
│   ├── httpauth/           # HTTP authentication & session middleware
│   ├── credential/         # Password hashing & secret credential management
│   ├── ratelimit/          # Token-bucket rate limiter & brute-force defense
│   ├── audit/              # Append-only security audit log & event exporter
│   ├── netguard/           # SSRF protection & CIDR socket validator
│   ├── metrics/            # Prometheus metrics registry & collectors
│   ├── logger/             # Structured slog handler & log buffer
│   ├── atomicfile/         # Durable temp-file + fsync + atomic rename helper
│   ├── web/                # Web UI HTTP server, API endpoints & WebSocket proxy
│   └── config/             # YAML & 12-factor loader, normalizer, and validator
├── web/                    # Embedded frontend assets (//go:embed dist/*)
│   ├── web.go              # Embedded filesystem root accessor
│   └── dist/               # Native ES modules Single-Page Application (Zero build)
│       ├── index.html      # Shell template with dynamic cache-buster
│       ├── css/style.css   # Unified dark/light design system stylesheet
│       └── js/             # Modular application architecture
│           ├── app.js      # Bootstrap, event lifecycle & theme init
│           ├── state.js    # Central reactive state & SVG icon registry
│           ├── api.js      # HTTP REST & WebSocket streaming client
│           ├── router.js   # Hash-based SPA router
│           ├── render.js   # Layout dispatcher & DOM renderer
│           ├── refresh.js  # Live polling coordinator
│           └── zones/      # Domain-specific UI views
│               ├── kafka.js # Kafka broker views & detail drawers
│               ├── fleet.js # Fleet servers, tokens, containers & jobs
│               └── mqtt.js  # MQTT Studio, client tracker & publisher
├── config/                 # Reference server configurations (default, docker, secure)
├── deploy/                 # Production deployment configs (Grafana dashboards)
├── docs/                   # Public guides (Runbooks, Security, Contributing, Roadmap)
├── test/
│   ├── interop/            # External client compatibility matrix & runners
│   └── bench/              # Reproducible benchmark runner
├── Dockerfile              # Ultra-lightweight multi-stage broker container (< 29MB)
├── Dockerfile.fleet-agent  # Standalone fleet-agent container
├── docker-compose.yml      # Multi-service stack (Broker, Postgres, Fleet Agent)
├── go.mod                  # 100% clean - zero external requires
└── LICENSE                 # MIT License
```

---

## 🧪 Testing

PocketKafka includes extensive unit and integration tests:

```sh
# Run all tests
go test -v ./...
```

Tests cover:
- Binary protocol serialization/deserialization & Castagnoli CRC-32C.
- Golden byte fixtures for every advertised API version (`pkg/protocol/golden_test.go`).
- Fuzz targets for the frame, record-batch, compression and request decoders.
- Commit log segment rolling, sparse index lookups, crash recovery & log compaction.
- Idempotent producer sequence validation.
- Consumer group rebalance state machine & offset commits, plus offset WAL/retention.
- Tiered storage against mock S3 server.
- End-to-end tests through the bundled SDK, including transactional fail-closed and restart recovery.

### Client compatibility

The `pocketkafka-native` client is covered by `go test ./internal/e2e` on every
PR. The external matrix — `kcat`/librdkafka (modern and the `0.9.0` fallback),
KafkaJS and franz-go — lives in [`test/interop`](test/interop/README.md) and is
driven by `test/interop/run.sh` (nightly / on demand, requires Docker). The
auditable list of what is supported vs planned is
[`test/interop/compatibility.yaml`](test/interop/compatibility.yaml); treat that
file, not this README, as the source of truth.

---

## 🔒 Production Hardening

Behaviour that matters when you point real workloads at the broker:

### Exposure

- **Everything binds loopback by default.** `listeners.*`, `web.listen`,
  `schema_registry.listen`, `gateway.listen` and `mqtt.listen` default to
  `127.0.0.1`. A non-loopback bind while `security.enabled=false` is *refused at
  startup*; the message names the field, and `security.allow_insecure_public=true`
  is the explicit acknowledgement for a deliberately open development broker
  (that is what the shipped compose files set).
- **`security.enabled=true` means default-deny** on every ingress: Kafka, REST
  proxy, Schema Registry, MQTT and the dashboard. `security.super_users` names
  the principals that bypass the ACLs, so a fresh install has somebody who can
  administer them. With no super user and an empty ACL file, everyone is denied
  (by design).
- **Dashboard routes are authorized one by one.** Each route names the operation
  and resource it needs (`GET /topics/{t}/messages` → `Read topic:{t}`,
  `truncate` → `Admin topic:{t}`, `/acls`, `/audit`, `/logs`, `/config` →
  `Admin cluster`). A route with no rule is denied, and every handler that
  returns a collection filters it: `/topics`, `/groups`, the group detail rows
  and the group offset snapshot (including its `lag`/offset entries) omit
  resources the caller may not `Describe`. `Describe` is implied by
  `Read`/`Write`/`Admin`, so ACLs written before it existed keep working.
- **A cluster grant is visibility, not payload access.** A `Describe cluster`
  grant (implied by any cluster-level `Read`/`Write`/`Admin`) satisfies
  `Describe` on every resource, so an operator who administers the broker sees
  the topics and groups it holds — their partitions, log-end offsets and lag.
  It does *not* grant `Read` on a topic (message contents) or on a group
  (membership/offsets), and it does not grant `Write`/`Admin` on a topic or
  group: those stay strictly resource-scoped, so a cluster admin still needs an
  explicit group grant to export or reset that group's offsets.
  A group grant is likewise never a topic grant: group endpoints show only the
  topics that principal may describe, and resetting/importing offsets for an
  invisible topic is refused (with the same answer as an unknown topic, so the
  endpoint cannot be used to probe for existence).
- **The cluster health token is read-only in practice**: it reaches
  `/api/v1/health/overview` and nothing else. It carries no principal, and an
  empty principal is never authorized. A wildcard `Admin` ACL rule now requires
  `security.allow_wildcard_admin=true`.
- **pprof is opt-in and loopback-only.** Set `KAFKA_PPROF_LISTEN` to enable it;
  a non-loopback address additionally needs `KAFKA_PPROF_ALLOW_PUBLIC=true`, and
  the handlers are served from a dedicated mux instead of `DefaultServeMux`.

### Authentication

- **Sessions expire and can be revoked.** A session has an absolute
  (`web.session_ttl_minutes`) and an idle (`web.session_idle_minutes`) lifetime,
  logout deletes the server-side record, and a user removed from the config can
  no longer use an old cookie. The cookie is `HttpOnly`, `SameSite=Strict`, and
  `Secure` + `__Host-` prefixed when the browser's connection is HTTPS
  (behind a TLS-terminating proxy, list the proxy in `web.trusted_proxies`).
- **Credential guessing is delayed.** Login, HTTP Basic, SASL and MQTT CONNECT
  share an exponential backoff keyed on the source *address* (not address:port,
  so reconnecting does not reset it), plus a per-account counter for the login
  form; a Kafka connection is refused at the door while its source is in
  backoff, and one that fails three SASL attempts is closed. Everything is
  audit-logged, and `pocketkafka_auth_failures_total` /
  `pocketkafka_authz_denials_total` expose the counts on `/metrics`. Note the
  consequence of a per-source rule: clients sharing one NAT address (or one mTLS
  identity's address) wait out the same backoff, which is bounded at 30s
  (login: 5 attempts, 2s doubling to 5min).
- **Sessions are scoped to the connection scheme.** A session issued over HTTPS
  (cookie `__Host-`, `Secure`) is not honoured over plain HTTP and vice versa, so
  a login is only valid on the scheme that issued it. Behind a TLS-terminating
  proxy, list the proxy in `web.trusted_proxies` or the dashboard stays on the
  plain cookie class.
- **Passwords need not be in the file.** `security.users[]` accepts `password`,
  `password_file`, `password_hash` (PBKDF2, `pocketkafka hash-password`) or
  `scram_verifier` (SCRAM, `pocketkafka scram-verifier`). Every comparison is
  constant-time. `${ENV_VAR}` is expanded anywhere in the YAML, and an unset
  variable is a startup error rather than an empty secret. A default or
  too-short password stops startup once security is on.
- **PLAIN needs TLS.** `security.require_tls_for_plain` (default true) keeps
  SASL/PLAIN, HTTP Basic and MQTT credentials off unencrypted connections; PLAIN
  is not even offered on a plaintext connection, so a client is steered to
  SCRAM. SCRAM-SHA-256/512 use a per-user salt derived from the server secret
  (stable across restarts) and answer uniformly, so the mechanism cannot be used
  to enumerate users.
- **Stored cluster credentials are sealed with AES-256-GCM** under a PBKDF2
  derived key (`web.secrets_key`/`KAFKA_SECRETS_KEY`, or a generated 0600 key
  file next to the data). The test endpoint only reuses a stored token or SASL
  password for the exact target it was stored for.

### Transport, traffic and trail

- **TLS covers every surface** from one certificate:
  `security.tls.{enabled,listen}` for the Kafka `SSL://` listener,
  `security.tls.http` for the dashboard/Schema Registry/REST proxy,
  `security.tls.mqtt` for the MQTT bridge. `min_version` selects TLS 1.2
  (AEAD-only cipher allow-list) or 1.3, and `client_ca_file` turns on mTLS, where
  the verified certificate subject *is* the connection principal. A client that
  arrives over TLS is never advertised a plaintext listener.
- **Frames are bounded before authentication.** `network.pre_auth_max_request_bytes`
  (64 KiB) applies until SASL succeeds, so an anonymous client cannot make the
  broker allocate the 100 MB it accepts from a producer.
- **WebSockets check Origin and Host.** The handshake requires `Origin` to match
  `Host` and the host to be an IP literal, `localhost`, or a name listed in
  `web.allowed_hosts` (which is what stops DNS rebinding). Client frames must be
  masked, control frames stay within 125 bytes, data frames within 64 KiB.
- **Outbound monitoring fetches are guarded.** Peer requests never follow
  redirects, every dial refuses link-local/metadata addresses after resolution
  (DNS rebinding included), and errors are uniform.
- **The audit trail is durable.** Every security-relevant event (login
  success/failure, logout, denials, topic create/delete/truncate/compact,
  offset reset, import, ACL and cluster edits, token issue/revoke) is written as
  a structured log line and, with `web.audit_log`, appended to a rotated 0600
  JSON-lines file. The dashboard's in-memory view is only a view.
- **Configuration is validated before any listener opens.** `config.Load`
  normalizes and validates ports, buffer/timeout limits, storage backend, flush
  policy, TLS material, credential strength, tiered-storage endpoint scheme and
  security settings; a bad value stops startup naming the field.
- **Durability is explicit.** `storage.flush_policy` selects `none` (OS decides),
  `interval` (periodic fsync, default), or `request` (fsync before the produce
  ack). Independent of that, `acks=-1` syncs when
  `storage.sync_on_acks_all=true`. Under `acks=1` a record is in the local page
  cache, not necessarily on disk.
- **Transactional APIs fail closed.** `AddPartitionsToTxn`, `AddOffsetsToTxn`
  and `EndTxn` (keys 24-26) are not advertised and answer `UNSUPPORTED_VERSION`;
  the client SDK returns `ErrTransactionsUnsupported` rather than pretending a
  commit succeeded. On a single node `acks=-1` means durable, not replicated.
- **Schema Registry is an API-compatible subset.** Schemas and IDs persist across
  restarts and `AVRO`/`JSON` payloads are syntax-checked, but full
  backward/forward compatibility checking is not implemented; `PROTOBUF` is
  rejected until a validator exists.

Authorization has a table-driven test in the local (unpublished) suite: it walks
every dashboard route with a scoped reader, a topic admin, a cluster admin and the
cluster token, so a route that loses its rule fails that run.

---

## 📜 License

PocketKafka is open-source software licensed under the [MIT License](LICENSE).

Created by **Muhammad Jafar Ikbal (Yukaz0)** — jaffrhere23@gmail.com.
Fork/derivatif wajib mempertahankan atribusi ini (lihat [AUTHORS.md](docs/AUTHORS.md)).
