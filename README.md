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

PocketKafka is a from-scratch implementation of the Apache Kafka binary wire protocol and a modern commit-log storage engine. It packs a **high-performance Kafka Broker**, an **embedded Control Center UI**, a **Confluent-compatible Schema Registry**, **HTTP REST & MQTT Gateways**, **Log Compaction**, **S3 Tiered Storage**, and **SASL/SCRAM + TLS Security** into a **single static binary under 30MB**.

`go.mod` has **zero third-party dependencies**.

---

## ⚡ Why PocketKafka?

| Feature | 🦘 **PocketKafka** | **Apache Kafka** | **Redpanda** |
| :--- | :---: | :---: | :---: |
| **Language & Runtime** | **Pure Go (Static Binary)** | Java / JVM | C++ (Seastar) |
| **Idle Memory Footprint** | **< 60 MB** | ~1.5 GB+ (JVM) | ~512 MB - 1 GB |
| **Docker Image Size** | **< 29 MB** | ~600 MB+ | ~300 MB |
| **Embedded Web UI** | **Yes (Control Center, :8080)** | ❌ Needs extra container | Optional Console |
| **Built-in Schema Registry** | **Yes (Port 8081)** | ❌ Needs `cp-schema-registry` | Built-in |
| **HTTP REST & MQTT Gateways**| **Yes (:8082 / :1883)** | ❌ Needs separate proxies | ❌ Needs separate proxies |
| **External Dependencies** | **Zero (`go.mod` is clean)** | Java, ZooKeeper/KRaft | C++ libs |
| **CGO / `librdkafka` Required**| **No (Pure Go)** | N/A | N/A |

---

## 🌐 Components & Ports

All services are baked into a single binary and boot in milliseconds:

| Component | Port | Description |
| :--- | :---: | :--- |
| **Kafka Wire Protocol** | `9092` / `29092` | Core broker listener (`PLAINTEXT` / internal network) |
| **Embedded Web UI** | `8080` | Control Center dashboard with grouped navigation, health monitoring, live tail, and message inspector |
| **Schema Registry** | `8081` | Confluent-compatible Avro / Protobuf / JSON schema registry |
| **HTTP REST Proxy** | `8082` | Publish and consume messages over standard HTTP POST/GET |
| **MQTT 3.1.1 Bridge** | `1883` | Direct IoT sensor telemetry bridged to Kafka topics |
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

### 2. Python (`kafka-python` or `confluent-kafka`)

```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('orders', b'{"item":"phone","price":800}')
```

### 3. Node.js (`kafkajs`)

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

PocketKafka ships a single-file SPA at `http://localhost:8080`, served directly from the Go binary via `//go:embed`. No Node.js, no build step, no external fonts or CDN.

- **Grouped sidebar navigation** with collapsible rail (56 px collapsed / 224 px expanded), organized into *Overview*, *Operations*, *Developer Tools*, and *Connections* groups.
- **Overview dashboard**: broker health status, attention items, throughput and consumer lag summary cards with links to detailed views.
- **Health view**: per-broker health report with configurable time window, consumed rate, and attention alerts from `GET /api/v1/health/overview`.
- **Global broker selector**: switch the dashboard between this broker and registered PocketKafka peers or Kafka clusters. Kafka targets expose read-only Overview, Health, Connections, Topics, and Consumer Groups; unsupported features explain why. The Clusters registry remains local.
- **Peer dashboard credentials**: with security/ACLs enabled, a cluster Admin can issue a bearer token bound to an existing user via `POST /api/v1/auth/delegated-tokens`, list ID/principal metadata with `GET /api/v1/auth/delegated-tokens`, and revoke by ID with `DELETE /api/v1/auth/delegated-tokens/{id}`. The raw token is returned once; only its SHA-256 verifier is stored in `<data_dir>/__dashboard_tokens.json` (mode `0600`). Unlike `web.cluster_token`, a delegated token acts only within its principal's ACL grants; the health token remains read-only.
- **Data Browser**: WebSocket live-tail with message inspector (collapsible JSON tree, syntax-highlighted), topic explorer panel, and message diff.
- **Topics & Consumer Groups**: partition detail, log-end/high-watermark offsets, per-partition lag visualization.
- **Producer Studio & Dev Studio**: compose and inject test records; experiment with schema payloads.
- **Topology map**: SVG-rendered broker-to-topic-to-consumer-group graph.
- **Metrics**: message throughput charts and MQTT bridge status.
- **Broker Logs**: filterable log viewer by severity level.
- **Security & ACL**: manage SASL users and ACL rules.
- **Dark / Light theme toggle** with `localStorage` persistence.
- **Responsive**: full mobile layout with hamburger drawer and slide-out topic explorer at narrow viewports.

---

## 📁 Project Layout

```
pocketkafka/
├── cmd/
│   ├── server/             # All-in-one PocketKafka broker entrypoint
│   ├── kctl/               # Admin CLI management tool (pkctl)
│   └── client-example/     # Bundled SDK demo
├── pkg/
│   ├── protocol/           # Hand-written Kafka wire protocol codecs (Zero deps)
│   └── client/             # Zero-dependency Go client SDK
├── internal/
│   ├── server/             # TCP socket server, dispatcher, SASL/SCRAM, TLS
│   ├── handler/            # Core Kafka API handlers (0..42) + dispatcher
│   ├── storage/            # Commit log, sparse index, compaction, S3 tiered storage
│   ├── coordinator/        # Consumer group coordinator, offsets (snapshot + WAL)
│   ├── schemaregistry/     # Embedded Confluent-compatible subset, persisted
│   ├── gateway/            # HTTP REST proxy & MQTT 3.1.1 bridge
│   ├── authz/              # Shared ACL store + Authorizer (all ingresses)
│   ├── httpauth/           # HTTP authentication/authorization middleware
│   ├── atomicfile/         # Durable temp-file + fsync + rename helper
│   ├── web/                # Embedded UI HTTP server & WebSocket live tail
│   └── config/             # YAML & 12-factor loader, normalizer and validator
├── web/                    # Frontend SPA source code & embedded assets
│   └── dist/index.html     # Embedded single-file dashboard
├── test/
│   ├── interop/            # External client compatibility matrix & runners
│   └── bench/              # Reproducible benchmark runner
├── deploy/                 # Docker Compose, Prometheus & Grafana configs
├── Dockerfile              # Ultra-lightweight multi-stage container (< 29MB)
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
