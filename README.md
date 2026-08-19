# go-kafka-neu

An **all-in-one, 100% pure-Go, zero external dependency** Kafka streaming platform.

It is a from-scratch implementation of the Apache Kafka binary wire protocol and
a modern commit-log engine, plus an embedded web UI, schema registry, REST proxy,
MQTT bridge, log compaction, S3/MinIO tiered storage, and SASL/PLAIN + TLS
security. The module's `go.mod` has **no third-party requires**.

## Components & Ports

| Component | Port | Description |
| :--- | :---: | :--- |
| Kafka wire protocol | 9092 / 29092 | Broker listener (`PLAINTEXT` / internal) |
| Embedded Web UI | 8080 | Kadeck-like dashboard, REST API + WebSocket live tail |
| Schema Registry | 8081 | Confluent-compatible REST API |
| REST Proxy | 8082 | Publish/consume messages over plain HTTP |
| MQTT Bridge | 1883 | IoT MQTT 3.1.1 topics bridged to Kafka |
| TLS (SSL) | 9093 | Optional encrypted listener |

The Docker image (`< 29MB`) runs all of the above from a single binary.

## Quick Start

```sh
# build
go build -o bin/server ./cmd/server
go build -o bin/client-example ./cmd/client-example

# run the broker (config/config.yaml)
./bin/server -config config/config.yaml

# in another terminal: publish and consume via the bundled SDK
./bin/client-example -brokers localhost:9092 -topic demo -mode produce -count 5
./bin/client-example -brokers localhost:9092 -topic demo -mode consume -group g1 -time 5s
```

Open the dashboard at <http://localhost:8080> (create topics, explore messages,
produce via the simulator, watch live tail).

## Run with Docker

The recommended way to run go-kafka-neu in Docker is the minimal single-service
compose file (no portainer/kadeck/postgres extras):

```sh
docker compose -f docker-compose.minimal.yml up -d --build
# stop
docker compose -f docker-compose.minimal.yml down
```

This maps all service ports to the host (Kafka 9092/29092, Web UI 8080, Schema
Registry 8081, REST Proxy 8082, MQTT 1883) and persists data in a `kafka_data`
volume.

> **Port conflict note**: go-kafka-neu uses the same ports as Redpanda/Kadeck
> (9092/29092/8080/8081). Do **not** run the host binary and the container at the
> same time, and do **not** run this together with the `~/docker-infra` stack
> (Redpanda + Kadeck) — they will fight over the ports. Pick one.

The default `docker-compose.yml` bundles optional `portainer`, `postgres`, and
`kadeck-web` services; use it only if you want that full stack, and ensure the
extra container names/ports are free.

## Feature Highlights

- **Kafka wire protocol** (`pkg/protocol`): hand-written BigEndian reader/writer,
  frame slicing, RecordBatch v2 codec with Castagnoli CRC-32C, and codecs for the
  core API keys. Advertises conservative non-flexible max versions so standard
  clients (rpk, kcat, franz-go, sarama) interoperate.
- **Storage engine** (`internal/storage`): append-only commit log with sparse
  binary index, segment rolling, retention, crash recovery, **log compaction**
  (`cleanup.policy=compact`), and **S3/MinIO tiered storage** with on-demand fetch.
- **Client SDK** (`pkg/client`): zero-dependency producer (batch/async) and a
  rebalancing consumer group.
- **Group coordinator** (`internal/coordinator`): consumer group state machine and
  offset store.
- **Gateways** (`internal/gateway`): HTTP REST proxy and MQTT bridge.
- **Embedded UI** (`internal/web` + `web/`): REST API, minimal WebSocket live tail,
  and an embedded dark-theme SPA served via `//go:embed`.
- **Schema Registry** (`internal/schemaregistry`): Confluent-compatible endpoints.
- **Security** (`internal/server`): SASL/PLAIN authentication and an optional TLS
  listener.

## Configuration

Configuration lives in `config/config.yaml` and can be overridden with
12-factor environment variables (`KAFKA_*`). Key sections: `broker`, `listeners`,
`storage` (with `retention` and `tiered`), `topics`, `coordinator`, `network`,
`web`, `schema_registry`, `gateway`, `mqtt`, and `security` (SASL users + TLS).
See `docs/` for the full specs.

## Testing

```sh
go test ./...
```

Unit + integration tests cover protocol codec round-trips and CRC, storage
append/read/rolling/recovery/compaction, tiered storage against a mock S3 server,
the REST proxy and MQTT bridge, and SASL authentication. End-to-end interop with a
real Kafka client (franz-go) was verified separately.

## Project Layout

```
cmd/server            # all-in-one engine entrypoint
cmd/client-example    # bundled SDK demo
pkg/protocol          # hand-written Kafka wire protocol
pkg/client            # zero-dependency client SDK
internal/server       # TCP listener, dispatcher, SASL/TLS
internal/handler      # per-API request handlers
internal/storage      # commit log, compaction, tiered storage
internal/coordinator  # consumer group coordinator + offsets
internal/web          # embedded dashboard (REST + WebSocket)
internal/gateway      # REST proxy + MQTT bridge
internal/schemaregistry# embedded schema registry
internal/tier         # S3 client (SigV4) for tiered storage
internal/config       # YAML + env config loader
web/                  # embedded SPA frontend (//go:embed)
docs/                 # technical specifications
```
