<div align="center">

# 🦘 PocketKafka

### *The PocketBase of Event Streaming*

**An ultra-lightweight, all-in-one, 100% pure-Go Kafka streaming platform with zero external dependencies.**

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat-square&logo=go)](https://golang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](LICENSE)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-Zero-success.svg?style=flat-square)](go.mod)
[![Docker Image Size](https://img.shields.io/badge/Docker%20Size-%3C%2029MB-brightgreen.svg?style=flat-square)](#-run-with-docker)
[![Idle Memory](https://img.shields.io/badge/Idle%20RAM-%3C%2060MB-purple.svg?style=flat-square)](#-performance--resource-comparison)

<p align="center">
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-why-pocketkafka">Why PocketKafka?</a> •
  <a href="#-components--ports">Components & Ports</a> •
  <a href="#-embedded-web-ui">Embedded Web UI</a> •
  <a href="#-client-sdk">Client SDK</a> •
  <a href="#-docs">Documentation</a>
</p>

---

</div>

PocketKafka is a from-scratch implementation of the Apache Kafka binary wire protocol and a modern commit-log storage engine. It packs a **high-performance Kafka Broker**, an **embedded Kadeck-grade Web UI**, a **Confluent-compatible Schema Registry**, **HTTP REST & MQTT Gateways**, **Log Compaction**, **S3 Tiered Storage**, and **SASL/SCRAM + TLS Security** into a **single static binary under 30MB**.

`go.mod` has **zero third-party dependencies**.

---

## ⚡ Why PocketKafka?

| Feature | 🦘 **PocketKafka** | **Apache Kafka** | **Redpanda** |
| :--- | :---: | :---: | :---: |
| **Language & Runtime** | **Pure Go (Static Binary)** | Java / JVM | C++ (Seastar) |
| **Idle Memory Footprint** | **< 60 MB** | ~1.5 GB+ (JVM) | ~512 MB - 1 GB |
| **Docker Image Size** | **< 29 MB** | ~600 MB+ | ~300 MB |
| **Embedded Web UI** | **Yes (Kadeck 5 grade, :8080)** | ❌ Needs extra container | Optional Console |
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
| **Embedded Web UI** | `8080` | Kadeck-grade dashboard with WebSocket live tail & message inspector |
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

---

## 🎨 Embedded Web UI (Kadeck 5 Aesthetic)

PocketKafka includes a built-in Single Page Application at `http://localhost:8080` served directly from the Go binary via `//go:embed`:

- **56px Slim Navigation Rail**: Connections, Data Browser, Topics, Schema Registry, Producer Studio, Dark/Light Mode.
- **Realtime Live Tail**: WebSocket streaming with smooth row-entry animations.
- **Deep Message Inspector**: Syntax-highlighted collapsible JSON tree viewer (String, Number, Boolean, Key).
- **Producer Simulator Studio**: Compose and inject test records directly from the browser.
- **Consumer Group Lag Visualizer**: Real-time progress bars indicating unconsumed lag per partition.
- **Topic Partitions & Disk Metrics**: Monitor Log End Offset (LEO) vs High Watermark (HWM).

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
│   ├── handler/            # Core Kafka API handlers (0..42)
│   ├── storage/            # Commit log, sparse index, compaction, S3 tiered storage
│   ├── coordinator/        # Consumer group coordinator, rebalance & offsets
│   ├── schemaregistry/     # Embedded Confluent-compatible schema registry
│   ├── gateway/            # HTTP REST proxy & MQTT 3.1.1 bridge
│   ├── web/                # Embedded UI HTTP server & WebSocket live tail
│   └── config/             # YAML & 12-factor environment loader
├── web/                    # Frontend SPA source code & embedded assets
│   └── dist/index.html     # Embedded single-file dashboard
├── docs/                   # Complete architectural and technical specs
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
- Commit log segment rolling, sparse index lookups, crash recovery & log compaction.
- Idempotent producer sequence validation.
- Consumer group rebalance state machine & offset commits.
- Tiered storage against mock S3 server.
- End-to-end client compatibility with `franz-go`, `kcat`, and official Kafka CLI tools.

---

## 📜 License

PocketKafka is open-source software licensed under the [MIT License](LICENSE).
