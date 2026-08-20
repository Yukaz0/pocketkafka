# Contributing to PocketKafka

Thank you for your interest in contributing to **PocketKafka**! We welcome bug reports, feature proposals, documentation improvements, and pull requests.

---

## 🛠️ Development Setup

PocketKafka requires **Go 1.22+** and has **zero third-party dependencies**.

```bash
# 1. Clone repository
git clone https://github.com/Yukaz0/pocketkafka.git
cd pocketkafka

# 2. Run all unit & integration tests
go test -v ./...

# 3. Build broker binary and CLI tool
go build -o bin/pocketkafka ./cmd/server
go build -o bin/pkctl ./cmd/kctl

# 4. Start local broker
./bin/pocketkafka -config config/config.yaml
```

---

## 📐 Code Guidelines

1. **Zero External Runtime Dependencies**: `go.mod` must remain clean without third-party module requirements.
2. **Deterministic Protocol Serialization**: All binary wire protocol codecs must use BigEndian byte order and adhere to standard Kafka specifications.
3. **Comprehensive Testing**: Any new handler, storage modification, or protocol feature must include automated unit tests under `go test ./...`.

---

## 📋 Pull Request Process

1. Fork the repository and create a feature branch (`git checkout -b feature/my-feature`).
2. Ensure all tests pass (`go test ./...`) and code is formatted (`go fmt ./...`).
3. Commit your changes with clear, concise commit messages.
4. Push to your branch and open a Pull Request.
