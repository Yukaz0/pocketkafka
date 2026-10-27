# Contributing to PocketKafka

Thank you for your interest in contributing to **PocketKafka**! We welcome bug reports, feature proposals, documentation improvements, and pull requests.

---

## 🛠️ Development Setup

PocketKafka requires **Go 1.26+** and has **zero third-party dependencies**.

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

### Quality gates

CI runs the same commands you can run locally. Please make sure they pass before
opening a PR:

```bash
test -z "$(gofmt -l .)"          # formatting
go build ./...
go vet ./...
go test ./...
go test -race ./...
go test ./pkg/protocol -run=Fuzz -count=1      # fuzz seed corpus
go test -coverpkg=./... -coverprofile=coverage.out ./...
go tool cover -func=coverage.out | tail -1     # total coverage must stay >= 51%
```

Wire-format changes additionally need updated golden fixtures
(`pkg/protocol/golden_test.go`). External client coverage lives in
`test/interop/`; see its README before editing the compatibility matrix.

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
