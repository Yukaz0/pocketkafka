# pocketkafka: multi-stage scratch build (< 25MB, zero external deps).
# See docs/DEPLOYMENT_AND_OPS.md.

# Stage 1: build the broker binary
FROM golang:1.26-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git

# Copy module files first for layer caching (no external deps -> fast).
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=1.0.0" \
    -o /app/bin/pocketkafka ./cmd/server

# Stage 2: minimal runtime image
FROM alpine:3.20

WORKDIR /app
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd

COPY --from=builder /app/bin/pocketkafka /usr/local/bin/pocketkafka
COPY config/config.yaml /etc/pocketkafka/config.yaml

# Kafka TCP listeners + Embedded Web UI (8080) + Schema Registry (8081) +
# REST Proxy (8082) + MQTT Bridge (1883)
EXPOSE 9092 29092 8080 8081 8082 1883

VOLUME ["/var/lib/pocketkafka/data"]

ENV KAFKA_DATA_DIR=/var/lib/pocketkafka/data

ENTRYPOINT ["pocketkafka"]
CMD ["--config", "/etc/pocketkafka/config.yaml"]

# Built-in healthcheck (Fitur 19): the TCP listener must accept connections and
# the embedded web UI must answer /healthz.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD nc -z localhost 9092 && wget -qO- http://localhost:8080/healthz || exit 1
