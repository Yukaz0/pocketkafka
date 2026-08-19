# go-kafka-neu: multi-stage scratch build (< 25MB, zero external deps).
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
    -o /app/bin/go-kafka-neu ./cmd/server

# Stage 2: minimal runtime image
FROM alpine:3.20

WORKDIR /app
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd

COPY --from=builder /app/bin/go-kafka-neu /usr/local/bin/go-kafka-neu
COPY config/config.yaml /etc/go-kafka/config.yaml

# Kafka TCP Plaintext & Internal listeners + metrics placeholder
EXPOSE 9092 29092

VOLUME ["/var/lib/go-kafka/data"]

ENV KAFKA_DATA_DIR=/var/lib/go-kafka/data

ENTRYPOINT ["go-kafka-neu"]
CMD ["--config", "/etc/go-kafka/config.yaml"]
