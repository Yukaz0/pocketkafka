# pocketkafka: multi-stage build (< 30MB, zero third-party deps).
#
# Both base images are pinned by tag *and* digest: the tag keeps the file
# readable, the digest makes the build reproducible and immune to a re-tagged
# or compromised upstream image. Digests are the multi-arch (OCI index) digests
# so the same file builds for linux/amd64 and linux/arm64.
# Dependabot (docker) bumps these weekly.

# Stage 1: build the broker binary
FROM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS builder

# Cross-compilation targets, injected by `docker buildx build --platform ...`.
# Defaults keep plain `docker build` working on amd64.
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /app
RUN apk add --no-cache git

# Copy module files first for layer caching (no external deps -> fast).
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-amd64} \
    go build \
    -trimpath \
    -buildvcs=true \
    -ldflags="-s -w -X main.version=${VERSION:-dev}" \
    -o /app/bin/pocketkafka ./cmd/server

# Stage 2: minimal runtime image
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

WORKDIR /app
# No extra probing tools in the runtime image: the healthcheck below uses the
# broker's own `healthcheck` subcommand.
RUN apk add --no-cache ca-certificates tzdata su-exec

COPY --from=builder /app/bin/pocketkafka /usr/local/bin/pocketkafka
COPY config/config.yaml /etc/pocketkafka/config.yaml
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# The broker runs as an unprivileged user: it only needs ports above 1024 and its
# own data directory. The entrypoint takes ownership of an existing volume (one
# created by an earlier image is owned by root) and then drops privileges, so
# upgrading keeps working without touching the data.
RUN addgroup -g 10001 -S pocketkafka \
 && adduser -u 10001 -S -G pocketkafka -h /var/lib/pocketkafka pocketkafka \
 && mkdir -p /var/lib/pocketkafka/data \
 && chown -R pocketkafka:pocketkafka /var/lib/pocketkafka \
 && chmod +x /usr/local/bin/docker-entrypoint.sh

# Kafka TCP listeners + Embedded Web UI (8080) + Schema Registry (8081) +
# REST Proxy (8082) + MQTT Bridge (1883)
EXPOSE 9092 29092 8080 8081 8082 1883

VOLUME ["/var/lib/pocketkafka/data"]

ENV KAFKA_DATA_DIR=/var/lib/pocketkafka/data

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["--config", "/etc/pocketkafka/config.yaml"]

# Built-in healthcheck: `pocketkafka healthcheck --addr <host:port>` exits 0 when
# the broker is serving, non-zero otherwise (see cmd/server). This replaces the
# old `nc`/`wget` probe so the runtime image needs no extra packages.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/usr/local/bin/pocketkafka", "healthcheck", "--addr", "127.0.0.1:9092"]
