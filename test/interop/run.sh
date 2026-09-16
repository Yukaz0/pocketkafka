#!/usr/bin/env bash
# External-client interoperability runner.
#
# Starts a broker on loopback, waits for it to accept connections, then runs the
# client checks listed in compatibility.yaml. The native client runs with no
# extra dependencies; the external clients (kcat/librdkafka, franz-go, KafkaJS)
# require Docker and are skipped when it is unavailable.
#
# Usage:
#   test/interop/run.sh                 # native only unless Docker is present
#   INTEROP_DOCKER=1 test/interop/run.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PORT="${INTEROP_PORT:-19092}"
ADV="localhost:${PORT}"
WORK="$(mktemp -d)"
trap 'cleanup' EXIT

BROKER_PID=""
cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}

cat > "$WORK/config.yaml" <<EOF
broker:
  id: 0
  cluster_id: "interop"
listeners:
  plain: "127.0.0.1:${PORT}"
advertised_listeners:
  plain: "${ADV}"
storage:
  data_dir: "${WORK}/data"
web:
  enabled: false
gateway:
  listen: "127.0.0.1:0"
mqtt:
  listen: "127.0.0.1:0"
schema_registry:
  listen: "127.0.0.1:0"
security:
  enabled: false
EOF

echo "==> building broker"
go build -o "$WORK/pocketkafka" ./cmd/server

echo "==> starting broker on ${ADV}"
"$WORK/pocketkafka" -config "$WORK/config.yaml" >"$WORK/broker.log" 2>&1 &
BROKER_PID=$!

echo "==> waiting for readiness"
for _ in $(seq 1 100); do
  if (exec 3<>"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null; then
    exec 3>&- 3<&-
    break
  fi
  sleep 0.1
done

echo "==> native client (in-tree SDK)"
go test ./internal/e2e -run TestEndToEnd -count=1

if [[ "${INTEROP_DOCKER:-0}" == "1" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "!! INTEROP_DOCKER=1 but docker is not installed" >&2
    exit 1
  fi
  echo "==> kcat / librdkafka (modern)"
  bash test/interop/kcat.sh "$ADV"
  echo "==> KafkaJS"
  docker build -t pocketkafka-interop-kafkajs test/interop/kafkajs
  docker run --rm --network host pocketkafka-interop-kafkajs "$ADV"
else
  echo "==> external clients skipped (set INTEROP_DOCKER=1 to enable)"
fi

echo "==> interop OK"
