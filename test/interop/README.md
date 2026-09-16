# Interoperability tests

This directory verifies that real Kafka clients — not just the in-tree SDK — can
talk to PocketKafka. The point is to exercise independent encoder/decoder
implementations, so a bug shared between our own client and server cannot hide.

## What is checked

`compatibility.yaml` is the source of truth for the client matrix and the
protocol versions the broker advertises. Each client must complete the
operations listed there (metadata, create topic, produce, fetch, join/sync
group, commit/fetch offset, reconnect, restart/resume).

| Client | Runner | Status |
|---|---|---|
| `pocketkafka-native` | `go test ./internal/e2e` | runs on every PR |
| `kcat` / librdkafka (modern) | `kcat.sh` (Docker) | nightly / manual |
| `librdkafka` legacy (`broker.version.fallback=0.9.0`) | `kcat.sh <addr> legacy` | nightly / manual |
| `kafkajs` | `kafkajs/` (Docker) | nightly / manual |

## Running locally

```bash
test/interop/run.sh                    # native client only
INTEROP_DOCKER=1 test/interop/run.sh   # full matrix (needs Docker)
```

The script builds the broker, starts it on `127.0.0.1:19092` with a throwaway
data dir, waits for the port to accept connections, runs the checks, and cleans
up on exit. Override the port with `INTEROP_PORT`.

## Adding a client

1. Add an entry to `compatibility.yaml` with `status: planned`.
2. Add a runner script (or `Dockerfile`) under this directory.
3. Wire it into `run.sh` and flip the status to `supported` once it passes in CI.

Do not mark a client `supported` before its checks are green — the manifest is
the claim we make to users.
