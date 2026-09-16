#!/usr/bin/env bash
# kcat (librdkafka) interoperability check.
#
# Exercises metadata, produce, consume, and offset listing through librdkafka,
# which is a different codec implementation from the in-tree Go client and so
# catches encoder/decoder bugs the native tests cannot.
#
# Usage: test/interop/kcat.sh <host:port> [legacy]
set -euo pipefail

ADDR="${1:?usage: kcat.sh <host:port> [legacy]}"
LEGACY="${2:-}"
IMAGE="${KCAT_IMAGE:-edenhill/kcat:1.7.1}"
TOPIC="interop-kcat"
GROUP="interop-kcat-group"

KCAT=(docker run --rm --network host "$IMAGE" -b "$ADDR")
if [[ "$LEGACY" == "legacy" ]]; then
  # Force librdkafka onto its 0.9.0 fallback path (ListOffsets v0).
  KCAT=(docker run --rm --network host "$IMAGE" -b "$ADDR"
    -X broker.version.fallback=0.9.0
    -X api.version.request=false)
fi

echo "-- metadata"
"${KCAT[@]}" -L

echo "-- produce"
for i in 1 2 3; do
  printf 'key-%d' "$i" | "${KCAT[@]}" -P -t "$TOPIC" -p 0
done

echo "-- consume from beginning"
sleep 1
out="$("${KCAT[@]}" -C -t "$TOPIC" -p 0 -o beginning -e -c 3)"
echo "$out"
if [[ "$(echo "$out" | wc -l)" -lt 3 ]]; then
  echo "FAIL: expected 3 messages, got: $out" >&2
  exit 1
fi

echo "-- consume with consumer group"
"${KCAT[@]}" -G "$GROUP" "$TOPIC" -e -c 3 >/dev/null

echo "kcat OK"
