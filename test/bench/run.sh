#!/usr/bin/env bash
# Reproducible benchmark runner. Records the command, dataset shape, hardware,
# and configuration alongside the output so results can be compared honestly.
#
# Usage: test/bench/run.sh [benchtime]   (default 1s)
set -euo pipefail

BENCHTIME="${1:-1s}"
OUT="${OUT:-test/bench/results}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT"
REPORT="$OUT/bench-$STAMP.txt"

{
  echo "# PocketKafka benchmark"
  echo "date:    $STAMP"
  echo "commit:  $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "go:      $(go version)"
  echo "kernel:  $(uname -srmo)"
  echo "cpu:     $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | xargs || echo unknown)"
  echo "benchtime: $BENCHTIME"
  echo
  echo "# benchstat-compatible output"
} > "$REPORT"

go test ./pkg/protocol -run=XXX -bench=. -benchtime="$BENCHTIME" -benchmem | tee -a "$REPORT"
go test ./internal/storage -run=XXX -bench=. -benchtime="$BENCHTIME" -benchmem | tee -a "$REPORT"

echo
echo "wrote $REPORT"
