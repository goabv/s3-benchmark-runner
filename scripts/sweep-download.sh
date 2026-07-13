#!/usr/bin/env bash
# DOWNLOAD sweep: seed the configured object sizes (skipping any that already
# exist), then benchmark download throughput across the whole size curve.
# Run ON THE EC2 INSTANCE, in the same region as the bucket.
#
# Settings come from bench.config.json (shared keys + the "download" section).
# Override per-run with env vars: WORKERS, CONCURRENCY, ITERATIONS, WARMUP, PART_SIZE.
#
# Usage: ./scripts/sweep-download.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# libuv thread pool serves async fs writes (file mode + fileAsync) and dns.lookup.
# Default is 4, which serializes writes across many workers; raise it so async
# disk writes actually run in parallel. Overridable: UV_THREADPOOL_SIZE=128 ...
export UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-64}"

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="results/download-sweep-${STAMP}.json"

echo ">> Seeding configured sizes (bench.config.json), skipping existing objects"
node src/upload-test-data.js \
  ${PART_SIZE:+--part-size "$PART_SIZE"}

echo ">> DOWNLOAD benchmarking configured sizes -> ${OUT}"
node src/benchmark.js \
  ${WORKERS:+--workers "$WORKERS"} \
  ${CONCURRENCY:+--concurrency "$CONCURRENCY"} \
  ${ITERATIONS:+--iterations "$ITERATIONS"} \
  ${WARMUP:+--warmup "$WARMUP"} \
  --out "$OUT"

echo ">> Done. JSON: ${OUT}"
