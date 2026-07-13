#!/usr/bin/env bash
# UPLOAD sweep: benchmark multipart upload throughput (worker threads) across the
# whole configured size curve. Run ON THE EC2 INSTANCE, in-region.
#
# Settings come from bench.config.json (shared keys + the "upload" section).
# Override per-run with env vars: WORKERS, CONCURRENCY, ITERATIONS, WARMUP, PART_SIZE.
#
# Uploads are FORCED by default (that's the point of an upload sweep) so existing
# objects are re-uploaded. Set FORCE=0 to respect the config's forceUpload instead.
# WARNING: forcing re-uploads every size each iteration (e.g. 30 GiB x iterations).
#
# Usage: ./scripts/sweep-upload.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Raise libuv's thread pool (default 4) so async fs / dns.lookup don't serialize.
# Overridable: UV_THREADPOOL_SIZE=128 ./scripts/sweep-upload.sh
export UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-64}"

FORCE="${FORCE:-1}"
FORCE_ARG=()
[[ "$FORCE" != "0" ]] && FORCE_ARG=(--force)

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="results/upload-sweep-${STAMP}.json"

echo ">> UPLOAD benchmarking configured sizes -> ${OUT} (force=${FORCE})"
node src/upload-benchmark.js \
  "${FORCE_ARG[@]}" \
  ${WORKERS:+--workers "$WORKERS"} \
  ${CONCURRENCY:+--concurrency "$CONCURRENCY"} \
  ${ITERATIONS:+--iterations "$ITERATIONS"} \
  ${WARMUP:+--warmup "$WARMUP"} \
  ${PART_SIZE:+--part-size "$PART_SIZE"} \
  --out "$OUT"

echo ">> Done. JSON: ${OUT}"
