#!/usr/bin/env bash
# DOWNLOAD sweep: benchmark download throughput across the whole size curve.
# The objects must already exist in the bucket — run the UPLOAD sweep first
# (./scripts/sweep-upload.sh), which creates them. Run ON THE EC2 INSTANCE, in the
# same region as the bucket.
#
# Settings come from bench.config.json (shared keys + the "download" section).
# Override per-run with env vars: WORKERS, CONCURRENCY, ITERATIONS, WARMUP.
#
# Usage: ./scripts/sweep-download.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# libuv thread pool serves dns.lookup and any async fs work. Default is 4; raise it
# so background I/O across many workers isn't serialized. Overridable.
export UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-64}"

STAMP="$(date +%Y%m%dT%H%M%S)"
OUT="results/download-sweep-${STAMP}.json"

echo ">> DOWNLOAD benchmarking configured sizes -> ${OUT}"
# PROFILE=1 -> CPU-profile each worker; profiles land in results/profile-<node>/
# (per node version, so runs under different nodes don't clobber). PROFILE_DIR overrides.
node src/benchmark.js \
  ${WORKERS:+--workers "$WORKERS"} \
  ${CONCURRENCY:+--concurrency "$CONCURRENCY"} \
  ${ITERATIONS:+--iterations "$ITERATIONS"} \
  ${WARMUP:+--warmup "$WARMUP"} \
  ${PROFILE:+--profile} \
  ${PROFILE_DIR:+--profile-dir "$PROFILE_DIR"} \
  --out "$OUT"

echo ">> Done. JSON: ${OUT}"
