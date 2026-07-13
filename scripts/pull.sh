#!/usr/bin/env bash
# Pull project from S3 staging prefix -> $BENCH_DIR (run on the EC2 instance).
# Defaults are baked in below, so `./pull.sh` with no args just works. Override
# positionally if needed: ./pull.sh <bucket> [prefix] [region]
set -euo pipefail

# --- defaults (edit here) --------------------------------------------------
DEFAULT_BUCKET="s3dl-bench-usw2-801400661003"
DEFAULT_PREFIX="code/"
DEFAULT_REGION="us-west-2"
# ---------------------------------------------------------------------------

BUCKET="${1:-$DEFAULT_BUCKET}"
PREFIX="${2:-$DEFAULT_PREFIX}"
REGION="${3:-$DEFAULT_REGION}"
REGION_ARG=()
if [[ -n "$REGION" ]]; then REGION_ARG=(--region "$REGION"); fi

DEST="${BENCH_DIR:-$HOME/s3-bench}"
mkdir -p "$DEST"

echo "Syncing s3://${BUCKET}/${PREFIX} -> ${DEST}"
aws s3 sync "s3://${BUCKET}/${PREFIX}" "$DEST" \
  --delete \
  --exclude ".git/*" \
  --exclude "node_modules/*" \
  --exclude "results/*" \
  "${REGION_ARG[@]}"

echo "Installing deps..."
cd "$DEST"
npm install --omit=dev

echo "Ready in ${DEST}. All settings live in bench.config.json. Examples:"
echo "  ./scripts/sweep-download.sh              # seed + benchmark DOWNLOAD across the size curve"
echo "  ./scripts/sweep-upload.sh                # benchmark UPLOAD across the size curve"
echo "  node src/benchmark.js --sizes 100MiB,1GiB   # download specific sizes ad hoc"
echo "  WORKERS=16 CONCURRENCY=8 ./scripts/sweep-download.sh   # override tunables per-run"
