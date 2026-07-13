#!/usr/bin/env bash
# Pull captured benchmark runs from S3 into the local repo (results/runs/) so they
# can be committed alongside the code. Reads bucket/region from bench.config.json.
# Usage: ./scripts/pull-results.sh
set -euo pipefail
cd "$(dirname "$0")/.."

BUCKET="$(node -e "process.stdout.write(String(require('./bench.config.json').bucket||''))")"
REGION="$(node -e "process.stdout.write(String(require('./bench.config.json').region||''))")"
[[ -n "$BUCKET" ]] || { echo "No bucket in bench.config.json" >&2; exit 1; }

mkdir -p results/runs
echo ">> Syncing s3://${BUCKET}/results/runs/ -> results/runs/"
aws s3 sync "s3://${BUCKET}/results/runs/" results/runs/ ${REGION:+--region "$REGION"}
echo ">> Done. Then: git add results/runs && git commit -m 'benchmarks: ...' && git push"
