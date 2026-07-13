#!/usr/bin/env bash
# Run a benchmark ON THE EC2 INSTANCE and capture a self-contained, committable
# run under results/runs/<timestamp>[-label]/, then upload it to S3 so you can
# pull it down and commit it from your dev machine.
#
# Each run directory contains:
#   config.json          - exact bench.config.json used (snapshot)
#   env.txt              - instance type, node/SDK versions, kernel, key sysctls
#   download-sweep.json  - download results (modes: both, download, bench)
#   upload-sweep.json    - upload results   (modes: both, upload)
#   summary.txt          - the formatted console output (throughput + resources)
#   *.csv / *.svg        - any part-times / time-series artifacts produced
#
# Usage:
#   ./scripts/run.sh [both]   [label]     # DEFAULT: seed + download sweep, then upload sweep
#   ./scripts/run.sh download [label]     # seed (if needed) + download sweep only
#   ./scripts/run.sh upload   [label]     # upload sweep (forced) only
#   ./scripts/run.sh bench    [label]     # download sweep WITHOUT seeding
#
# Example: ./scripts/run.sh both aes128-spread
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-both}"
LABEL="${2:-}"
STAMP="$(date +%Y%m%dT%H%M%S)"
NAME="${STAMP}${LABEL:+-${LABEL}}"
DIR="results/runs/${NAME}"
mkdir -p "$DIR"

# Raise libuv's thread pool (async fs writes / dns.lookup) unless already set.
export UV_THREADPOOL_SIZE="${UV_THREADPOOL_SIZE:-64}"

# Read bucket/region straight from the config (single source of truth).
BUCKET="$(node -e "process.stdout.write(String(require('./bench.config.json').bucket||''))")"
REGION="$(node -e "process.stdout.write(String(require('./bench.config.json').region||''))")"

# --- snapshot the exact config used ---
cp bench.config.json "$DIR/config.json"

# --- capture environment for reproducibility ---
{
  echo "timestamp:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "mode:        ${MODE}"
  echo "label:       ${LABEL:-}"
  echo "host:        $(hostname)"
  echo "node:        $(node --version)"
  echo "os:          $(uname -srmo 2>/dev/null || uname -a)"
  echo "cpus:        $(nproc 2>/dev/null || echo '?')"
  echo "mem:         $(free -h 2>/dev/null | awk '/^Mem:/{print $2}' || echo '?')"
  echo "UV_THREADPOOL_SIZE: ${UV_THREADPOOL_SIZE}"
  # EC2 instance metadata (IMDSv2), best-effort.
  TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" --max-time 2 2>/dev/null || true)
  if [[ -n "${TOKEN:-}" ]]; then
    md() { curl -s -H "X-aws-ec2-metadata-token: $TOKEN" --max-time 2 \
      "http://169.254.169.254/latest/meta-data/$1" 2>/dev/null || true; }
    echo "instance-type: $(md instance-type)"
    echo "az:            $(md placement/availability-zone)"
  fi
  echo "--- sysctl (network) ---"
  for k in net.ipv4.tcp_congestion_control net.core.default_qdisc \
           net.ipv4.tcp_slow_start_after_idle net.core.rmem_max net.core.wmem_max; do
    printf '  %s = %s\n' "$k" "$(sysctl -n "$k" 2>/dev/null || echo '?')"
  done
} > "$DIR/env.txt"

MARKER="$DIR/.start"; : > "$MARKER"

# The formatted table + [info]/[done] lines (exactly what you see on the terminal)
# are appended to summary.txt so each committed run is human-readable at a glance.
SUMMARY="$DIR/summary.txt"; : > "$SUMMARY"

do_seed()     { echo ">> Seeding configured sizes (skipping existing) ..."; node src/upload-test-data.js; }
do_download() { echo ">> DOWNLOAD sweep -> $DIR/download-sweep.json"
                node src/benchmark.js --out "$DIR/download-sweep.json" 2>&1 | tee -a "$SUMMARY"; }
do_upload()   { echo ">> UPLOAD sweep (forced) -> $DIR/upload-sweep.json"
                node src/upload-benchmark.js --force --out "$DIR/upload-sweep.json" 2>&1 | tee -a "$SUMMARY"; }

case "$MODE" in
  both|all) do_seed; do_download; do_upload ;;
  download) do_seed; do_download ;;
  bench)    do_download ;;
  upload)   do_upload ;;
  *)
    echo "unknown mode '${MODE}' (use both | download | upload | bench)" >&2
    exit 1
    ;;
esac

# Collect any part-times / time-series artifacts produced during this run.
find results -maxdepth 1 -type f \( -name '*.csv' -o -name '*.svg' \) -newer "$MARKER" \
  -exec mv {} "$DIR/" \; 2>/dev/null || true
rm -f "$MARKER"

# Upload the captured run to S3 so it can be pulled + committed from the dev box.
if [[ -n "$BUCKET" ]]; then
  echo ">> Uploading run to s3://${BUCKET}/results/runs/${NAME}/"
  aws s3 cp "$DIR" "s3://${BUCKET}/results/runs/${NAME}/" --recursive \
    ${REGION:+--region "$REGION"}
fi

echo ">> Done. Run captured in ${DIR}"
echo ">> On your dev machine: .\\scripts\\pull-results.ps1   then commit results/runs/"
