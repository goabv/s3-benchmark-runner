#!/usr/bin/env bash
# Compare the download path between two node versions to localize a regression.
# Runs layered microbenchmarks under each node binary and prints them side by
# side, so you can see WHICH layer moved (crypto / TLS stream / plain stream /
# handshake / real S3).
#
# Usage:
#   ./run.sh <nodeA> <nodeB> [--s3]
#     nodeA, nodeB : paths to the two node binaries to compare, e.g.
#                    ./run.sh "$(nvm which 22)" "$(nvm which 24)"
#     --s3         : also run the real single-connection S3 probe (needs a seeded
#                    object + network; hits S3, adds variance).
#
# Offline probes (no external network) are the decisive signal; --s3 is the
# ground-truth confirmation.
set -euo pipefail
cd "$(dirname "$0")"

NA="${1:?usage: run.sh <nodeA> <nodeB> [--s3]}"
NB="${2:?usage: run.sh <nodeA> <nodeB> [--s3]}"
S3=0; [[ "${3:-}" == "--s3" ]] && S3=1

# Pin the TLS suite for the https probe so both versions negotiate the same cipher
# (otherwise a different default would confound the comparison).
CIPHER="TLS_AES_128_GCM_SHA256"

run_all() {
  local node="$1"
  echo "==================================================================="
  echo " $("$node" -v)   ($node)"
  echo "==================================================================="
  echo "[fingerprint]"; "$node" fingerprint.mjs
  echo "[crypto-aesgcm] (OpenSSL bulk AEAD, no network)"; "$node" crypto-aesgcm.mjs
  echo "[crc32-js] (JS CRC32 by coding pattern — local vs field vs for..of)"; "$node" crc32-js.mjs --seconds 3
  echo "[stream-chunks] (per-chunk stream receive path, ~ real download)"; "$node" stream-chunks.mjs --chunk 65536 --stages 1 --checksum --total 1073741824
  echo "[loopback http  keepalive node]   (stream/http path, no TLS)"; "$node" loopback.mjs --conns 8 --duration 5 --gc-stats
  echo "[loopback https keepalive node]   (TLS record crypto + stream)"; "$node" loopback.mjs --tls --cipher "$CIPHER" --conns 8 --duration 5 --gc-stats
  echo "[loopback https fresh     node]   (per-request handshake cost)"; "$node" loopback.mjs --tls --cipher "$CIPHER" --conns 8 --duration 5 --fresh
  echo "[loopback http  keepalive fetch]  (bundled-undici path, no TLS)"; "$node" loopback.mjs --client fetch --conns 8 --duration 5 --gc-stats
  echo "[loopback https keepalive fetch]  (bundled-undici path, TLS)"; "$node" loopback.mjs --client fetch --tls --conns 8 --duration 5 --gc-stats
  if [[ "$S3" -eq 1 ]]; then
    echo "[s3-single node handler]";   "$node" s3-single.mjs --handler node
    echo "[s3-single undici handler]"; "$node" s3-single.mjs --handler undici
  fi
  echo
}

run_all "$NA"
run_all "$NB"

cat <<'EOF'
How to read it:
  - crypto-aesgcm regressed                 -> bundled OpenSSL (bulk cipher)
  - http loopback (node) regressed          -> V8 / core streams / Buffer / GC
  - only https loopback (node) regressed    -> TLS record crypto/OpenSSL, not JS
  - only "https fresh" regressed            -> TLS handshake/connect (OpenSSL)
  - node http regressed but fetch did NOT   -> node core http/stream (not undici) -> try the undici handler
  - fetch (undici) also regressed           -> bundled undici and/or shared net/stream layer
  - GC% jumps on the slower version         -> GC-driven regression (V8) -> try --max-semi-space-size
  - loopback fine but s3-single regressed   -> network/DNS/handshake to S3, or SDK path
Compare the two version blocks line by line; the layer with the biggest gap is the cause.
EOF
