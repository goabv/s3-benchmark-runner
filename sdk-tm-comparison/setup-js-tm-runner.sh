#!/usr/bin/env bash
# setup-js-tm-runner.sh — stand up the aws-crt-s3-benchmarks "s3-benchmark-js"
# runner (PR #119, smilkuri:js-runner-tm) on this EC2 box, so you can:
#   1) benchmark the OFFICIAL @aws-sdk/lib-transfer-manager, and
#   2) deep-dive (and optionally INSTRUMENT) its source to see why it's slower.
#
# It does NOT touch your own runner repo (~/s3-bench). Idempotent: re-run safe.
#
# Two ways to get the SDK Transfer Manager into the runner:
#   (A) PREBUILT TGZ (default) — use the .tgz your teammate handed you. Fast.
#   (B) --build-from-source     — clone aws-sdk-js-v3, build lib-transfer-manager
#                                 + its deps, pack a fresh tgz, and wire it in. Use
#                                 this when you want to edit/instrument the SDK and
#                                 benchmark your modified build.
#
# Usage (run from anywhere; lives in sdk-tm-comparison/):
#   ./sdk-tm-comparison/setup-js-tm-runner.sh --tgz ~/aws-sdk-lib-transfer-manager-3.1090.0.tgz
#   ./sdk-tm-comparison/setup-js-tm-runner.sh --build-from-source
#   ./sdk-tm-comparison/setup-js-tm-runner.sh --build-from-source --sdk-ref v3.1090.0
#
# Flags:
#   --tgz PATH           (mode A) Path to aws-sdk-lib-transfer-manager-<ver>.tgz. If
#                        omitted, searches $HOME and CWD for one.
#   --build-from-source  (mode B) Build the TM from aws-sdk-js-v3 source and pack a
#                        fresh tgz instead of using a prebuilt one. Slow first run
#                        (installs + builds the workspace dep subgraph).
#   --sdk-ref REF        Git tag/branch of aws-sdk-js-v3 to read/build. Default: main.
#                        For mode B, pick a tag whose @aws-sdk/client-s3 is published
#                        to npm (the runner pulls client-s3 as a peer) — e.g. v3.1090.0.
#   --no-sdk-source      (mode A only) Skip the read-only TS source clone.
#   --harness-dir DIR    Where to clone the harness. Default: $HOME/aws-crt-s3-benchmarks
set -euo pipefail

# --- tunables / defaults ---------------------------------------------------
HARNESS_DIR="${HOME}/aws-crt-s3-benchmarks"
HARNESS_REPO="https://github.com/awslabs/aws-crt-s3-benchmarks.git"
PR_NUMBER=119
PR_BRANCH="js-runner-tm"
RUNNER_SUBDIR="runners/s3-benchmark-js"
SDK_EXTRACT_DIR="${HOME}/sdk-tm-shipped"   # extracted prebuilt tgz (reference only)
SDK_SOURCE_DIR="${HOME}/aws-sdk-js-v3"     # TS source clone (read + build)
SDK_SUBPATH="lib/lib-transfer-manager"
SDK_PKG="@aws-sdk/lib-transfer-manager"
BUILT_TGZ="${HOME}/aws-sdk-lib-transfer-manager-source.tgz"  # produced by mode B
TGZ_PATH=""
WITH_SDK_SOURCE=1
BUILD_FROM_SOURCE=0
SDK_REF="main"
# ---------------------------------------------------------------------------

# --- parse args ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --tgz) TGZ_PATH="$2"; shift 2 ;;
    --build-from-source) BUILD_FROM_SOURCE=1; shift ;;
    --no-sdk-source) WITH_SDK_SOURCE=0; shift ;;
    --sdk-ref) SDK_REF="$2"; shift 2 ;;
    --harness-dir) HARNESS_DIR="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

log() { printf '\n\033[1;36m==>\033[0m %s\n' "$*"; }

# --- node sanity (lib-transfer-manager needs Node 20+, runner wants 22+) ---
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
if [[ "$NODE_MAJOR" -lt 22 ]]; then
  echo "WARNING: Node $NODE_MAJOR detected; use Node 22+ for the runner (nvm use 22)." >&2
fi

CLIENT_S3_VERSION=""  # set in mode B so the runner's peer dep matches the built TM

# ===========================================================================
# MODE B: build @aws-sdk/lib-transfer-manager from source and pack a tgz
# ===========================================================================
if [[ "$BUILD_FROM_SOURCE" -eq 1 ]]; then
  log "Build-from-source: cloning aws-sdk-js-v3 (full, blobless) at ref '$SDK_REF'"
  if [[ ! -d "$SDK_SOURCE_DIR/.git" ]]; then
    git clone --filter=blob:none https://github.com/aws/aws-sdk-js-v3.git "$SDK_SOURCE_DIR"
  fi
  cd "$SDK_SOURCE_DIR"
  git sparse-checkout disable 2>/dev/null || true   # need the whole tree to build
  git fetch --tags --quiet origin || true
  if git checkout --quiet "$SDK_REF" 2>/dev/null || git checkout --quiet "v${SDK_REF#v}" 2>/dev/null; then
    echo "  building $(git describe --tags --always 2>/dev/null || git rev-parse --short HEAD)"
  else
    echo "  ref '$SDK_REF' not found; building main"
    git checkout --quiet main
  fi

  # The monorepo uses yarn (berry). corepack ships with Node and pins the right yarn.
  corepack enable 2>/dev/null || true

  log "yarn install (workspace bootstrap — slow the first time)"
  yarn install

  log "Building $SDK_PKG + its workspace deps (turbo; slow the first time)"
  yarn workspace "$SDK_PKG" build:include:deps

  log "Packing a fresh tgz -> $BUILT_TGZ"
  rm -f "$BUILT_TGZ"
  yarn workspace "$SDK_PKG" pack --out "$BUILT_TGZ"

  # Built version drives the runner's client-s3 peer (must be published to npm).
  CLIENT_S3_VERSION="$(node -p "require('$SDK_SOURCE_DIR/$SDK_SUBPATH/package.json').version")"
  TGZ_PATH="$BUILT_TGZ"
  echo "  built $SDK_PKG@$CLIENT_S3_VERSION -> $BUILT_TGZ"
  echo "  NOTE: the runner will pull @aws-sdk/client-s3@$CLIENT_S3_VERSION (peer) from npm."
fi

# --- locate the tgz (mode A: provided/discovered; mode B: the freshly built one)
if [[ -z "$TGZ_PATH" ]]; then
  log "No --tgz given; searching \$HOME and CWD for aws-sdk-lib-transfer-manager-*.tgz"
  TGZ_PATH="$(find "$HOME" . -maxdepth 2 -name 'aws-sdk-lib-transfer-manager-*.tgz' 2>/dev/null | head -n1 || true)"
fi
if [[ -z "$TGZ_PATH" || ! -f "$TGZ_PATH" ]]; then
  echo "ERROR: no lib-transfer-manager .tgz found." >&2
  echo "       Pass --tgz <path>, or use --build-from-source." >&2
  exit 1
fi
TGZ_PATH="$(cd "$(dirname "$TGZ_PATH")" && pwd)/$(basename "$TGZ_PATH")"  # absolutize
TGZ_NAME="$(basename "$TGZ_PATH")"
log "Using artifact: $TGZ_PATH"

# --- clone harness + check out the PR branch -------------------------------
if [[ ! -d "$HARNESS_DIR/.git" ]]; then
  log "Cloning harness -> $HARNESS_DIR"
  git clone "$HARNESS_REPO" "$HARNESS_DIR"
else
  log "Harness already present at $HARNESS_DIR"
fi
cd "$HARNESS_DIR"
log "Fetching PR #$PR_NUMBER -> local branch '$PR_BRANCH'"
git fetch origin "pull/${PR_NUMBER}/head:${PR_BRANCH}" 2>&1 | tail -n2 || true
git checkout "$PR_BRANCH"

RUNNER_DIR="$HARNESS_DIR/$RUNNER_SUBDIR"
if [[ ! -d "$RUNNER_DIR" ]]; then
  echo "ERROR: expected runner dir not found: $RUNNER_DIR" >&2
  exit 1
fi

# --- wire the tgz into the runner BEFORE install ---------------------------
log "Copying artifact into runner (must be there before install)"
cp -f "$TGZ_PATH" "$RUNNER_DIR/$TGZ_NAME"

# Rewrite the runner's package.json so it points at OUR tgz name, and (mode B)
# align the @aws-sdk/client-s3 peer to the built TM version. Done with node so the
# JSON stays valid regardless of the pinned name in the PR.
log "Pointing runner package.json at $TGZ_NAME$( [[ -n "$CLIENT_S3_VERSION" ]] && echo " + client-s3 $CLIENT_S3_VERSION" )"
TGZ_NAME="$TGZ_NAME" CLIENT_S3_VERSION="$CLIENT_S3_VERSION" node -e '
  const fs = require("fs");
  const p = process.argv[1];
  const pkg = JSON.parse(fs.readFileSync(p, "utf8"));
  pkg.dependencies = pkg.dependencies || {};
  pkg.dependencies["@aws-sdk/lib-transfer-manager"] = "file:./" + process.env.TGZ_NAME;
  if (process.env.CLIENT_S3_VERSION) pkg.dependencies["@aws-sdk/client-s3"] = process.env.CLIENT_S3_VERSION;
  fs.writeFileSync(p, JSON.stringify(pkg, null, 2) + "\n");
' "$RUNNER_DIR/package.json"

# --- install runner deps ---------------------------------------------------
cd "$RUNNER_DIR"
rm -f yarn.lock   # stale lock may pin the old tgz/version; let it resolve fresh
if command -v yarn >/dev/null 2>&1; then
  log "yarn install (runner deps + local tgz)"
  yarn install
else
  log "yarn not found; using npm install"
  npm install
fi

# --- reading copy of the source -------------------------------------------
if [[ "$BUILD_FROM_SOURCE" -eq 1 ]]; then
  READ_DIR="$SDK_SOURCE_DIR/$SDK_SUBPATH/src/submodules"   # you already cloned full source
else
  # Extract the prebuilt tgz for reference (transpiled, no comments).
  log "Extracting shipped build (reference only) -> $SDK_EXTRACT_DIR"
  rm -rf "$SDK_EXTRACT_DIR"; mkdir -p "$SDK_EXTRACT_DIR"
  tar -xzf "$TGZ_PATH" -C "$SDK_EXTRACT_DIR" --strip-components=1
  echo "  Shipped build: $SDK_EXTRACT_DIR/dist-cjs/"
  if [[ "$WITH_SDK_SOURCE" -eq 1 ]]; then
    log "Cloning original TS source (sparse: only $SDK_SUBPATH) at ref '$SDK_REF'"
    if [[ ! -d "$SDK_SOURCE_DIR/.git" ]]; then
      git clone --filter=blob:none --sparse https://github.com/aws/aws-sdk-js-v3.git "$SDK_SOURCE_DIR"
    fi
    ( cd "$SDK_SOURCE_DIR"
      git sparse-checkout set "$SDK_SUBPATH"
      git fetch --tags --quiet origin || true
      git checkout --quiet "$SDK_REF" 2>/dev/null || git checkout --quiet "v${SDK_REF#v}" 2>/dev/null || git checkout --quiet main
    )
    READ_DIR="$SDK_SOURCE_DIR/$SDK_SUBPATH/src/submodules"
  else
    READ_DIR="$SDK_EXTRACT_DIR/dist-cjs (transpiled)"
  fi
fi

# --- done: next steps ------------------------------------------------------
cat <<EOF

================================ SETUP COMPLETE ================================
Harness + js runner: $RUNNER_DIR
Artifact installed:  $TGZ_NAME$( [[ "$BUILD_FROM_SOURCE" -eq 1 ]] && echo "  (built from source @ $CLIENT_S3_VERSION)" )
Source to read:      $READ_DIR

$( if [[ "$BUILD_FROM_SOURCE" -eq 1 ]]; then cat <<'INNER'
INSTRUMENT-AND-REBUILD LOOP (mode B)
------------------------------------
1) Edit the SDK source, e.g. add timing/logging in:
     ~/aws-sdk-js-v3/lib/lib-transfer-manager/src/submodules/transfer-manager/S3TransferManager.ts
     ~/aws-sdk-js-v3/lib/lib-transfer-manager/src/submodules/transfer-manager/worker-http-handler.ts
2) Rebuild + repack + reinstall into the runner (re-run this script):
     ~/s3-bench/sdk-tm-comparison/setup-js-tm-runner.sh --build-from-source --sdk-ref <same ref>
   (or, faster, just the SDK bits by hand:)
     cd ~/aws-sdk-js-v3 && yarn workspace @aws-sdk/lib-transfer-manager build:include:deps \\
       && yarn workspace @aws-sdk/lib-transfer-manager pack --out ~/aws-sdk-lib-transfer-manager-source.tgz
     cd <runner> && cp ~/aws-sdk-lib-transfer-manager-source.tgz . && yarn install --check-files
INNER
fi )

NEXT STEPS
----------
1) Build workloads:  cd $HARNESS_DIR && python3 scripts/build-workloads.py
2) Prep files:       cd $HARNESS_DIR && python3 scripts/prep-s3-files.py \\
                       --bucket <BUCKET> --region <REGION> --files-dir \$HOME/files
3) Run SDK runner:   cd \$HOME/files && node $RUNNER_DIR/main.mjs sdk-js-tm \\
                       $HARNESS_DIR/workloads/<workload>.run.json <BUCKET> <REGION>
4) Profile it:       cd $RUNNER_DIR && ./profile.sh sdk-js-tm \\
                       $HARNESS_DIR/workloads/<workload>.run.json <BUCKET> <REGION>
5) Profile yours:    cd \$HOME/s3-bench && node src/upload-benchmark.js --profile
   Compare hotspots: node ~/s3-bench/scripts/prof-top.mjs <any .cpuprofile>

DEEP-DIVE MAP (src/submodules/)
  transfer-manager/S3TransferManager.ts   Main class: upload()/download() orchestration.
  transfer-manager/worker-http-handler.ts KEY: dispatches each HTTP request to a worker
                                          (request/response marshalled across threads).
  transfer-manager/chunker.ts             Body -> parts splitting.
  transfer-manager/join-streams.ts        Download part reassembly.
  worker/http-request-worker.ts           Worker entry: runs one serialized HTTP request.
===============================================================================
EOF
