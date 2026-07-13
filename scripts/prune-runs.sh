#!/usr/bin/env bash
# Prune captured benchmark runs from the local repo AND S3, then stage the git
# deletion (optionally commit+push). Reads bucket/region from bench.config.json.
#
# List all runs:            ./scripts/prune-runs.sh
# Remove runs (globs ok):   ./scripts/prune-runs.sh '20260713T101500-exp1'
#                           ./scripts/prune-runs.sh '*exp1*' '20260714T*'
# Flags:  --force (no prompt)   --push (commit + push)
set -euo pipefail
cd "$(dirname "$0")/.."

FORCE=0; PUSH=0; PATTERNS=()
for a in "$@"; do
  case "$a" in
    --force) FORCE=1 ;;
    --push)  PUSH=1 ;;
    *)       PATTERNS+=("$a") ;;
  esac
done

BUCKET="$(node -e "process.stdout.write(String(require('./bench.config.json').bucket||''))")"
REGION="$(node -e "process.stdout.write(String(require('./bench.config.json').region||''))")"
REGION_ARG=(); [[ -n "$REGION" ]] && REGION_ARG=(--region "$REGION")

mapfile -t ALL < <(find results/runs -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null | sort)

if [[ ${#PATTERNS[@]} -eq 0 ]]; then
  echo "Runs in results/runs/ (${#ALL[@]}):"
  printf '  %s\n' "${ALL[@]}"
  echo
  echo "Prune with: ./scripts/prune-runs.sh <name-or-glob> [...] [--force] [--push]"
  exit 0
fi

MATCHED=()
for name in "${ALL[@]}"; do
  for p in "${PATTERNS[@]}"; do
    # shellcheck disable=SC2053
    [[ "$name" == $p ]] && { MATCHED+=("$name"); break; }
  done
done
if [[ ${#MATCHED[@]} -eq 0 ]]; then echo "Nothing to prune."; exit 0; fi

echo "Will remove these runs (local + s3://${BUCKET}/results/runs/):"
printf '  %s\n' "${MATCHED[@]}"
if [[ "$FORCE" -ne 1 ]]; then
  read -r -p "Proceed? (y/N) " ans
  [[ "$ans" == "y" || "$ans" == "Y" ]] || { echo "Aborted."; exit 0; }
fi

for name in "${MATCHED[@]}"; do
  rm -rf "results/runs/$name"
  [[ -n "$BUCKET" ]] && aws s3 rm "s3://${BUCKET}/results/runs/${name}/" --recursive "${REGION_ARG[@]}" >/dev/null || true
  echo "removed $name"
done

git add -A
if [[ "$PUSH" -eq 1 ]]; then
  git commit -m "clean: prune benchmark runs (${#MATCHED[@]})"
  git push
  echo "Committed and pushed."
else
  echo "Staged removals. Finish with: git commit -m 'prune runs' ; git push"
fi
