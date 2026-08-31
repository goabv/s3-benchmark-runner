# DOWNLOAD sweep (Windows): benchmark download across the size curve. The objects
# must already exist — run the upload sweep first. For real numbers run
# scripts/sweep-download.sh on in-region EC2. Settings come from bench.config.json
# (shared + "download" section); params below override per-run.
#
# Usage: .\scripts\sweep-download.ps1  [-Workers N] [-Concurrency N] [-Iterations N] [-Warmup N]
param(
  [int]$Workers = 0,
  [int]$Concurrency = 0,
  [int]$Iterations = 0,
  [int]$Warmup = -1
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$out = "results/download-sweep-$stamp.json"

# Raise libuv's thread pool (default 4) so async fs / dns.lookup don't serialize.
# Override by setting $env:UV_THREADPOOL_SIZE before running.
if (-not $env:UV_THREADPOOL_SIZE) { $env:UV_THREADPOOL_SIZE = "64" }

$benchArgs = @("--out", $out)
if ($Workers -gt 0)     { $benchArgs += @("--workers", $Workers) }
if ($Concurrency -gt 0) { $benchArgs += @("--concurrency", $Concurrency) }
if ($Iterations -gt 0)  { $benchArgs += @("--iterations", $Iterations) }
if ($Warmup -ge 0)      { $benchArgs += @("--warmup", $Warmup) }
if ($env:PROFILE)       { $benchArgs += "--profile" }
if ($env:PROFILE_DIR)   { $benchArgs += @("--profile-dir", $env:PROFILE_DIR) }

Write-Host ">> DOWNLOAD benchmarking configured sizes -> $out"
node "$root/src/benchmark.js" @benchArgs

Write-Host ">> Done. JSON: $out"
