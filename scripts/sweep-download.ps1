# DOWNLOAD sweep (Windows): seed configured sizes, then benchmark download across
# the size curve. For real numbers run scripts/sweep-download.sh on in-region EC2.
# Settings come from bench.config.json (shared + "download" section); params below
# override per-run.
#
# Usage: .\scripts\sweep-download.ps1  [-Workers N] [-Concurrency N] [-Iterations N] [-Warmup N] [-PartSize 64MiB]
param(
  [int]$Workers = 0,
  [int]$Concurrency = 0,
  [int]$Iterations = 0,
  [int]$Warmup = -1,
  [string]$PartSize = ""
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$out = "results/download-sweep-$stamp.json"

# Raise libuv's thread pool (default 4) so async fs writes / dns.lookup don't
# serialize. Override by setting $env:UV_THREADPOOL_SIZE before running.
if (-not $env:UV_THREADPOOL_SIZE) { $env:UV_THREADPOOL_SIZE = "64" }

$seedArgs = @()
if ($PartSize) { $seedArgs += @("--part-size", $PartSize) }

$benchArgs = @("--out", $out)
if ($Workers -gt 0)     { $benchArgs += @("--workers", $Workers) }
if ($Concurrency -gt 0) { $benchArgs += @("--concurrency", $Concurrency) }
if ($Iterations -gt 0)  { $benchArgs += @("--iterations", $Iterations) }
if ($Warmup -ge 0)      { $benchArgs += @("--warmup", $Warmup) }

Write-Host ">> Seeding configured sizes (bench.config.json), skipping existing objects"
node "$root/src/upload-test-data.js" @seedArgs

Write-Host ">> DOWNLOAD benchmarking configured sizes -> $out"
node "$root/src/benchmark.js" @benchArgs

Write-Host ">> Done. JSON: $out"
