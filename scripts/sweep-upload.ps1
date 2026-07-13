# UPLOAD sweep (Windows): benchmark multipart upload throughput (worker threads)
# across the size curve. For real numbers run scripts/sweep-upload.sh on EC2.
# Settings come from bench.config.json (shared + "upload" section); params override.
#
# Uploads are FORCED by default (re-uploads existing objects). Pass -NoForce to
# respect the config's forceUpload instead.
# WARNING: forcing re-uploads every size each iteration (e.g. 30 GiB x iterations).
#
# Usage: .\scripts\sweep-upload.ps1  [-Workers N] [-Concurrency N] [-Iterations N] [-Warmup N] [-PartSize 64MiB] [-NoForce]
param(
  [int]$Workers = 0,
  [int]$Concurrency = 0,
  [int]$Iterations = 0,
  [int]$Warmup = -1,
  [string]$PartSize = "",
  [switch]$NoForce
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$stamp = Get-Date -Format "yyyyMMddTHHmmss"
$out = "results/upload-sweep-$stamp.json"

# Raise libuv's thread pool (default 4) so async fs / dns.lookup don't serialize.
# Override by setting $env:UV_THREADPOOL_SIZE before running.
if (-not $env:UV_THREADPOOL_SIZE) { $env:UV_THREADPOOL_SIZE = "64" }

$benchArgs = @("--out", $out)
if (-not $NoForce)      { $benchArgs += "--force" }
if ($Workers -gt 0)     { $benchArgs += @("--workers", $Workers) }
if ($Concurrency -gt 0) { $benchArgs += @("--concurrency", $Concurrency) }
if ($Iterations -gt 0)  { $benchArgs += @("--iterations", $Iterations) }
if ($Warmup -ge 0)      { $benchArgs += @("--warmup", $Warmup) }
if ($PartSize)          { $benchArgs += @("--part-size", $PartSize) }

Write-Host ">> UPLOAD benchmarking configured sizes -> $out (force=$(-not $NoForce))"
node "$root/src/upload-benchmark.js" @benchArgs

Write-Host ">> Done. JSON: $out"
