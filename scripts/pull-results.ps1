# Pull captured benchmark runs from S3 into the local repo (results/runs/), so you
# can review and commit them alongside the code. Reads bucket/region from
# bench.config.json. Run from the project root on your dev machine.
#
# Usage: .\scripts\pull-results.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$cfg = Get-Content (Join-Path $root "bench.config.json") -Raw | ConvertFrom-Json
$bucket = $cfg.bucket
$region = $cfg.region
if (-not $bucket) { throw "No bucket in bench.config.json" }

$dest = Join-Path $root "results\runs"
New-Item -ItemType Directory -Force -Path $dest | Out-Null

$regionArgs = @()
if ($region) { $regionArgs = @("--region", $region) }

Write-Host ">> Syncing s3://$bucket/results/runs/ -> results\runs\"
aws s3 sync "s3://$bucket/results/runs/" $dest @regionArgs

Write-Host ">> Done. Review with: git status ; then:"
Write-Host "     git add results/runs && git commit -m 'benchmarks: <describe>' && git push"
