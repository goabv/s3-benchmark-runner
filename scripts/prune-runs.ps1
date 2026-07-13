# Prune captured benchmark runs you don't want to keep — removes them from the
# local repo AND from S3, then stages the git deletion (optionally commits+pushes).
# Reads bucket/region from bench.config.json. Run from the project root.
#
# List all runs:
#   .\scripts\prune-runs.ps1
# Remove specific runs (names or globs; keeps README.md / .gitkeep):
#   .\scripts\prune-runs.ps1 20260713T101500-exp1
#   .\scripts\prune-runs.ps1 *exp1* 20260714T*        # multiple patterns / globs
#   .\scripts\prune-runs.ps1 *old* -Force             # skip the confirm prompt
#   .\scripts\prune-runs.ps1 *old* -Push              # also commit + push
param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$Patterns,
  [switch]$Force,
  [switch]$Push
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$runsDir = Join-Path $root "results\runs"
$cfg = Get-Content (Join-Path $root "bench.config.json") -Raw | ConvertFrom-Json
$bucket = $cfg.bucket
$region = $cfg.region
$regionArgs = @(); if ($region) { $regionArgs = @("--region", $region) }

$allRuns = @(Get-ChildItem $runsDir -Directory -ErrorAction SilentlyContinue | Sort-Object Name)

# No patterns -> just list what's there.
if (-not $Patterns -or $Patterns.Count -eq 0) {
  Write-Host "Runs in results/runs/ ($($allRuns.Count)):"
  foreach ($r in $allRuns) { Write-Host "  $($r.Name)" }
  Write-Host ""
  Write-Host "Prune with: .\scripts\prune-runs.ps1 <name-or-glob> [...] [-Force] [-Push]"
  return
}

# Resolve matches (supports globs like 20260713* or *exp1*).
$matched = @()
foreach ($p in $Patterns) {
  $m = @($allRuns | Where-Object { $_.Name -like $p })
  if (-not $m) { Write-Warning "no run matches '$p'" }
  $matched += $m
}
$matched = @($matched | Sort-Object Name -Unique)
if (-not $matched) { Write-Host "Nothing to prune."; return }

Write-Host "Will remove these runs (local + s3://$bucket/results/runs/):"
foreach ($r in $matched) { Write-Host "  $($r.Name)" }
if (-not $Force) {
  $ans = Read-Host "Proceed? (y/N)"
  if ($ans -ne 'y' -and $ans -ne 'Y') { Write-Host "Aborted."; return }
}

foreach ($r in $matched) {
  Remove-Item $r.FullName -Recurse -Force
  if ($bucket) {
    aws s3 rm "s3://$bucket/results/runs/$($r.Name)/" --recursive @regionArgs | Out-Null
  }
  Write-Host "removed $($r.Name)"
}

git -C $root add -A
if ($Push) {
  git -C $root commit -m "clean: prune benchmark runs ($($matched.Count))"
  git -C $root push
  Write-Host "Committed and pushed."
} else {
  Write-Host "Staged removals. Finish with: git commit -m 'prune runs' ; git push"
}
