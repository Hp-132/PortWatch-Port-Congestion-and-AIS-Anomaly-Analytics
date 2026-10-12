# Run the full Spark pipeline locally (all stages, ~1 h on 48M rows / 12 cores).
#   .\scripts\run_pipeline_local.ps1                 # everything
#   .\scripts\run_pipeline_local.ps1 -From gaps      # resume from a stage
param([string]$From = "", [string[]]$Only = @())
. "$PSScriptRoot\env_local.ps1"
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force output\logs | Out-Null
$pyArgs = @()
if ($From) { $pyArgs += @("--from", $From) }
if ($Only) { $pyArgs += @("--only", ($Only -join ",")) }
python -u -m src.pipeline @pyArgs 2>&1 | Tee-Object -FilePath output\logs\pipeline_local.log
