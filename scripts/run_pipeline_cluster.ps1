# Run the MARITIME-X Spark pipeline on the 3-node cluster: YARN executors, HDFS lake.
# Start the cluster first:  .\scripts\cluster.ps1 start
#   .\scripts\run_pipeline_cluster.ps1                          # all stages
#   .\scripts\run_pipeline_cluster.ps1 -From gaps               # resume from a stage
#   .\scripts\run_pipeline_cluster.ps1 -Only serving_views,load_mongodb
param([string]$From = "", [string[]]$Only = @())
. "$PSScriptRoot\env_cluster.ps1"
Set-Location (Split-Path $PSScriptRoot -Parent)
New-Item -ItemType Directory -Force output\logs | Out-Null
$pyArgs = @()
if ($From) { $pyArgs += @("--from", $From) }
if ($Only) { $pyArgs += @("--only", ($Only -join ",")) }
python -u -m src.pipeline @pyArgs 2>&1 | Tee-Object -FilePath output\logs\pipeline_cluster.log
