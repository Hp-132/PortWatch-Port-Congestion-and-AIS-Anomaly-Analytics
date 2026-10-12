. "$PSScriptRoot\env_local.ps1"
Set-Location (Split-Path $PSScriptRoot -Parent)
python -m pytest -q tests -p no:warnings
