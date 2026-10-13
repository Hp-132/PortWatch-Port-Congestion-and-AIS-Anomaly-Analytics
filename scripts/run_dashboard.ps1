# Start the FastAPI dashboard (reads MongoDB only).  Open http://localhost:8000
param([int]$Port = 8000)
Set-Location (Split-Path $PSScriptRoot -Parent)
if (-not $env:MX_MONGO_URI) { $env:MX_MONGO_URI = "mongodb://localhost:27017" }
python -m uvicorn dashboard.backend.app:app --host 0.0.0.0 --port $Port
