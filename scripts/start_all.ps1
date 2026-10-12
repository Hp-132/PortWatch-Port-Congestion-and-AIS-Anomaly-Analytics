# Start the whole MARITIME-X system: 3-node cluster + dashboard (MongoDB runs as a Windows service).
#   .\scripts\start_all.ps1
$Project = Split-Path $PSScriptRoot -Parent
Set-Location $Project
& "$PSScriptRoot\cluster.ps1" start

$pidFile = "C:\mx-cluster\dashboard.pid"
$running = (Test-Path $pidFile) -and (Get-Process -Id ([int](Get-Content $pidFile)) -ErrorAction SilentlyContinue)
if (-not $running) {
    New-Item -ItemType Directory -Force output\logs | Out-Null
    $env:MX_MONGO_URI = "mongodb://localhost:27017"
    $p = Start-Process -FilePath python -ArgumentList "-m", "uvicorn", "dashboard.backend.app:app", "--host", "0.0.0.0", "--port", "8000" `
        -WorkingDirectory $Project -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput output\logs\dashboard.out -RedirectStandardError output\logs\dashboard.err
    Set-Content $pidFile $p.Id
    Write-Host "dashboard started (pid $($p.Id))"
}
for ($i = 0; $i -lt 40; $i++) {
    try { if ((Invoke-WebRequest http://localhost:8000/api/health -UseBasicParsing -TimeoutSec 3).StatusCode -eq 200) { break } } catch {}
    Start-Sleep 1
}
Write-Host ""
Write-Host "MARITIME-X dashboard : http://localhost:8000"
Write-Host "API docs (Swagger)   : http://localhost:8000/docs"
Write-Host "HDFS NameNode UI     : http://localhost:9870"
Write-Host "YARN ResourceManager : http://localhost:8088"
Write-Host "Spark Master UI      : http://localhost:8080"
