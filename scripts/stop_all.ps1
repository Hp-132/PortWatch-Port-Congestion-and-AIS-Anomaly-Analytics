# Stop the dashboard and all cluster daemons (MongoDB keeps running as a Windows service).
#   .\scripts\stop_all.ps1
$pidFile = "C:\mx-cluster\dashboard.pid"
if (Test-Path $pidFile) {
    & cmd.exe /c "taskkill /PID $(Get-Content $pidFile) /T /F >nul 2>&1"
    Remove-Item $pidFile
    Write-Host "dashboard stopped"
}
& "$PSScriptRoot\cluster.ps1" stop
