param(
    [int]$Rows = 1000,
    [int]$Days = 7,
    [double]$InvalidRate = 0.01,
    [double]$DuplicateRate = 0.02,
    [int]$Seed = 42,
    [int]$DashboardPort = 8501
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:DASHBOARD_PORT = $DashboardPort

docker compose up -d --build zookeeper kafka minio notebook dashboard
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose failed to start the local stack."
}

$minioReady = $false
for ($attempt = 1; $attempt -le 30; $attempt++) {
    try {
        $response = Invoke-WebRequest `
            -UseBasicParsing `
            -Uri "http://localhost:9000/minio/health/live" `
            -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            $minioReady = $true
            break
        }
    }
    catch {
        Start-Sleep -Seconds 2
    }
}
if (-not $minioReady) {
    throw "MinIO did not become healthy within 60 seconds."
}

docker compose exec -T notebook python -m insightsstream.delta_demo `
    --rows $Rows `
    --days $Days `
    --invalid-rate $InvalidRate `
    --duplicate-rate $DuplicateRate `
    --seed $Seed
if ($LASTEXITCODE -ne 0) {
    throw "The local Delta pipeline failed."
}

Write-Host ""
Write-Host "MinIO objects: http://localhost:9001"
Write-Host "  username: insights"
Write-Host "  password: insights-local"
Write-Host "Jupyter Lab:  http://localhost:8888"
Write-Host "Dashboard:    http://localhost:$DashboardPort"
