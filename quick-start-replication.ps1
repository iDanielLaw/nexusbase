param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("leader", "follower", "both", "build", "clean")]
    [string]$Mode = "both",
    [Parameter(Mandatory=$false)]
    [switch]$WithTLS = $false
)
$ErrorActionPreference = "Stop"
function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Build-NexusBase {
    Write-Info "Building NexusBase server..."
    go build -o bin/nexusbase.exe ./cmd/server
    if ($LASTEXITCODE -ne 0) { Write-Host "Build failed!" -ForegroundColor Red; exit 1 }
    Write-Success "Build successful"
}
function Clean-Data {
    Write-Info "Cleaning up data directories..."
    if (Test-Path "data-leader") { Remove-Item -Recurse -Force data-leader; Write-Success "Removed data-leader" }
    if (Test-Path "data-follower") { Remove-Item -Recurse -Force data-follower; Write-Success "Removed data-follower" }
}
function Start-Leader {
    param([bool]$UseTLS = $false)
    $configFile = if ($UseTLS) { "dev/config-leader-tls.yaml" } else { "config-test-leader.yaml" }
    Write-Info "Starting Leader node..."
    Write-Info "Config: $configFile"
    Write-Info "Ports: gRPC=50051, TCP=50052, Replication=50053, Debug=6060, Query=8088"
    Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
    & .\bin\nexusbase.exe --config=$configFile
}
function Start-Follower {
    param([bool]$UseTLS = $false)
    $configFile = if ($UseTLS) { "dev/config-follower-tls.yaml" } else { "config-test-follower.yaml" }
    Write-Info "Starting Follower node..."
    Write-Info "Config: $configFile"
    Write-Info "Ports: gRPC=50055, TCP=50056, Debug=6061, Query=8089"
    Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Yellow
    & .\bin\nexusbase.exe --config=$configFile
}
switch ($Mode) {
    "build" { Build-NexusBase }
    "clean" { Clean-Data }
    "leader" {
        if (-not (Test-Path "bin/nexusbase.exe")) { Write-Host "Building..." -ForegroundColor Yellow; Build-NexusBase }
        if ($WithTLS -and -not (Test-Path "dev/certs/replication.crt")) {
            Write-Host "Generating TLS certs..." -ForegroundColor Yellow
            Push-Location dev; .\generate-certs.ps1; Pop-Location
        }
        Start-Leader -UseTLS $WithTLS
    }
    "follower" {
        if (-not (Test-Path "bin/nexusbase.exe")) { Write-Host "Server binary not found. Run with -Mode build first!" -ForegroundColor Red; exit 1 }
        if ($WithTLS -and -not (Test-Path "dev/certs/replication.crt")) { Write-Host "TLS certs not found!" -ForegroundColor Red; exit 1 }
        Start-Follower -UseTLS $WithTLS
    }
    "both" {
        if (-not (Test-Path "bin/nexusbase.exe")) { Write-Host "Building..." -ForegroundColor Yellow; Build-NexusBase }
        if ($WithTLS -and -not (Test-Path "dev/certs/replication.crt")) {
            Write-Host "Generating TLS certs..." -ForegroundColor Yellow
            Push-Location dev; .\generate-certs.ps1; Pop-Location
        }
        Write-Info "Starting Leader... Open another terminal and run: .\quick-start-replication.ps1 -Mode follower"
        Start-Sleep -Seconds 2
        Start-Leader -UseTLS $WithTLS
    }
}
