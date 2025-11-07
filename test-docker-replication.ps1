# Test Docker Replication
# This script tests NexusBase replication using Docker Compose

param(
    [Parameter(Mandatory=$false)]
    [int]$NumPoints = 10,
    
    [Parameter(Mandatory=$false)]
    [switch]$CleanStart = $false
)

$ErrorActionPreference = "Stop"

Write-Host "`n=== NexusBase Docker Replication Test ===" -ForegroundColor Cyan
Write-Host ""

# Clean start if requested
if ($CleanStart) {
    Write-Host "Cleaning up existing containers and volumes..." -ForegroundColor Yellow
    docker-compose -f docker-compose-replication.yaml down -v 2>$null
    Write-Host "✓ Cleanup complete" -ForegroundColor Green
    Write-Host ""
}

# Start services
Write-Host "Starting NexusBase Leader and Follower..." -ForegroundColor Cyan
docker-compose -f docker-compose-replication.yaml up -d --build

if ($LASTEXITCODE -ne 0) {
    Write-Host "✗ Failed to start services!" -ForegroundColor Red
    exit 1
}

Write-Host "✓ Services started" -ForegroundColor Green
Write-Host ""

# Wait for services to be ready
Write-Host "Waiting for services to be ready..." -ForegroundColor Cyan
$maxWait = 30
$waited = 0

while ($waited -lt $maxWait) {
    $leaderHealth = docker inspect nexusbase-leader --format='{{.State.Health.Status}}' 2>$null
    $followerHealth = docker inspect nexusbase-follower --format='{{.State.Health.Status}}' 2>$null
    
    if ($leaderHealth -eq "healthy" -and $followerHealth -eq "healthy") {
        Write-Host "✓ Both services are healthy" -ForegroundColor Green
        break
    }
    
    Write-Host "." -NoNewline
    Start-Sleep -Seconds 2
    $waited += 2
}

if ($waited -ge $maxWait) {
    Write-Host "`n✗ Services did not become healthy in time" -ForegroundColor Red
    Write-Host "`nLeader logs:" -ForegroundColor Yellow
    docker-compose -f docker-compose-replication.yaml logs --tail=20 nexusbase-leader
    Write-Host "`nFollower logs:" -ForegroundColor Yellow
    docker-compose -f docker-compose-replication.yaml logs --tail=20 nexusbase-follower
    exit 1
}

Write-Host ""

# Send data to Leader
Write-Host "Sending $NumPoints data points to Leader..." -ForegroundColor Cyan
$baseTimestamp = [int][double]::Parse((Get-Date -UFormat %s))
$sent = 0

for ($i = 0; $i -lt $NumPoints; $i++) {
    $timestamp = $baseTimestamp + $i
    $value = 50 + ($i * 0.5)
    
    $result = grpcurl -plaintext -d "{
        `"metric`": `"docker.test`",
        `"tags`": {`"container`": `"leader`", `"test`": `"replication`"},
        `"timestamp`": $timestamp,
        `"fields`": {`"value`": $value, `"count`": $i}
    }" localhost:50051 tsdb.TSDBService.PutEvent 2>&1
    
    if ($LASTEXITCODE -eq 0) {
        $sent++
    } else {
        Write-Host "✗ Failed to send point $i" -ForegroundColor Red
    }
}

Write-Host "✓ Sent $sent/$NumPoints points to Leader" -ForegroundColor Green
Write-Host ""

# Wait for replication
Write-Host "Waiting for replication..." -ForegroundColor Cyan
Start-Sleep -Seconds 3
Write-Host "✓ Replication delay complete" -ForegroundColor Green
Write-Host ""

# Query from Leader
Write-Host "Querying from Leader..." -ForegroundColor Cyan
$leaderResult = grpcurl -plaintext -d "{
    `"metric`": `"docker.test`",
    `"tags`": {},
    `"start_time`": $($baseTimestamp - 10),
    `"end_time`": $($baseTimestamp + $NumPoints + 10)
}" localhost:50051 tsdb.TSDBService.Query 2>&1

# Count points from Leader
$leaderCount = ($leaderResult | Select-String -Pattern '"timestamp"' -AllMatches).Matches.Count
Write-Host "Leader returned: $leaderCount points" -ForegroundColor Cyan

# Query from Follower
Write-Host "Querying from Follower..." -ForegroundColor Cyan
$followerResult = grpcurl -plaintext -d "{
    `"metric`": `"docker.test`",
    `"tags`": {},
    `"start_time`": $($baseTimestamp - 10),
    `"end_time`": $($baseTimestamp + $NumPoints + 10)
}" localhost:50055 tsdb.TSDBService.Query 2>&1

# Count points from Follower
$followerCount = ($followerResult | Select-String -Pattern '"timestamp"' -AllMatches).Matches.Count
Write-Host "Follower returned: $followerCount points" -ForegroundColor Cyan
Write-Host ""

# Verify replication
if ($followerCount -eq $leaderCount -and $followerCount -eq $NumPoints) {
    Write-Host "✓✓✓ REPLICATION SUCCESSFUL! ✓✓✓" -ForegroundColor Green
    Write-Host "  All $NumPoints points replicated correctly" -ForegroundColor Green
    Write-Host "  Leader: $leaderCount points" -ForegroundColor Green
    Write-Host "  Follower: $followerCount points" -ForegroundColor Green
    $exitCode = 0
} elseif ($followerCount -eq $leaderCount) {
    Write-Host "⚠ PARTIAL SUCCESS ⚠" -ForegroundColor Yellow
    Write-Host "  Replication working but count mismatch" -ForegroundColor Yellow
    Write-Host "  Expected: $NumPoints points" -ForegroundColor Yellow
    Write-Host "  Leader: $leaderCount points" -ForegroundColor Yellow
    Write-Host "  Follower: $followerCount points" -ForegroundColor Yellow
    $exitCode = 1
} else {
    Write-Host "✗✗✗ REPLICATION FAILED! ✗✗✗" -ForegroundColor Red
    Write-Host "  Leader: $leaderCount points" -ForegroundColor Red
    Write-Host "  Follower: $followerCount points" -ForegroundColor Red
    Write-Host "  Difference: $($leaderCount - $followerCount) points" -ForegroundColor Red
    $exitCode = 1
}

Write-Host ""
Write-Host "=== Additional Information ===" -ForegroundColor Cyan

# Show container stats
Write-Host "`nContainer Status:" -ForegroundColor Cyan
docker-compose -f docker-compose-replication.yaml ps

# Show metrics URLs
Write-Host "`nMetrics Endpoints:" -ForegroundColor Cyan
Write-Host "  Leader:   http://localhost:6060/debug/vars" -ForegroundColor White
Write-Host "  Follower: http://localhost:6061/debug/vars" -ForegroundColor White

# Show query UIs
Write-Host "`nQuery UIs:" -ForegroundColor Cyan
Write-Host "  Leader:   http://localhost:8088/query" -ForegroundColor White
Write-Host "  Follower: http://localhost:8089/query" -ForegroundColor White

# Show logs command
Write-Host "`nView Logs:" -ForegroundColor Cyan
Write-Host "  docker-compose -f docker-compose-replication.yaml logs -f" -ForegroundColor White

# Show cleanup command
Write-Host "`nCleanup:" -ForegroundColor Cyan
Write-Host "  docker-compose -f docker-compose-replication.yaml down -v" -ForegroundColor White

Write-Host ""
exit $exitCode
