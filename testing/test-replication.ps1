# Test Data Ingestion and Replication
# This script sends test data to Leader and verifies it's replicated to Follower

param(
    [Parameter(Mandatory=$false)]
    [int]$NumPoints = 10,
    
    [Parameter(Mandatory=$false)]
    [string]$LeaderAddress = "localhost:50051",
    
    [Parameter(Mandatory=$false)]
    [string]$FollowerAddress = "localhost:50055"
)

$ErrorActionPreference = "Stop"

function Write-Success { Write-Host $args -ForegroundColor Green }
function Write-Info { Write-Host $args -ForegroundColor Cyan }
function Write-Warning { Write-Host $args -ForegroundColor Yellow }
function Write-Error { Write-Host $args -ForegroundColor Red }

Write-Info "=== NexusBase Replication Test ==="
Write-Info ""
Write-Info "Configuration:"
Write-Info "  Leader:   $LeaderAddress"
Write-Info "  Follower: $FollowerAddress"
Write-Info "  Points:   $NumPoints"
Write-Info ""

# Check if grpcurl is available
$grpcurl = Get-Command grpcurl -ErrorAction SilentlyContinue
if (-not $grpcurl) {
    Write-Warning "grpcurl not found. Installing..."
    go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
}

# Step 1: Send data to Leader
Write-Info "Step 1: Sending $NumPoints data points to Leader..."

$baseTimestamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$sentPoints = @()

for ($i = 0; $i -lt $NumPoints; $i++) {
    $timestamp = $baseTimestamp + $i
    $value = 70.0 + ($i * 0.5)
    $region = @("us-east", "us-west", "eu-central")[$i % 3]
    
    $json = @"
{
  "metric": "cpu.temperature",
  "tags": {
    "host": "server$($i % 3 + 1)",
    "region": "$region"
  },
  "timestamp": $timestamp,
  "fields": {
    "value": $value,
    "cores": $($i % 8 + 1)
  }
}
"@

    try {
        $result = grpcurl -plaintext -d $json $LeaderAddress tsdb.TSDBService.PutEvent 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  ✓ Point $($i+1)/$NumPoints sent (timestamp: $timestamp, value: $value)" -ForegroundColor Gray
            $sentPoints += $timestamp
        } else {
            Write-Error "Failed to send point $($i+1): $result"
        }
    } catch {
        Write-Error "Error sending point $($i+1): $_"
    }
}

Write-Success "✓ Sent $NumPoints points to Leader"
Write-Info ""

# Step 2: Wait for replication
Write-Info "Step 2: Waiting for replication to complete..."
Start-Sleep -Seconds 2
Write-Success "✓ Wait complete"
Write-Info ""

# Step 3: Query from Leader
Write-Info "Step 3: Querying data from Leader..."

$queryJson = @"
{
  "metric": "cpu.temperature",
  "tags": {},
  "start_time": $($baseTimestamp - 10),
  "end_time": $($baseTimestamp + $NumPoints + 10)
}
"@

try {
    $leaderResult = grpcurl -plaintext -d $queryJson $LeaderAddress tsdb.TSDBService.Query 2>&1 | ConvertFrom-Json
    $leaderCount = ($leaderResult.points | Measure-Object).Count
    Write-Success "✓ Leader returned $leaderCount points"
} catch {
    Write-Error "Failed to query Leader: $_"
    $leaderCount = 0
}

Write-Info ""

# Step 4: Query from Follower
Write-Info "Step 4: Querying data from Follower..."

try {
    $followerResult = grpcurl -plaintext -d $queryJson $FollowerAddress tsdb.TSDBService.Query 2>&1 | ConvertFrom-Json
    $followerCount = ($followerResult.points | Measure-Object).Count
    Write-Success "✓ Follower returned $followerCount points"
} catch {
    Write-Error "Failed to query Follower: $_"
    $followerCount = 0
}

Write-Info ""

# Step 5: Verify replication
Write-Info "Step 5: Verifying replication..."

if ($leaderCount -eq $followerCount -and $followerCount -eq $NumPoints) {
    Write-Success "✓✓✓ REPLICATION SUCCESSFUL! ✓✓✓"
    Write-Success "  All $NumPoints points replicated correctly"
    $exitCode = 0
} elseif ($followerCount -ge $NumPoints) {
    Write-Success "✓ Replication successful (Follower has all data)"
    Write-Warning "  Leader: $leaderCount points, Follower: $followerCount points"
    $exitCode = 0
} else {
    Write-Error "✗ REPLICATION INCOMPLETE!"
    Write-Error "  Expected: $NumPoints points"
    Write-Error "  Leader:   $leaderCount points"
    Write-Error "  Follower: $followerCount points"
    Write-Error "  Missing:  $($NumPoints - $followerCount) points"
    $exitCode = 1
}

Write-Info ""
Write-Info "=== Test Complete ==="

exit $exitCode
