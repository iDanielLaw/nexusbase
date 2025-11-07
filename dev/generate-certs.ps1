# PowerShell script to generate TLS certificates for NexusBase on Windows
# This creates self-signed certificates for development/testing purposes
# For production, use certificates from a trusted CA

$ErrorActionPreference = "Stop"

$CERT_DIR = "certs"
$DAYS_VALID = 365
$CA_DAYS_VALID = 3650

Write-Host "=== NexusBase TLS Certificate Generator ===" -ForegroundColor Cyan
Write-Host ""

# Check if OpenSSL is available
try {
    $null = Get-Command openssl -ErrorAction Stop
} catch {
    Write-Host "ERROR: OpenSSL is not installed or not in PATH" -ForegroundColor Red
    Write-Host "Please install OpenSSL from: https://slproweb.com/products/Win32OpenSSL.html" -ForegroundColor Yellow
    exit 1
}

# Create certificate directory
if (-not (Test-Path $CERT_DIR)) {
    New-Item -ItemType Directory -Path $CERT_DIR | Out-Null
}
Set-Location $CERT_DIR

Write-Host "Step 1: Generating CA certificate..." -ForegroundColor Yellow
# Generate CA private key
& openssl genrsa -out ca.key 4096 2>$null

# Generate CA certificate
& openssl req -new -x509 -days $CA_DAYS_VALID -key ca.key -out ca.crt `
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=NexusBase CA" `
  2>$null

Write-Host "✓ CA certificate generated (valid for $CA_DAYS_VALID days)" -ForegroundColor Green
Write-Host ""

Write-Host "Step 2: Generating server certificate..." -ForegroundColor Yellow
# Generate server private key
& openssl genrsa -out server.key 2048 2>$null

# Create certificate signing request
& openssl req -new -key server.key -out server.csr `
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=localhost" `
  2>$null

# Sign with CA
& openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key `
  -CAcreateserial -out server.crt -days $DAYS_VALID `
  2>$null

Write-Host "✓ Server certificate generated (valid for $DAYS_VALID days)" -ForegroundColor Green
Write-Host ""

Write-Host "Step 3: Generating replication certificate..." -ForegroundColor Yellow
# Generate replication private key
& openssl genrsa -out replication.key 2048 2>$null

# Create certificate signing request
& openssl req -new -key replication.key -out replication.csr `
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=replication.nexusbase.local" `
  2>$null

# Sign with CA
& openssl x509 -req -in replication.csr -CA ca.crt -CAkey ca.key `
  -CAcreateserial -out replication.crt -days $DAYS_VALID `
  2>$null

Write-Host "✓ Replication certificate generated (valid for $DAYS_VALID days)" -ForegroundColor Green
Write-Host ""

Write-Host "Step 4: Cleaning up temporary files..." -ForegroundColor Yellow
Remove-Item -Path "*.csr", "*.srl" -ErrorAction SilentlyContinue

Write-Host "✓ Cleanup complete" -ForegroundColor Green
Write-Host ""

Write-Host "=== Certificate Generation Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Generated files in ${CERT_DIR}:" -ForegroundColor White
Write-Host "  - ca.crt, ca.key           (Certificate Authority)" -ForegroundColor Gray
Write-Host "  - server.crt, server.key   (Main server)" -ForegroundColor Gray
Write-Host "  - replication.crt, replication.key (Replication)" -ForegroundColor Gray
Write-Host ""
Write-Host "Certificate details:" -ForegroundColor White
& openssl x509 -in server.crt -noout -subject -issuer -dates
Write-Host ""
Write-Host "⚠️  IMPORTANT SECURITY NOTES:" -ForegroundColor Yellow
Write-Host "  1. These are self-signed certificates for DEVELOPMENT/TESTING only" -ForegroundColor Yellow
Write-Host "  2. For PRODUCTION, obtain certificates from a trusted CA" -ForegroundColor Yellow
Write-Host "  3. Never commit *.key files to version control" -ForegroundColor Yellow
Write-Host "  4. Store private keys securely" -ForegroundColor Yellow
Write-Host ""
Write-Host "To verify certificates:" -ForegroundColor White
Write-Host "  openssl verify -CAfile ca.crt server.crt" -ForegroundColor Gray
Write-Host "  openssl verify -CAfile ca.crt replication.crt" -ForegroundColor Gray
Write-Host ""

Set-Location ..
