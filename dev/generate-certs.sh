#!/bin/bash

# Script to generate TLS certificates for NexusBase
# This creates self-signed certificates for development/testing purposes
# For production, use certificates from a trusted CA

set -e

CERT_DIR="certs"
DAYS_VALID=365
CA_DAYS_VALID=3650

echo "=== NexusBase TLS Certificate Generator ==="
echo ""

# Create certificate directory
mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

echo "Step 1: Generating CA certificate..."
# Generate CA private key
openssl genrsa -out ca.key 4096 2>/dev/null

# Generate CA certificate
openssl req -new -x509 -days $CA_DAYS_VALID -key ca.key -out ca.crt \
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=NexusBase CA" \
  2>/dev/null

echo "✓ CA certificate generated (valid for $CA_DAYS_VALID days)"
echo ""

echo "Step 2: Generating server certificate..."
# Generate server private key
openssl genrsa -out server.key 2048 2>/dev/null

# Create certificate signing request
openssl req -new -key server.key -out server.csr \
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=localhost" \
  2>/dev/null

# Sign with CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days $DAYS_VALID \
  2>/dev/null

echo "✓ Server certificate generated (valid for $DAYS_VALID days)"
echo ""

echo "Step 3: Generating replication certificate..."
# Generate replication private key
openssl genrsa -out replication.key 2048 2>/dev/null

# Create certificate signing request
openssl req -new -key replication.key -out replication.csr \
  -subj "/C=TH/ST=Bangkok/L=Bangkok/O=NexusBase/OU=IT/CN=replication.nexusbase.local" \
  2>/dev/null

# Sign with CA
openssl x509 -req -in replication.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out replication.crt -days $DAYS_VALID \
  2>/dev/null

echo "✓ Replication certificate generated (valid for $DAYS_VALID days)"
echo ""

echo "Step 4: Setting proper permissions..."
chmod 600 *.key
chmod 644 *.crt

echo "✓ Permissions set"
echo ""

echo "Step 5: Cleaning up temporary files..."
rm -f *.csr *.srl

echo "✓ Cleanup complete"
echo ""

echo "=== Certificate Generation Complete ==="
echo ""
echo "Generated files in $CERT_DIR/:"
echo "  - ca.crt, ca.key           (Certificate Authority)"
echo "  - server.crt, server.key   (Main server)"
echo "  - replication.crt, replication.key (Replication)"
echo ""
echo "Certificate details:"
openssl x509 -in server.crt -noout -subject -issuer -dates
echo ""
echo "⚠️  IMPORTANT SECURITY NOTES:"
echo "  1. These are self-signed certificates for DEVELOPMENT/TESTING only"
echo "  2. For PRODUCTION, obtain certificates from a trusted CA"
echo "  3. Never commit *.key files to version control"
echo "  4. Store private keys securely"
echo ""
echo "To verify certificates:"
echo "  openssl verify -CAfile ca.crt server.crt"
echo "  openssl verify -CAfile ca.crt replication.crt"
echo ""
