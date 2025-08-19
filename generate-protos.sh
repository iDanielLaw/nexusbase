#!/bin/bash

# This script generates Go gRPC server code from the .proto files.
# It should be run from the root of the project directory.

set -e

echo "Checking for prerequisites..."

# Check if protoc is installed
if ! command -v protoc &> /dev/null
then
    echo "protoc could not be found. Please install the protobuf compiler."
    echo "See: https://grpc.io/docs/protoc-installation/"
    exit 1
fi

# Define the source directory
API_SOURCE_DIR="api/v1"

echo "Generating Go code for ${API_SOURCE_DIR}/replication.proto..."
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       "${API_SOURCE_DIR}/replication.proto"

echo "Proto files generated successfully."
echo "Generated files are in ${API_SOURCE_DIR}/"

