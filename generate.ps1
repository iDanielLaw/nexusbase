# This script generates Go code from the .proto files in the project.

# Ensure protoc is installed and in your PATH.
# Ensure the Go protobuf plugins are installed:
# go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
# go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Define the base directory
$baseDir = Get-Location

# Define the proto file path relative to the base directory
$protoFile = "replication\proto\replication.proto"

# Construct the full path to the proto file
$fullProtoPath = Join-Path -Path $baseDir -ChildPath $protoFile

# Check if the proto file exists
if (-not (Test-Path $fullProtoPath)) {
    Write-Error "Proto file not found: $fullProtoPath"
    exit 1
}

# Define the output directory (relative to the proto file's directory)
$outputDir = "."

Write-Host "Generating Go code for $protoFile..."

# Run the protoc command
# We specify --proto_path to the project root so that protoc can resolve the file's path correctly.
protoc --proto_path=$baseDir --go_out=$outputDir --go_opt=paths=source_relative --go-grpc_out=$outputDir --go-grpc_opt=paths=source_relative $fullProtoPath

if ($LASTEXITCODE -eq 0) {
    Write-Host "Successfully generated Go code."
} else {
    Write-Error "Failed to generate Go code. Please check your protoc installation and paths."
}
