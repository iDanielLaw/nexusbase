# Dockerfile for the performance test client.
# This creates a minimal image containing only the client binary.

# Use a multi-stage build to keep the final image small.
# Stage 1: Builder
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy dependency files and download them first to leverage Docker's layer caching.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application's source code.
COPY . .

# Build the perf-client binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o /perf-client ./cmd/perf-client

# Stage 2: Runner
# Use a minimal image for the final stage.
FROM alpine:latest

WORKDIR /app

# Copy the binary from the builder stage.
COPY --from=builder /perf-client .

# The entrypoint is the client itself. The command will be provided during `docker run`.
ENTRYPOINT ["./perf-client"]