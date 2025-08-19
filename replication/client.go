package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
)

// Client handles the gRPC communication from a Follower to a Leader.
type Client struct {
	conn       *grpc.ClientConn
	grpcClient apiv1.ReplicationServiceClient
	logger     *slog.Logger
}

// NewClient creates a new replication client.
func NewClient(leaderAddr string, logger *slog.Logger) (*Client, error) {
	// In a real application, use secure credentials.
	// For this example, we use insecure credentials.
	conn, err := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to leader at %s: %w", leaderAddr, err)
	}

	return &Client{
		conn:       conn,
		grpcClient: apiv1.NewReplicationServiceClient(conn),
		logger:     logger.With("component", "ReplicationClient"),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ReceiveSnapshot connects to the leader, requests a full snapshot, and streams it
// into the specified destination directory. The destination directory will be created
// if it doesn't exist.
func (c *Client) ReceiveSnapshot(ctx context.Context, destinationDir string) error {
	c.logger.Info("Requesting full snapshot from leader...", "destination", destinationDir)

	// Ensure the destination directory exists.
	if err := os.MkdirAll(destinationDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", destinationDir, err)
	}

	// 1. Call the GetSnapshot RPC.
	stream, err := c.grpcClient.GetSnapshot(ctx, &apiv1.SnapshotRequest{})
	if err != nil {
		return fmt.Errorf("failed to initiate snapshot stream: %w", err)
	}

	// 2. Manage open file handles to avoid re-opening for every chunk.
	openFiles := make(map[string]*os.File)
	defer func() {
		for _, file := range openFiles {
			file.Close()
		}
	}()

	// 3. Loop to receive chunks and reconstruct files.
	for {
		chunk, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				c.logger.Info("Snapshot stream finished successfully.")
				break // End of stream, success.
			}
			// Any other error is a failure.
			return fmt.Errorf("error receiving snapshot chunk: %w", err)
		}

		// Get or open the file handle for this chunk's file.
		file, ok := openFiles[chunk.FilePathRelative]
		if !ok {
			destPath := filepath.Join(destinationDir, chunk.FilePathRelative)
			// Ensure subdirectory exists (e.g., "sst/")
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return fmt.Errorf("failed to create snapshot subdirectory %s: %w", filepath.Dir(destPath), err)
			}

			// Open the file for writing. O_APPEND is crucial as chunks for the same file arrive sequentially.
			file, err = os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return fmt.Errorf("failed to open file for snapshot chunk %s: %w", destPath, err)
			}
			openFiles[chunk.FilePathRelative] = file
			c.logger.Debug("Opened new file for snapshot chunk", "path", destPath)
		}

		// Write the chunk content to the file.
		if _, err := file.Write(chunk.Content); err != nil {
			return fmt.Errorf("failed to write snapshot chunk to %s: %w", file.Name(), err)
		}
	}

	c.logger.Info("All snapshot files received and written to disk.", "destination", destinationDir)
	return nil
}

// StreamWAL connects to the leader and starts streaming WAL entries from a given sequence number.
// It returns a read-only channel for WAL entries, a read-only channel for a potential terminal error,
// and an initial error if the stream could not be established.
// The caller is responsible for handling the context cancellation, which will terminate the stream.
func (c *Client) StreamWAL(ctx context.Context, fromSeqNum uint64) (<-chan *apiv1.WALEntry, <-chan error, error) {
	c.logger.Info("Requesting WAL stream from leader...", "from_sequence_number", fromSeqNum)

	req := &apiv1.StreamWALRequest{
		FromSequenceNumber: fromSeqNum,
	}

	stream, err := c.grpcClient.StreamWAL(ctx, req)
	if err != nil {
		c.logger.Error("Failed to initiate WAL stream", "error", err)
		return nil, nil, fmt.Errorf("failed to initiate WAL stream: %w", err)
	}

	entryChan := make(chan *apiv1.WALEntry, 100) // Buffered channel for entries
	errChan := make(chan error, 1)               // Buffered channel for the final error

	// Start a goroutine to continuously receive from the stream
	go func() {
		defer close(entryChan)
		defer close(errChan)

		for {
			entry, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					c.logger.Info("WAL stream finished cleanly (EOF).")
				} else if status.Code(err) == codes.Canceled || ctx.Err() != nil {
					c.logger.Info("WAL stream cancelled by context.")
				} else {
					c.logger.Error("Error receiving from WAL stream", "error", err)
					errChan <- err
				}
				return // Exit the goroutine
			}

			select {
			case entryChan <- entry:
			case <-ctx.Done():
				c.logger.Info("Context cancelled while sending WAL entry.")
				return
			}
		}
	}()

	return entryChan, errChan, nil
}
