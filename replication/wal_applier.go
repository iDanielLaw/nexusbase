package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// ReplicatedEngine defines the interface for a storage engine
// that can have replicated changes applied to it.
type ReplicatedEngine interface {
	// ApplyReplicatedEntry applies a change from the leader without writing to its own WAL.
	ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error
	// GetLatestAppliedSeqNum returns the latest sequence number that has been applied.
	GetLatestAppliedSeqNum() uint64
	// Close closes the engine.
	Close() error
	// ReplaceWithSnapshot replaces the entire engine state with the contents of a snapshot.
	ReplaceWithSnapshot(snapshotDir string) error
}

// WALApplier connects to a leader, streams WAL entries, and applies them to its own engine.
type WALApplier struct {
	leaderAddr  string
	engine      ReplicatedEngine
	snapshotMgr snapshot.ManagerInterface
	snapshotDir string // Directory to store downloaded snapshots
	logger      *slog.Logger
	conn        *grpc.ClientConn
	client      pb.ReplicationServiceClient
	cancel      context.CancelFunc
	dialOpts    []grpc.DialOption
	retrySleep  time.Duration
}

// NewWALApplier creates a new WAL applier.
func NewWALApplier(leaderAddr string, engine ReplicatedEngine, snapshotMgr snapshot.ManagerInterface, snapshotDir string, logger *slog.Logger) *WALApplier {
	return &WALApplier{
		leaderAddr:  leaderAddr,
		engine:      engine,
		snapshotMgr: snapshotMgr,
		snapshotDir: snapshotDir,
		logger:      logger.With("component", "wal_applier", "leader", leaderAddr),
		retrySleep:  6 * time.Second, // Default retry sleep
	}
}

// Start begins the process of connecting to and replicating from the leader.
func (a *WALApplier) Start(ctx context.Context) {
	a.logger.Info("Starting WAL Applier")

	applierCtx, cancel := context.WithCancel(ctx)

	opts := a.dialOpts
	if opts == nil {
		opts = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  1.0 * time.Second,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   120 * time.Second,
				},
				MinConnectTimeout: 20 * time.Second,
			}),
		}
	}

	conn, err := grpc.DialContext(applierCtx, a.leaderAddr, opts...)
	if err != nil {
		a.logger.Error("Failed to dial leader, WAL applier will not run", "error", err)
		cancel()
		return
	}

	a.cancel = cancel
	a.conn = conn
	a.client = pb.NewReplicationServiceClient(conn)

	a.logger.Info("Successfully connected to leader")

	// Run the main management loop in a goroutine.
	go func() {
		err := a.manageReplication(applierCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Error("Replication management loop failed", "error", err)
			a.Stop()
		}
	}()
}

// manageReplication is the high-level loop that decides whether to bootstrap or stream.
func (a *WALApplier) manageReplication(ctx context.Context) error {
	if err := a.bootstrap(ctx); err != nil {
		return fmt.Errorf("bootstrap failed: %w", err)
	}

	return a.replicationLoop(ctx)
}

// bootstrap checks if a snapshot is needed and performs the restore if so.
func (a *WALApplier) bootstrap(ctx context.Context) error {
	a.logger.Info("Starting bootstrap process...")

	leaderSnapshotInfo, err := a.client.GetLatestSnapshotInfo(ctx, &pb.GetLatestSnapshotInfoRequest{})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			a.logger.Info("Leader has no snapshots, proceeding with WAL streaming.")
			return nil
		}
		return fmt.Errorf("failed to get latest snapshot info from leader: %w", err)
	}
	a.logger.Info("Got latest snapshot info from leader", "snapshot_id", leaderSnapshotInfo.Id, "snapshot_seq_num", leaderSnapshotInfo.LastWalSequenceNumber)

	localSeqNum := a.engine.GetLatestAppliedSeqNum()
	if localSeqNum >= leaderSnapshotInfo.LastWalSequenceNumber {
		a.logger.Info("Local state is up-to-date or ahead of the latest snapshot, no bootstrap needed.", "local_seq_num", localSeqNum)
		return nil
	}

	a.logger.Info("Local state is behind leader's snapshot, starting snapshot download.", "local_seq_num", localSeqNum, "snapshot_seq_num", leaderSnapshotInfo.LastWalSequenceNumber)

	if err := a.downloadAndRestoreSnapshot(ctx, leaderSnapshotInfo.Id); err != nil {
		return fmt.Errorf("snapshot download and restore failed: %w", err)
	}

	a.logger.Info("Bootstrap process completed successfully.")
	return nil
}

// downloadAndRestoreSnapshot handles the entire process of getting a snapshot from the leader.
func (a *WALApplier) downloadAndRestoreSnapshot(ctx context.Context, snapshotId string) error {
	tempSnapDir := filepath.Join(a.snapshotDir, fmt.Sprintf("download-%s-%d", snapshotId, time.Now().UnixNano()))
	if err := os.MkdirAll(tempSnapDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp snapshot dir: %w", err)
	}
	defer os.RemoveAll(tempSnapDir)

	a.logger.Info("Streaming snapshot from leader", "snapshot_id", snapshotId, "temp_dir", tempSnapDir)

	stream, err := a.client.StreamSnapshot(ctx, &pb.StreamSnapshotRequest{Id: snapshotId})
	if err != nil {
		return fmt.Errorf("failed to start snapshot stream: %w", err)
	}

	var currentFile *os.File
	var currentPath string
	var currentHasher = sha256.New()
	var expectedChecksum string

	finalizeFile := func() error {
		if currentFile == nil {
			return nil
		}
		// Verify checksum of the previous file
		actualChecksum := hex.EncodeToString(currentHasher.Sum(nil))
		if actualChecksum != expectedChecksum {
			currentFile.Close()
			return fmt.Errorf("checksum mismatch for file %s: expected %s, got %s", currentPath, expectedChecksum, actualChecksum)
		}
		a.logger.Info("File verified", "path", currentPath, "checksum", actualChecksum)
		return currentFile.Close()
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			if err := finalizeFile(); err != nil {
				return err
			}
			break // Stream finished successfully
		}
		if err != nil {
			if currentFile != nil {
				currentFile.Close()
			}
			return fmt.Errorf("error receiving snapshot chunk: %w", err)
		}

		switch content := chunk.Content.(type) {
		case *pb.SnapshotChunk_FileInfo:
			if err := finalizeFile(); err != nil {
				return err
			}

			currentPath = filepath.Join(tempSnapDir, content.FileInfo.Path)
			expectedChecksum = content.FileInfo.Checksum
			currentHasher.Reset()

			if err := os.MkdirAll(filepath.Dir(currentPath), 0755); err != nil {
				return fmt.Errorf("failed to create dir for snapshot file: %w", err)
			}
			currentFile, err = os.OpenFile(currentPath, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return fmt.Errorf("failed to create snapshot file %s: %w", currentPath, err)
			}
		case *pb.SnapshotChunk_ChunkData:
			if currentFile == nil {
				return errors.New("received chunk data before file info")
			}
			// Write to both file and hasher
			if _, err := io.MultiWriter(currentFile, currentHasher).Write(content.ChunkData); err != nil {
				currentFile.Close()
				return fmt.Errorf("failed to write to snapshot file %s: %w", currentPath, err)
			}
		}
	}

	a.logger.Info("Snapshot download complete. Restoring engine state.")

	if err := a.engine.Close(); err != nil {
		return fmt.Errorf("failed to close engine before snapshot restore: %w", err)
	}

	if err := a.engine.ReplaceWithSnapshot(tempSnapDir); err != nil {
		return fmt.Errorf("failed to restore engine from snapshot: %w", err)
	}

	a.logger.Info("Successfully restored from snapshot.")
	return nil
}

// replicationLoop is the main loop for streaming and applying WAL entries.
func (a *WALApplier) replicationLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			a.logger.Info("Replication loop stopping due to context cancellation.")
			return ctx.Err()
		default:
			fromSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
			a.logger.Info("Starting WAL stream", "from_seq_num", fromSeqNum)

			req := &pb.StreamWALRequest{
				FromSequenceNumber: fromSeqNum,
			}

			stream, err := a.client.StreamWAL(ctx, req)
			if err != nil {
				a.logger.Error("Failed to start WAL stream, will retry...", "error", err)
				time.Sleep(a.retrySleep)
				continue
			}

			err = a.processStream(ctx, stream)
			if err != nil {
				if errors.Is(err, io.EOF) {
					a.logger.Info("WAL stream ended gracefully, will attempt to re-establish.")
					time.Sleep(500 * time.Millisecond)
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					a.logger.Info("Replication loop stopping due to context cancellation or deadline.", "error", err)
					return err
				}
				a.logger.Error("WAL stream broke with a critical error, stopping replication loop.", "error", err)
				a.Stop() // Self-terminate on critical error
				return err
			}
		}
	}
}

// processStream reads from the gRPC stream and applies entries.
func (a *WALApplier) processStream(ctx context.Context, stream pb.ReplicationService_StreamWALClient) error {
	const maxApplyRetries = 3
	for {
		entry, err := stream.Recv()
		if err != nil {
			return err
		}

		expectedSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
		if entry.GetSequenceNumber() != expectedSeqNum {
			return fmt.Errorf("received out-of-order WAL entry: expected %d, got %d", expectedSeqNum, entry.GetSequenceNumber())
		}

		var applyErr error
		for i := 0; i < maxApplyRetries; i++ {
			applyErr = a.engine.ApplyReplicatedEntry(ctx, entry)
			if applyErr == nil {
				break // Success
			}
			a.logger.Error("Failed to apply replicated entry, retrying...",
				"error", applyErr,
				"seq_num", entry.GetSequenceNumber(),
				"attempt", i+1,
				"max_attempts", maxApplyRetries,
			)
			time.Sleep(1 * time.Second)
		}

		if applyErr != nil {
			a.logger.Error("Failed to apply replicated entry after multiple retries, stopping replication.",
				"error", applyErr,
				"seq_num", entry.GetSequenceNumber(),
			)
			return fmt.Errorf("failed to apply replicated entry %d after %d retries: %w",
				entry.GetSequenceNumber(), maxApplyRetries, applyErr)
		}
	}
}

// Stop gracefully shuts down the WAL applier.
func (a *WALApplier) Stop() {
	a.logger.Info("Stopping WAL Applier")
	if a.cancel != nil {
		a.cancel()
	}
	if a.conn != nil {
		a.conn.Close()
	}
}
