package replication

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Follower manages the state of a follower node, handling the entire replication
// process from bootstrapping via snapshot to continuous WAL streaming.
type Follower struct {
	leaderAddr         string
	dataDir            string
	engineOpts         engine.StorageEngineOptions // Store engine options for restarts
	bootstrapThreshold uint64                      // If lag is > this, bootstrap from snapshot.

	mu                sync.RWMutex
	engine            engine.StorageEngineInterface
	client            *Client
	applier           *Applier
	lastAppliedSeqNum uint64

	logger       *slog.Logger
	shutdownChan chan struct{}
	wg           sync.WaitGroup
}

// NewFollower creates and initializes a new Follower instance.
func NewFollower(leaderAddr string, bootstrapThreshold uint64, engineOpts engine.StorageEngineOptions, logger *slog.Logger) (*Follower, error) {
	// Ensure the data directory exists
	if err := os.MkdirAll(engineOpts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", engineOpts.DataDir, err)
	}

	f := &Follower{
		leaderAddr:         leaderAddr,
		dataDir:            engineOpts.DataDir,
		engineOpts:         engineOpts,
		bootstrapThreshold: bootstrapThreshold,
		logger:             logger.With("component", "Follower"),
		shutdownChan:       make(chan struct{}),
	}

	return f, nil
}

// Start initiates the follower's main replication loop. It's non-blocking.
func (f *Follower) Start() error {
	f.logger.Info("Starting follower...")

	// Start the main replication loop in a background goroutine
	f.wg.Add(1)
	go f.runReplicationLoop()

	return nil
}

// Stop gracefully shuts down the follower and its replication loop.
func (f *Follower) Stop() {
	f.logger.Info("Stopping follower...")
	close(f.shutdownChan)

	// Close the client connection to interrupt any active streams
	if f.client != nil {
		f.client.Close()
	}

	f.wg.Wait()

	// Close the engine if it's running
	f.mu.Lock()
	if f.engine != nil {
		f.engine.Close()
		f.engine = nil
	}
	f.mu.Unlock()

	f.logger.Info("Follower stopped.")
}

// runReplicationLoop is the main control loop for the follower.
func (f *Follower) runReplicationLoop() {
	defer f.wg.Done()
	backoff := time.Second

	for {
		select {
		case <-f.shutdownChan:
			return
		default:
		}

		if f.client == nil {
			f.logger.Info("Attempting to connect to leader", "address", f.leaderAddr)
			client, err := NewClient(f.leaderAddr, f.logger)
			if err != nil {
				f.logger.Error("Failed to connect to leader, retrying...", "error", err, "backoff", backoff)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				continue
			}
			f.logger.Info("Successfully connected to leader", "address", f.leaderAddr)
			f.client = client
		}

		if err := f.ensureEngineIsRunning(); err != nil {
			f.logger.Error("Failed to start storage engine, retrying...", "error", err, "backoff", backoff)
			time.Sleep(backoff)
			continue
		}

		backoff = time.Second // Reset backoff on success

		if err := f.syncWithLeader(); err != nil {
			f.logger.Error("Error during synchronization with leader", "error", err)
			f.client.Close()
			f.client = nil
			time.Sleep(backoff)
		}
	}
}

// ensureEngineIsRunning checks if the engine is running and starts it if not.
func (f *Follower) ensureEngineIsRunning() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.engine != nil {
		return nil
	}

	f.logger.Info("Starting local storage engine...")
	eng, err := engine.NewStorageEngine(f.engineOpts)
	if err != nil {
		return fmt.Errorf("failed to create new storage engine instance: %w", err)
	}
	if err := eng.Start(); err != nil {
		return fmt.Errorf("failed to start storage engine: %w", err)
	}

	f.engine = eng
	f.applier = NewApplier(eng, f.logger)
	f.lastAppliedSeqNum = eng.GetSequenceNumber()
	f.logger.Info("Storage engine started", "last_applied_seq_num", f.lastAppliedSeqNum)
	return nil
}

// syncWithLeader determines the replication strategy and executes it.
func (f *Follower) syncWithLeader() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-f.shutdownChan:
			cancel()
		case <-ctx.Done():
		}
	}()

	f.mu.RLock()
	localSeqNum := f.lastAppliedSeqNum
	f.mu.RUnlock()

	leaderState, err := f.client.grpcClient.GetLatestState(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("could not get latest state from leader: %w", err)
	}
	f.logger.Info("Retrieved leader state", "leader_seq_num", leaderState.GetLatestSequenceNumber(), "local_seq_num", localSeqNum)

	// Decide whether to bootstrap or stream WAL
	// Bootstrap if:
	// 1. We have no local data (localSeqNum == 0).
	// 2. We are too far behind the leader (lag > bootstrapThreshold).
	lag := leaderState.GetLatestSequenceNumber() - localSeqNum
	shouldBootstrap := localSeqNum == 0 || (f.bootstrapThreshold > 0 && lag > f.bootstrapThreshold)

	if shouldBootstrap {
		f.logger.Info("Follower state requires bootstrap.", "local_seq_num", localSeqNum, "leader_seq_num", leaderState.GetLatestSequenceNumber(), "lag", lag, "threshold", f.bootstrapThreshold)
		snapshotSeqNum, err := f.bootstrapFromSnapshot(ctx)
		if err != nil {
			return fmt.Errorf("snapshot bootstrap failed: %w", err)
		}
		f.mu.Lock()
		f.lastAppliedSeqNum = snapshotSeqNum
		localSeqNum = snapshotSeqNum
		f.mu.Unlock()
	} else {
		f.logger.Info("Follower state is recent enough, proceeding with WAL catch-up.", "local_seq_num", localSeqNum, "leader_seq_num", leaderState.GetLatestSequenceNumber(), "lag", lag)
	}

	return f.streamAndApplyWAL(ctx)
}

// bootstrapFromSnapshot handles the full snapshot reception and application process.
func (f *Follower) bootstrapFromSnapshot(ctx context.Context) (uint64, error) {
	tempRestoreDir := fmt.Sprintf("%s.restore-tmp-%d", f.dataDir, time.Now().UnixNano())
	defer os.RemoveAll(tempRestoreDir)

	if err := f.client.ReceiveSnapshot(ctx, tempRestoreDir); err != nil {
		return 0, fmt.Errorf("failed to receive snapshot from leader: %w", err)
	}

	manifest, err := snapshot.ReadManifestFromDir(tempRestoreDir)
	if err != nil {
		return 0, fmt.Errorf("snapshot validation failed: %w", err)
	}

	f.mu.Lock()
	if f.engine != nil {
		f.engine.Close()
		f.engine = nil
	}
	f.mu.Unlock()

	if err := os.RemoveAll(f.dataDir); err != nil {
		return 0, fmt.Errorf("failed to remove old data directory: %w", err)
	}
	if err := os.Rename(tempRestoreDir, f.dataDir); err != nil {
		return 0, fmt.Errorf("failed to apply snapshot (rename failed): %w", err)
	}

	if err := f.ensureEngineIsRunning(); err != nil {
		return 0, fmt.Errorf("failed to restart engine after snapshot restore: %w", err)
	}

	f.engine.SetSequenceNumber(manifest.SequenceNumber)
	f.logger.Info("Bootstrap from snapshot complete.", "snapshot_seq_num", manifest.SequenceNumber)
	return manifest.SequenceNumber, nil
}

// streamAndApplyWAL connects to the leader's WAL stream and applies entries.
func (f *Follower) streamAndApplyWAL(ctx context.Context) error {
	f.mu.RLock()
	startSeqNum := f.lastAppliedSeqNum + 1
	f.mu.RUnlock()

	f.logger.Info("Starting WAL stream from leader", "from_seq_num", startSeqNum)

	entryChan, errChan, err := f.client.StreamWAL(ctx, startSeqNum)
	if err != nil {
		// A common "good" error is that the requested WAL segment has been purged by the leader.
		// The gRPC status code for this should be `NotFound`.
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			f.logger.Warn("Requested WAL segment not found on leader, forcing snapshot bootstrap.", "from_seq_num", startSeqNum, "error", err)
			// We can force a bootstrap by deleting our local data. This is a simple but effective strategy.
			// A more advanced strategy could be to just trigger the bootstrap logic without deleting.
			// For now, we'll just return an error that the main loop will catch, causing a full re-sync check.
			return fmt.Errorf("wal segment purged on leader, requires bootstrap: %w", err)
		}
		return fmt.Errorf("cannot start WAL streaming: %w", err)
	}

	reportTicker := time.NewTicker(200 * time.Millisecond) // Report progress every 200ms
	defer reportTicker.Stop()
	f.logger.Info("Successfully connected to WAL stream, processing entries...", "from_seq_num", startSeqNum)

	for {
		select {
		case entry, ok := <-entryChan:
			if !ok {
				if err := <-errChan; err != nil {
					return fmt.Errorf("WAL stream terminated with error: %w", err)
				} else if ctx.Err() != nil {
					// If the context was cancelled, the stream might close cleanly before the error is sent.
					return ctx.Err()
				}
				f.logger.Info("WAL stream finished cleanly.")
				return nil
			}
			f.logger.Debug("Received WAL entry from leader", "seq_num", entry.GetSequenceNumber())

			if err := f.applier.ApplyEntry(ctx, entry); err != nil {
				f.logger.Error("Failed to apply replicated entry", "seq_num", entry.GetSequenceNumber(), "error", err)
				return fmt.Errorf("CRITICAL: failed to apply replicated entry %d: %w", entry.GetSequenceNumber(), err)
			}

			f.mu.Lock()
			f.lastAppliedSeqNum = entry.GetSequenceNumber()
			f.mu.Unlock()

		case err := <-errChan:
			return fmt.Errorf("WAL stream terminated with error: %w", err)

		case <-reportTicker.C:
			f.mu.RLock()
			lastApplied := f.lastAppliedSeqNum
			f.mu.RUnlock()
			if lastApplied > 0 {
				f.logger.Debug("Reporting progress to leader", "last_applied_seq_num", lastApplied)
				// Use a short-lived context derived from the main loop's context for the RPC call.
				reportCtx, reportCancel := context.WithTimeout(ctx, 1*time.Second)
				// Send progress report to leader. We ignore errors here as it's a best-effort notification.
				// In a more robust system, we might handle repeated failures to report progress.
				f.client.grpcClient.ReportProgress(reportCtx, &apiv1.ReportProgressRequest{AppliedSequenceNumber: lastApplied})
				reportCancel()
			}

		case <-ctx.Done():
			f.logger.Info("Replication stream cancelled.")
			return ctx.Err()
		}
	}
}
