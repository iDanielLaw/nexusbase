package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// ReplicatedEngine นิยามอินเทอร์เฟซสำหรับ storage engine
// ที่สามารถนำการเปลี่ยนแปลงที่ถูก replicate มาใช้ได้
type ReplicatedEngine interface {
	// ApplyReplicatedEntry นำการเปลี่ยนแปลงจาก leader มาใช้
	// โดยต้องไม่เขียนลง WAL ของตัวเอง
	ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error
	// GetLatestAppliedSeqNum คืนค่า sequence number ล่าสุดที่ถูกนำไปใช้แล้ว
	GetLatestAppliedSeqNum() uint64
}

// WALApplier มีหน้าที่เชื่อมต่อไปยัง leader,
// สตรีม WAL entries, และนำมาใช้กับ engine ของตัวเอง
type WALApplier struct {
	leaderAddr string
	engine     ReplicatedEngine
	logger     *slog.Logger
	conn       *grpc.ClientConn
	client     pb.ReplicationServiceClient
	cancel     context.CancelFunc

	// dialOpts allows injecting gRPC dial options for testing.
	dialOpts []grpc.DialOption
	// retrySleep is the duration to wait before retrying a failed stream connection.
	// @TestOnly
	retrySleep time.Duration
}

// NewWALApplier สร้าง WAL applier ใหม่
func NewWALApplier(leaderAddr string, engine ReplicatedEngine, logger *slog.Logger) *WALApplier {
	return &WALApplier{
		leaderAddr: leaderAddr,
		engine:     engine,
		logger:     logger.With("component", "wal_applier", "leader", leaderAddr),
		retrySleep: 6 * time.Second, // Default retry sleep
	}
}

// Start เริ่มกระบวนการเชื่อมต่อและ replicate จาก leader
// เมธอดนี้จะทำงานใน loop และควรถูกเรียกใน goroutine
func (a *WALApplier) Start(ctx context.Context) {
	a.logger.Info("Starting WAL Applier")

	applierCtx, cancel := context.WithCancel(ctx)

	// Setup dial options. Production options are default.
	// Tests can inject their own options via the `dialOpts` field.
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

	// เชื่อมต่อไปยัง leader พร้อม retry logic
	conn, err := grpc.DialContext(applierCtx, a.leaderAddr, opts...)
	if err != nil {
		a.logger.Error("Failed to dial leader, WAL applier will not run", "error", err)
		cancel() // Avoid leaking the context
		return
	}

	// Only set the cancel function after a successful connection.
	a.cancel = cancel
	a.conn = conn
	a.client = pb.NewReplicationServiceClient(conn)

	a.logger.Info("Successfully connected to leader")

	go a.replicationLoop(applierCtx)
}

// replicationLoop คือ loop หลักที่ทำการสตรีมและนำ WAL entries มาใช้
func (a *WALApplier) replicationLoop(ctx context.Context) error { // Add error return
	for {
		select {
		case <-ctx.Done():
			a.logger.Info("Replication loop stopping due to context cancellation.")
			return ctx.Err() // Return context error if cancelled
		default:
			fromSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
			a.logger.Info("Starting WAL stream", "from_seq_num", fromSeqNum)

			req := &pb.StreamWALRequest{
				FromSequenceNumber: fromSeqNum,
			}

			// Add a small delay before attempting to establish a new stream
			// to allow the gRPC client to potentially recover from previous errors.
			if a.client != nil { // Only sleep if client is initialized
				time.Sleep(100 * time.Millisecond)
			}

			stream, err := a.client.StreamWAL(ctx, req)
			if err != nil {
				a.logger.Error("Failed to start WAL stream, will retry...", "error", err)
				time.Sleep(a.retrySleep) // Wait a bit before retrying
				continue
			}

			err = a.processStream(ctx, stream)
			if err != nil { // Check all errors from processStream
				if errors.Is(err, io.EOF) {
					a.logger.Info("WAL stream ended gracefully, will attempt to re-establish.")
					// The stream has ended. We'll loop around and create a new one.
					// Add a small delay to prevent busy-looping if the leader continuously has no new entries.
					time.Sleep(500 * time.Millisecond)
					continue
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					a.logger.Info("Replication loop stopping due to context cancellation or deadline.", "error", err)
					return err // Return context error to stop the loop
				}
				a.logger.Error("WAL stream broke with a critical error, stopping replication loop.", "error", err)
				a.Stop() // Self-terminate on critical error
				return err // Return critical error to stop the loop
			}
		}
	}
}

// processStream อ่านข้อมูลจาก gRPC stream และนำ entries มาใช้
func (a *WALApplier) processStream(ctx context.Context, stream pb.ReplicationService_StreamWALClient) error {
	const maxApplyRetries = 3 // Max retries for ApplyReplicatedEntry
	for {
		entry, err := stream.Recv()
		if err != nil {
			return err
		}

		expectedSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
		if entry.GetSequenceNumber() != expectedSeqNum {
			// This is a critical error. Out-of-order entries indicate a serious problem
			// with either the leader's stream or the follower's state.
			// We should not retry this, but rather stop and alert.
			return fmt.Errorf("received out-of-order WAL entry: expected %d, got %d", expectedSeqNum, entry.GetSequenceNumber())
		}

		// Retry applying the entry
		var applyErr error // Declare applyErr here to capture the last error
		for i := 0; i < maxApplyRetries; i++ {
			applyErr = a.engine.ApplyReplicatedEntry(ctx, entry) // Assign to applyErr
			if applyErr != nil {
				a.logger.Error("Failed to apply replicated entry, retrying...",
					"error", applyErr, // Use applyErr here
					"seq_num", entry.GetSequenceNumber(),
					"attempt", i+1,
					"max_attempts", maxApplyRetries,
				)
				time.Sleep(1 * time.Second) // Small delay before retrying
				continue                    // Try again
			}
			// Success, break retry loop
			break
		}

		// If we reached here and there was still an error after retries,
		// it means we couldn't apply this entry. This is a critical failure.
		if applyErr != nil { // Check applyErr from the loop
			a.logger.Error("Failed to apply replicated entry after multiple retries, stopping replication.",
				"error", applyErr,
				"seq_num", entry.GetSequenceNumber(),
			)
			return fmt.Errorf("failed to apply replicated entry %d after %d retries: %w",
				entry.GetSequenceNumber(), maxApplyRetries, applyErr)
		}
	}
}

// Stop ปิดการทำงานของ WAL applier อย่างนุ่มนวล
func (a *WALApplier) Stop() {
	a.logger.Info("Stopping WAL Applier")
	if a.cancel != nil {
		a.cancel()
	}
	if a.conn != nil {
		a.conn.Close()
	}
}
