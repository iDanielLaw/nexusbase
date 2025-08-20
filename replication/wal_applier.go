package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	pb "github.com/iDanielLaw/nexusbase/replication/proto"
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
}

// NewWALApplier สร้าง WAL applier ใหม่
func NewWALApplier(leaderAddr string, engine ReplicatedEngine, logger *slog.Logger) *WALApplier {
	return &WALApplier{
		leaderAddr: leaderAddr,
		engine:     engine,
		logger:     logger.With("component", "wal_applier", "leader", leaderAddr),
	}
}

// Start เริ่มกระบวนการเชื่อมต่อและ replicate จาก leader
// เมธอดนี้จะทำงานใน loop และควรถูกเรียกใน goroutine
func (a *WALApplier) Start(ctx context.Context) {
	a.logger.Info("Starting WAL Applier")

	applierCtx, cancel := context.WithCancel(ctx)
	a.cancel = cancel

	// เชื่อมต่อไปยัง leader พร้อม retry logic
	conn, err := grpc.DialContext(applierCtx, a.leaderAddr,
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
	)
	if err != nil {
		a.logger.Error("Failed to dial leader, WAL applier will not run", "error", err)
		return
	}
	a.conn = conn
	a.client = pb.NewReplicationServiceClient(conn)

	a.logger.Info("Successfully connected to leader")

	go a.replicationLoop(applierCtx)
}

// replicationLoop คือ loop หลักที่ทำการสตรีมและนำ WAL entries มาใช้
func (a *WALApplier) replicationLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			a.logger.Info("Replication loop stopping due to context cancellation.")
			return
		default:
			fromSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
			a.logger.Info("Starting WAL stream", "from_seq_num", fromSeqNum)

			req := &pb.StreamWALRequest{
				FromSequenceNumber: fromSeqNum,
			}

			stream, err := a.client.StreamWAL(ctx, req)
			if err != nil {
				a.logger.Error("Failed to start WAL stream, will retry...", "error", err)
				time.Sleep(5 * time.Second) // รอสักครู่ก่อนลองใหม่
				continue
			}

			err = a.processStream(ctx, stream)
			if err != nil && err != io.EOF && err != context.Canceled {
				a.logger.Error("WAL stream broke with an error", "error", err)
			}
		}
	}
}

// processStream อ่านข้อมูลจาก gRPC stream และนำ entries มาใช้
func (a *WALApplier) processStream(ctx context.Context, stream pb.ReplicationService_StreamWALClient) error {
	for {
		entry, err := stream.Recv()
		if err != nil {
			return err
		}

		expectedSeqNum := a.engine.GetLatestAppliedSeqNum() + 1
		if entry.GetSequenceNumber() != expectedSeqNum {
			return errors.New("received out-of-order WAL entry")
		}

		if err := a.engine.ApplyReplicatedEntry(ctx, entry); err != nil {
			a.logger.Error("Failed to apply replicated entry, stopping replication", "error", err, "seq_num", entry.GetSequenceNumber())
			return err
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
