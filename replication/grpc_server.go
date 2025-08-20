package replication

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/INLOpen/nexusbase/core"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Server คือการ implement gRPC service สำหรับ Replication ฝั่ง Leader
type Server struct {
	// ต้องฝัง UnimplementedReplicationServiceServer เพื่อให้เข้ากันได้กับ gRPC เวอร์ชันใหม่ๆ
	pb.UnimplementedReplicationServiceServer

	wal    wal.WALInterface // ใช้อินเทอร์เฟซเพื่อความยืดหยุ่นในการทดสอบ
	logger *slog.Logger
}

// NewServer สร้าง instance ใหม่ของ gRPC Replication Server
func NewServer(w wal.WALInterface, logger *slog.Logger) *Server {
	return &Server{
		wal:    w,
		logger: logger.With("component", "replication_grpc_server"),
	}
}

// StreamWAL คือเมธอดหลักที่ Follower เรียกใช้เพื่อรับข้อมูล WAL อย่างต่อเนื่อง
// ในขั้นตอนนี้ เราจะสร้างโครงไว้ก่อน
func (s *Server) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	ctx := stream.Context()
	followerAddr := "unknown"
	if p, ok := peer.FromContext(ctx); ok {
		followerAddr = p.Addr.String()
	}

	s.logger.Info("Follower connected for WAL streaming",
		"follower", followerAddr,
		"from_seq_num", req.GetFromSequenceNumber(),
	)

	// 1. สร้าง Reader สำหรับอ่าน WAL แบบ streaming
	walReader, err := s.wal.NewStreamReader(req.GetFromSequenceNumber())
	if err != nil {
		s.logger.Error("Failed to create WAL stream reader", "error", err, "follower", followerAddr)
		return status.Errorf(codes.Internal, "could not create WAL reader: %v", err)
	}
	defer walReader.Close()

	for {
		select {
		case <-ctx.Done():
			// Client ยกเลิกการเชื่อมต่อ
			s.logger.Info("Follower disconnected", "follower", followerAddr, "reason", ctx.Err())
			return ctx.Err()
		default:
			// 2. อ่าน entry ถัดไปจาก WAL
			// การเรียก Next() จะ block รอจนกว่าจะมีข้อมูลใหม่เข้ามา หรือคืน ErrNoNewEntries
			entry, err := walReader.Next()
			if err != nil {
				// หากไม่มี entry ใหม่ (แต่ยังไม่จบ stream) เราจะรอสักครู่แล้วลองใหม่
				// **ข้อควรปรับปรุงในอนาคต:** ควรเปลี่ยนจากการ polling แบบนี้ไปใช้
				// channel หรือ condition variable เพื่อให้ WAL แจ้งเตือนเมื่อมีข้อมูลใหม่
				// ซึ่งจะลด latency และการใช้ CPU ลง
				if errors.Is(err, wal.ErrNoNewEntries) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				// หาก WAL ถูกปิดหรือเสียหายอย่างถาวร
				if errors.Is(err, io.EOF) {
					s.logger.Info("Reached end of WAL stream permanently.", "follower", followerAddr)
					return nil // ปิด stream อย่างปกติ
				}

				s.logger.Error("Error reading from WAL for follower", "follower", followerAddr, "error", err)
				return status.Errorf(codes.Internal, "error reading WAL: %v", err)
			}

			// 3. แปลง WAL entry ให้อยู่ในรูปแบบ Protobuf
			// TODO: Implement a proper conversion from core.WALEntry to proto.WALEntry
			// This requires decoding the entry.Value based on entry.EntryType.
			// For now, we'll just pass the sequence number as a placeholder.
			protoEntry, err := s.convertWALEntryToProto(entry)
			if err != nil {
				s.logger.Error("Failed to convert WAL entry to protobuf message", "seq_num", entry.SeqNum, "error", err)
				// Decide whether to skip this entry or terminate the stream.
				// For now, we skip it.
				continue
			}

			// 4. ส่ง entry ผ่าน gRPC stream
			if err := stream.Send(protoEntry); err != nil {
				// หากส่งไม่สำเร็จ (เช่น follower ปิดการเชื่อมต่อ)
				s.logger.Error("Failed to send WAL entry to follower", "follower", followerAddr, "error", err)
				return err
			}
		}
	}
}

// convertWALEntryToProto is a placeholder for converting the internal WAL entry to the protobuf format.
func (s *Server) convertWALEntryToProto(entry *core.WALEntry) (*pb.WALEntry, error) {
	// This is a simplified conversion. A real implementation needs to decode
	// the entry.Key and entry.Value to populate the protobuf message correctly.
	return &pb.WALEntry{
		SequenceNumber: entry.SeqNum,
		// EntryType:      ...
		// Metric:         ...
		// Tags:           ...
		// Timestamp:      ...
		// Fields:         ...
	}, nil
}
