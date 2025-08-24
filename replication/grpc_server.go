package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server คือการ implement gRPC service สำหรับ Replication ฝั่ง Leader
type Server struct {
	// ต้องฝัง UnimplementedReplicationServiceServer เพื่อให้เข้ากันได้กับ gRPC เวอร์ชันใหม่ๆ
	pb.UnimplementedReplicationServiceServer

	wal         wal.WALInterface
	indexer     *indexer.StringStore
	snapshotMgr snapshot.ManagerInterface
	snapshotDir string // Base directory where snapshots are stored
	logger      *slog.Logger
}

// NewServer สร้าง instance ใหม่ของ gRPC Replication Server
func NewServer(w wal.WALInterface, indexer *indexer.StringStore, snapshotMgr snapshot.ManagerInterface, snapshotDir string, logger *slog.Logger) *Server {
	return &Server{
		wal:         w,
		indexer:     indexer,
		snapshotMgr: snapshotMgr,
		snapshotDir: snapshotDir,
		logger:      logger.With("component", "replication_grpc_server"),
	}
}

// GetLatestSnapshotInfo คืนค่า metadata ของ snapshot ล่าสุดที่มีอยู่
func (s *Server) GetLatestSnapshotInfo(ctx context.Context, req *pb.GetLatestSnapshotInfoRequest) (*pb.SnapshotInfo, error) {
	infos, err := s.snapshotMgr.ListSnapshots(s.snapshotDir)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list snapshots: %v", err)
	}

	if len(infos) == 0 {
		return nil, status.Errorf(codes.NotFound, "no snapshots found")
	}

	// ค้นหา snapshot ล่าสุด
	var latest snapshot.Info
	found := false
	for _, info := range infos {
		if !found || info.CreatedAt.After(latest.CreatedAt) {
			latest = info
			found = true
		}
	}

	if !found {
		return nil, status.Errorf(codes.NotFound, "no suitable snapshots found")
	}

	// อ่าน manifest เพื่อเอา sequence number
	manifestPath := filepath.Join(s.snapshotDir, latest.ID, "MANIFEST")
	f, err := os.Open(manifestPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to open snapshot manifest %s: %v", manifestPath, err)
	}
	defer f.Close()

	manifest, err := snapshot.ReadManifestBinary(f)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read snapshot manifest %s: %v", manifestPath, err)
	}

	return &pb.SnapshotInfo{
		Id:                    latest.ID,
		LastWalSequenceNumber: manifest.SequenceNumber,
		CreatedAt:             timestamppb.New(latest.CreatedAt),
		SizeBytes:             latest.TotalChainSize, // ใช้ขนาดของ chain ทั้งหมดเพื่อให้เห็นภาพรวม
	}, nil
}

// StreamSnapshot สตรีมเนื้อหาของ snapshot ไปยัง follower
func (s *Server) StreamSnapshot(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
	s.logger.Info("Follower requested snapshot stream", "snapshot_id", req.Id)

	snapshotPath := filepath.Join(s.snapshotDir, req.Id)

	// ทำการ Walk ใน snapshot directory เพื่อสตรีมไฟล์แต่ละไฟล์
	return filepath.Walk(snapshotPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// เราจะสตรีมเฉพาะ regular files เท่านั้น
		if !info.Mode().IsRegular() {
			return nil
		}

		// หา relative path เพื่อส่งไปให้ follower
		relPath, err := filepath.Rel(snapshotPath, path)
		if err != nil {
			return err
		}

		// 1. ส่ง file metadata ไปก่อน
		fileInfo := &pb.FileInfo{Path: relPath}
		if err := stream.Send(&pb.SnapshotChunk{Content: &pb.SnapshotChunk_FileInfo{FileInfo: fileInfo}}); err != nil {
			return err
		}

		// 2. สตรีมเนื้อหาไฟล์เป็นส่วนๆ (chunks)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		buf := make([]byte, 1024*64) // 64KB chunks
		for {
			n, err := f.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			chunk := &pb.SnapshotChunk{Content: &pb.SnapshotChunk_ChunkData{ChunkData: buf[:n]}}
			if err := stream.Send(chunk); err != nil {
				return err
			}
		}

		return nil
	})
}

// StreamWAL คือเมธอดหลักที่ Follower เรียกใช้เพื่อรับข้อมูล WAL อย่างต่อเนื่อง
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
			entry, err := walReader.Next()
			if err != nil {
				if errors.Is(err, wal.ErrNoNewEntries) {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				if errors.Is(err, io.EOF) {
					s.logger.Info("Reached end of WAL stream permanently.", "follower", followerAddr)
					return nil // ปิด stream อย่างปกติ
				}

				s.logger.Error("Error reading from WAL for follower", "follower", followerAddr, "error", err)
				return status.Errorf(codes.Internal, "error reading WAL: %v", err)
			}

			// 3. แปลง WAL entry ให้อยู่ในรูปแบบ Protobuf
			protoEntry, err := s.convertWALEntryToProto(entry)
			if err != nil {
				s.logger.Error("Failed to convert WAL entry to protobuf message", "seq_num", entry.SeqNum, "error", err)
				continue
			}

			// 4. ส่ง entry ผ่าน gRPC stream
			if err := stream.Send(protoEntry); err != nil {
				s.logger.Error("Failed to send WAL entry to follower", "follower", followerAddr, "error", err)
				return err
			}
		}
	}
}

// convertWALEntryToProto แปลง internal WAL entry ไปเป็น protobuf format
func (s *Server) convertWALEntryToProto(entry *core.WALEntry) (*pb.WALEntry, error) {
	protoEntry := &pb.WALEntry{
		SequenceNumber: entry.SeqNum,
	}

	switch entry.EntryType {
	case core.EntryTypePutEvent:
		protoEntry.EntryType = pb.WALEntry_PUT_EVENT

		// Decode series key and timestamp from the entry Key
		seriesKeyBytes, err := core.ExtractSeriesKeyFromInternalKeyWithErr(entry.Key)
		if err != nil {
			return nil, err
		}
		timestamp, err := core.DecodeTimestamp(entry.Key[len(entry.Key)-8:])
		if err != nil {
			return nil, err
		}
		protoEntry.Timestamp = timestamp

		// Decode metric and tags from the series key
		metricID, tagPairs, err := core.DecodeSeriesKey(seriesKeyBytes)
		if err != nil {
			return nil, err
		}

		metric, ok := s.indexer.GetString(metricID)
		if !ok {
			return nil, errors.New("metric ID not found in string store")
		}
		protoEntry.Metric = metric

		tags := make(map[string]string, len(tagPairs))
		for _, pair := range tagPairs {
			k, okK := s.indexer.GetString(pair.KeyID)
			v, okV := s.indexer.GetString(pair.ValueID)
			if !okK || !okV {
				return nil, errors.New("tag ID not found in string store")
			}
			tags[k] = v
		}
		protoEntry.Tags = tags

		// Decode fields from the entry Value
		fields, err := core.DecodeFieldsFromBytes(entry.Value)
		if err != nil {
			return nil, err
		}
		protoFields, err := structpb.NewStruct(fields.ToMap())
		if err != nil {
			return nil, err
		}
		protoEntry.Fields = protoFields

	case core.EntryTypeDeleteSeries:
		protoEntry.EntryType = pb.WALEntry_DELETE_SERIES

		// For DeleteSeries, the Key is the series key
		metricID, tagPairs, err := core.DecodeSeriesKey(entry.Key)
		if err != nil {
			return nil, err
		}

		metric, ok := s.indexer.GetString(metricID)
		if !ok {
			return nil, errors.New("metric ID not found in string store")
		}
		protoEntry.Metric = metric

		tags := make(map[string]string, len(tagPairs))
		for _, pair := range tagPairs {
			k, okK := s.indexer.GetString(pair.KeyID)
			v, okV := s.indexer.GetString(pair.ValueID)
			if !okK || !okV {
				return nil, errors.New("tag ID not found in string store")
			}
			tags[k] = v
		}
		protoEntry.Tags = tags

	case core.EntryTypeDeleteRange:
		protoEntry.EntryType = pb.WALEntry_DELETE_RANGE

		// For DeleteRange, the Key is the series key
		metricID, tagPairs, err := core.DecodeSeriesKey(entry.Key)
		if err != nil {
			return nil, err
		}

		metric, ok := s.indexer.GetString(metricID)
		if !ok {
			return nil, errors.New("metric ID not found in string store")
		}
		protoEntry.Metric = metric

		tags := make(map[string]string, len(tagPairs))
		for _, pair := range tagPairs {
			k, okK := s.indexer.GetString(pair.KeyID)
			v, okV := s.indexer.GetString(pair.ValueID)
			if !okK || !okV {
				return nil, errors.New("tag ID not found in string store")
			}
			tags[k] = v
		}
		protoEntry.Tags = tags

		// For DeleteRange, the Value is the start and end time
		startTime, endTime, err := core.DecodeRangeTombstoneValue(entry.Value)
		if err != nil {
			return nil, err
		}
		protoEntry.StartTime = startTime
		protoEntry.EndTime = endTime

	default:
		// We don't replicate other entry types like point deletes ('D') or batches ('B')
		// because they are handled at a higher level or are not needed for replication.
		// A batch in the leader's WAL is replicated as individual PUT/DELETE events.
		return nil, errors.New("unsupported WAL entry type for replication")
	}

	return protoEntry, nil
}
