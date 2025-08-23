package replication

import (
	"errors"
	"io"
	"log/slog"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// Server คือการ implement gRPC service สำหรับ Replication ฝั่ง Leader
type Server struct {
	// ต้องฝัง UnimplementedReplicationServiceServer เพื่อให้เข้ากันได้กับ gRPC เวอร์ชันใหม่ๆ
	pb.UnimplementedReplicationServiceServer

	wal     wal.WALInterface
	indexer *indexer.StringStore // เพิ่ม dependency ไปยัง StringStore
	logger  *slog.Logger
}

// NewServer สร้าง instance ใหม่ของ gRPC Replication Server
func NewServer(w wal.WALInterface, indexer *indexer.StringStore, logger *slog.Logger) *Server {
	return &Server{
		wal:     w,
		indexer: indexer,
		logger:  logger.With("component", "replication_grpc_server"),
	}
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
