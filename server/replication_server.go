package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/replication"
	"github.com/INLOpen/nexusbase/wal"
)

// ReplicationGRPCServer wraps the grpc.Server and implements the ReplicationService.
type ReplicationGRPCServer struct {
	apiv1.UnimplementedReplicationServiceServer
	engine    engine.StorageEngineInterface
	server    *grpc.Server
	healthSrv *health.Server
	logger    *slog.Logger
}

// NewReplicationGRPCServer creates and configures a new gRPC server for replication.
func NewReplicationGRPCServer(eng engine.StorageEngineInterface, cfg *config.ServerConfig, logger *slog.Logger) (*ReplicationGRPCServer, error) {
	s := &ReplicationGRPCServer{
		engine:    eng,
		logger:    logger.With("component", "ReplicationGRPCServer"),
		healthSrv: health.NewServer(),
	}

	var opts []grpc.ServerOption
	// NOTE: TLS configuration can be added here, similar to the main gRPC server.
	// For simplicity, it is omitted in this initial plan.

	// TODO: Add authentication/authorization interceptors specific to replication.
	// For example, only specific nodes (followers) should be allowed to connect.

	s.server = grpc.NewServer(opts...)
	apiv1.RegisterReplicationServiceServer(s.server, s)
	grpc_health_v1.RegisterHealthServer(s.server, s.healthSrv)
	reflection.Register(s.server)

	return s, nil
}

// Start begins listening for gRPC requests.
func (s *ReplicationGRPCServer) Start(lis net.Listener) error {
	s.logger.Info("Replication gRPC server listening", "address", lis.Addr().String())
	s.healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	return s.server.Serve(lis)
}

// Stop gracefully stops the gRPC server.
func (s *ReplicationGRPCServer) Stop() {
	s.logger.Info("Stopping replication gRPC server...")
	if s.healthSrv != nil {
		s.healthSrv.Shutdown()
	}
	if s.server != nil {
		s.server.GracefulStop()
	}
	s.logger.Info("Replication gRPC server stopped.")
}

// StreamWAL is the implementation for the WAL streaming RPC.
// This is a placeholder implementation based on the plan in `docs/TODO-data-replication.md`.
func (s *ReplicationGRPCServer) StreamWAL(req *apiv1.StreamWALRequest, stream apiv1.ReplicationService_StreamWALServer) error {
	s.logger.Info("Received StreamWAL request", "from_sequence_number", req.GetFromSequenceNumber())
	ctx := stream.Context()

	// The engine interface needs to expose the WAL. This is a design decision.
	// We define a local interface to check if the engine supports this.
	type walProvider interface {
		GetWAL() wal.WALInterface
	}

	engineWithWAL, ok := s.engine.(walProvider)
	if !ok {
		s.logger.Error("Storage engine does not support providing a WAL for replication")
		return status.Error(codes.Unimplemented, "replication is not supported by the current storage engine")
	}

	reader, err := engineWithWAL.GetWAL().OpenReader(req.GetFromSequenceNumber())
	if err != nil {
		s.logger.Error("Failed to open WAL reader", "error", err)
		return status.Errorf(codes.Internal, "could not start WAL stream: %v", err)
	}
	defer reader.Close()

	for {
		coreEntry, err := reader.Next(ctx)
		if err != nil {
			// NEW: Check if the error is because the requested segment was not found (purged).
			if os.IsNotExist(errors.Unwrap(err)) {
				s.logger.Warn("Follower requested a purged WAL segment", "from_seq_num", req.GetFromSequenceNumber(), "error", err)
				return status.Errorf(codes.NotFound, "requested WAL segment has been purged: %v", err)
			}

			if err == io.EOF || status.Code(err) == codes.Canceled || err == context.Canceled || err == context.DeadlineExceeded {
				s.logger.Info("Stopping WAL stream", "reason", err)
				return nil // Cleanly exit the stream
			}
			s.logger.Error("Error reading next WAL entry", "error", err)
			return status.Errorf(codes.Internal, "error during WAL stream: %v", err)
		}

		// Convert core.WALEntry to apiv1.WALEntry
		apiEntry, err := s.convertCoreWALEntryToAPI(coreEntry)
		if err != nil {
			s.logger.Error("Failed to convert WAL entry for replication", "seq_num", coreEntry.SeqNum, "error", err)
			// Skip corrupted/unconvertible entries? For now, we fail the stream.
			return status.Errorf(codes.Internal, "failed to process WAL entry %d: %v", coreEntry.SeqNum, err)
		}

		if err := stream.Send(apiEntry); err != nil {
			s.logger.Warn("Failed to send WAL entry to follower", "error", err)
			return err
		}
	}
}

// GetLatestState returns the latest sequence number from the leader.
// This is a placeholder implementation.
func (s *ReplicationGRPCServer) GetLatestState(ctx context.Context, req *emptypb.Empty) (*apiv1.LatestStateResponse, error) {
	s.logger.Info("Received GetLatestState request")

	// Define local interfaces to check if the engine supports the required methods.
	// This avoids modifying the public StorageEngineInterface for internal replication needs.
	type sequenceProvider interface {
		GetSequenceNumber() uint64
	}
	type walProvider interface {
		GetWAL() wal.WALInterface
	}

	var seqNum uint64
	if sp, ok := s.engine.(sequenceProvider); ok {
		seqNum = sp.GetSequenceNumber()
	} else {
		return nil, status.Error(codes.Unimplemented, "replication is not supported: engine does not provide sequence number")
	}

	var walIndex uint64
	if wp, ok := s.engine.(walProvider); ok {
		if wal := wp.GetWAL(); wal != nil {
			walIndex = wal.ActiveSegmentIndex()
		}
	} else {
		return nil, status.Error(codes.Unimplemented, "replication is not supported: engine does not provide WAL")
	}

	return &apiv1.LatestStateResponse{
		LatestSequenceNumber:  seqNum,
		LatestWalSegmentIndex: walIndex,
	}, nil
}

// ReportProgress is called by a follower to report its replication progress.
func (s *ReplicationGRPCServer) ReportProgress(ctx context.Context, req *apiv1.ReportProgressRequest) (*emptypb.Empty, error) {
	// Define a local interface to get the tracker from the engine.
	type trackerProvider interface {
		GetReplicationTracker() *replication.ReplicationTracker
	}

	engineWithTracker, ok := s.engine.(trackerProvider)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "synchronous replication is not supported by the current storage engine")
	}

	tracker := engineWithTracker.GetReplicationTracker()
	tracker.ReportAppliedSequence(req.GetAppliedSequenceNumber())
	return &emptypb.Empty{}, nil
}

// GetSnapshot allows a follower to bootstrap by receiving a full snapshot.
func (s *ReplicationGRPCServer) GetSnapshot(req *apiv1.SnapshotRequest, stream apiv1.ReplicationService_GetSnapshotServer) error {
	s.logger.Info("Received GetSnapshot request")
	ctx := stream.Context()

	snapshotPath, err := s.engine.CreateSnapshot(ctx)
	if err != nil {
		s.logger.Error("Failed to create snapshot for replication", "error", err)
		return status.Errorf(codes.Internal, "failed to create database snapshot: %v", err)
	}
	defer func() {
		s.logger.Info("Cleaning up temporary snapshot directory", "path", snapshotPath)
		if err := os.RemoveAll(snapshotPath); err != nil {
			s.logger.Error("Failed to clean up snapshot directory", "path", snapshotPath, "error", err)
		}
	}()

	s.logger.Info("Snapshot created, beginning stream.", "path", snapshotPath)

	err = filepath.WalkDir(snapshotPath, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if err := s.streamFile(stream, snapshotPath, path); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		s.logger.Error("Error during snapshot streaming", "error", err)
		if status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
			s.logger.Info("Snapshot stream cancelled by client.")
			return nil
		}
		return status.Errorf(codes.Internal, "error streaming snapshot: %v", err)
	}

	s.logger.Info("Successfully streamed all snapshot files.")
	return nil
}

func (s *ReplicationGRPCServer) convertCoreWALEntryToAPI(coreEntry *core.WALEntry) (*apiv1.WALEntry, error) {
	apiEntry := &apiv1.WALEntry{
		SequenceNumber:  coreEntry.SeqNum,
		WalSegmentIndex: coreEntry.SegmentIndex,
	}

	switch coreEntry.EntryType {
	case core.EntryTypePutEvent:
		apiEntry.Payload = &apiv1.WALEntry_PutEvent{
			PutEvent: &apiv1.PutEvent{
				Key:   coreEntry.Key,
				Value: coreEntry.Value,
			},
		}
	case core.EntryTypeDelete:
		apiEntry.Payload = &apiv1.WALEntry_DeleteEvent{
			DeleteEvent: &apiv1.DeleteEvent{
				Key: coreEntry.Key,
			},
		}
	case core.EntryTypeDeleteSeries:
		apiEntry.Payload = &apiv1.WALEntry_DeleteSeriesEvent{
			DeleteSeriesEvent: &apiv1.DeleteSeriesEvent{
				KeyPrefix: coreEntry.Key,
			},
		}
	case core.EntryTypeDeleteRange:
		// TODO: The core.WALEntry for DeleteRange needs to be properly defined.
		// Assuming the value contains the start and end timestamps.
		return nil, status.Errorf(codes.Unimplemented, "delete range replication not yet supported")
	default:
		return nil, status.Errorf(codes.Internal, "unknown WAL entry type: %v", coreEntry.EntryType)
	}
	return apiEntry, nil
}

// streamFile reads a file from the snapshot and streams it in chunks.
func (s *ReplicationGRPCServer) streamFile(stream apiv1.ReplicationService_GetSnapshotServer, snapshotRoot, filePath string) error {
	relPath, err := filepath.Rel(snapshotRoot, filePath)
	if err != nil {
		return fmt.Errorf("failed to calculate relative path for %s: %w", filePath, err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot file %s: %w", filePath, err)
	}
	defer file.Close()

	buffer := make([]byte, 64*1024) // 64KB chunks
	for {
		// Check for context cancellation before each read.
		if err := stream.Context().Err(); err != nil {
			return err
		}

		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to read snapshot file chunk %s: %w", filePath, err)
		}
		if n == 0 {
			break // End of file.
		}

		chunk := &apiv1.SnapshotChunk{
			FilePathRelative: relPath,
			Content:          buffer[:n],
		}

		if sendErr := stream.Send(chunk); sendErr != nil {
			return sendErr // Client likely disconnected.
		}
	}
	return nil
}
