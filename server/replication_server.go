package server

import (
	"context"
	"log/slog"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/engine"
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
	// TODO: Implement actual WAL reading and streaming logic.
	return status.Errorf(codes.Unimplemented, "method StreamWAL not implemented")
}

// GetLatestState returns the latest sequence number from the leader.
// This is a placeholder implementation.
func (s *ReplicationGRPCServer) GetLatestState(ctx context.Context, req *emptypb.Empty) (*apiv1.LatestStateResponse, error) {
	s.logger.Info("Received GetLatestState request")
	// TODO: Implement logic to get the latest sequence number from the engine.
	return nil, status.Errorf(codes.Unimplemented, "method GetLatestState not implemented")
}

// GetSnapshot allows a follower to bootstrap by receiving a full snapshot.
// This is a placeholder implementation.
func (s *ReplicationGRPCServer) GetSnapshot(req *apiv1.SnapshotRequest, stream apiv1.ReplicationService_GetSnapshotServer) error {
	s.logger.Info("Received GetSnapshot request")
	// TODO: Implement snapshot creation and streaming logic.
	return status.Errorf(codes.Unimplemented, "method GetSnapshot not implemented")
}
