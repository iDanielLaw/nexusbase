package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/INLOpen/nexusbase/config"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/grpc"
)

// Manager handles the lifecycle of the replication gRPC server.
type Manager struct {
	grpcServer *grpc.Server
	config     config.ReplicationConfig
	logger     *slog.Logger
	listener   net.Listener
}

// NewManager creates a new Replication Manager.
func NewManager(cfg config.ReplicationConfig, replicationServer pb.ReplicationServiceServer, logger *slog.Logger) (*Manager, error) {
	// Create a new gRPC server instance.
	grpcServer := grpc.NewServer()

	// Register the replication service implementation with the gRPC server.
	pb.RegisterReplicationServiceServer(grpcServer, replicationServer)

	// Create a listener on the specified address.
	lis, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.ListenAddress, err)
	}

	logger.Info("Replication gRPC server listening", "address", lis.Addr().String())

	return &Manager{
		grpcServer: grpcServer,
		config:     cfg,
		logger:     logger.With("component", "replication_manager"),
		listener:   lis,
	}, nil
}

// Start begins serving gRPC requests. This is a blocking call.
// It should be run in a goroutine.
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("Replication manager started", "mode", m.config.Mode)

	// Start serving requests.
	if err := m.grpcServer.Serve(m.listener); err != nil {
		m.logger.Error("gRPC server failed", "error", err)
		return err
	}
	return nil
}

// Stop gracefully shuts down the gRPC server.
func (m *Manager) Stop() {
	m.logger.Info("Stopping replication manager...")
	m.grpcServer.GracefulStop()
	m.logger.Info("Replication manager stopped.")
}
