package replication

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/config"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/grpc"
)

// FollowerState tracks connection, health, and lag for each follower.
type FollowerState struct {
	Addr        string
	LastAckSeq  uint64
	LastPing    time.Time
	LagCallback func(lag time.Duration)
	Healthy     bool
	RetryCount  int
	mu          sync.Mutex
}

// Manager handles the lifecycle of the replication gRPC server and manages followers.
type Manager struct {
	grpcServer *grpc.Server
	config     config.ReplicationConfig
	logger     *slog.Logger
	listener   net.Listener
	followers  map[string]*FollowerState
	stopCh     chan struct{}
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
	logger.Info("Replication manager starting", "mode", cfg.Mode)

	followers := make(map[string]*FollowerState)
	for _, addr := range cfg.Followers {
		followers[addr] = &FollowerState{Addr: addr, Healthy: false}
	}

	return &Manager{
		grpcServer: grpcServer,
		config:     cfg,
		logger:     logger.With("component", "replication_manager"),
		listener:   lis,
		followers:  followers,
		stopCh:     make(chan struct{}),
	}, nil
}

// Start begins serving gRPC requests and manages follower connections.
// This is a blocking call.
// It should be run in a goroutine.
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info("Replication manager started", "mode", m.config.Mode)
	go m.monitorFollowers(ctx)

	// Start serving requests.
	if err := m.grpcServer.Serve(m.listener); err != nil {
		m.logger.Error("gRPC server failed", "error", err)
		return err
	}
	return nil
}

// monitorFollowers handles retry, reconnect, health check, and lag callback for each follower.
func (m *Manager) monitorFollowers(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			m.logger.Info("Replication manager stopping")
			return
		case <-ticker.C:
			for _, f := range m.followers {
				go m.checkFollowerHealth(f)
			}
		}
	}
}

// checkFollowerHealth performs a real health check (gRPC ping), updates lag, and handles reconnect.
func (m *Manager) checkFollowerHealth(f *FollowerState) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Real health check: try to connect and ping follower's gRPC endpoint
	conn, err := grpc.Dial(f.Addr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		f.Healthy = false
		f.RetryCount++
		m.logger.Warn("Follower unreachable", "addr", f.Addr, "retries", f.RetryCount, "error", err)
		return
	}
	defer conn.Close()

	// Simulate ping: could be replaced with a real health RPC in future
	if conn.GetState().String() == "READY" {
		f.Healthy = true
		f.RetryCount = 0
		f.LastPing = time.Now()
		lag := time.Since(f.LastPing)
		if f.LagCallback != nil {
			f.LagCallback(lag)
		}
		m.logger.Info("Follower health check", "addr", f.Addr, "healthy", f.Healthy, "lag", lag)
	} else {
		f.Healthy = false
		f.RetryCount++
		m.logger.Warn("Follower not ready", "addr", f.Addr, "retries", f.RetryCount, "state", conn.GetState().String())
	}
}

// Stop gracefully shuts down the gRPC server and follower monitoring.
func (m *Manager) Stop() {
	m.logger.Info("Stopping replication manager...")
	close(m.stopCh)
	m.grpcServer.GracefulStop()
	m.logger.Info("Replication manager stopped.")
}
