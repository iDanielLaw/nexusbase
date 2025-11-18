package replication

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/config"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
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
	grpcServer          *grpc.Server
	config              config.ReplicationConfig
	logger              *slog.Logger
	listener            net.Listener
	followers           map[string]*FollowerState
	stopCh              chan struct{}
	gracefulStopTimeout time.Duration
	tlsEnabled          bool
	clientDialOptions   []grpc.DialOption
	stopOnce            sync.Once // Ensures Stop is only called once
}

// NewManager creates a new Replication Manager.
func NewManager(cfg config.ReplicationConfig, replicationServer pb.ReplicationServiceServer, logger *slog.Logger) (*Manager, error) {
	// Prepare server options
	var serverOpts []grpc.ServerOption
	tlsEnabled := false

	// Configure TLS if enabled
	if cfg.TLS.Enabled {
		tlsConfig, err := loadServerTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS config: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		serverOpts = append(serverOpts, grpc.Creds(creds))
		tlsEnabled = true
		logger.Info("TLS enabled for replication gRPC server")
	}

	// Create a new gRPC server instance.
	grpcServer := grpc.NewServer(serverOpts...)

	// Register the replication service implementation with the gRPC server.
	pb.RegisterReplicationServiceServer(grpcServer, replicationServer)

	// Create a listener on the specified address.
	lis, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", cfg.ListenAddress, err)
	}

	logger.Info("Replication gRPC server listening", "address", lis.Addr().String(), "tls_enabled", tlsEnabled)
	logger.Info("Replication manager starting", "mode", cfg.Mode)

	followers := make(map[string]*FollowerState)
	for _, addr := range cfg.Followers {
		followers[addr] = &FollowerState{Addr: addr, Healthy: false}
	}

	// Prepare client dial options for health checks
	var clientDialOpts []grpc.DialOption
	if tlsEnabled {
		// For client connections to followers, use TLS
		tlsConfig, err := loadClientTLSConfig(cfg.TLS)
		if err != nil {
			return nil, fmt.Errorf("failed to load client TLS config: %w", err)
		}
		creds := credentials.NewTLS(tlsConfig)
		clientDialOpts = append(clientDialOpts, grpc.WithTransportCredentials(creds))
	} else {
		// Use insecure credentials only if TLS is not enabled
		clientDialOpts = append(clientDialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Set graceful stop timeout
	gracefulStopTimeout := time.Duration(cfg.GracefulStopTimeoutSeconds) * time.Second
	if gracefulStopTimeout == 0 {
		gracefulStopTimeout = 30 * time.Second // Default to 30 seconds
	}

	return &Manager{
		grpcServer:          grpcServer,
		config:              cfg,
		logger:              logger.With("component", "replication_manager"),
		listener:            lis,
		followers:           followers,
		stopCh:              make(chan struct{}),
		gracefulStopTimeout: gracefulStopTimeout,
		tlsEnabled:          tlsEnabled,
		clientDialOptions:   clientDialOpts,
	}, nil
}

// loadServerTLSConfig loads TLS configuration for the gRPC server
func loadServerTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert, // Can be changed to tls.RequireAndVerifyClientCert for mTLS
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// loadClientTLSConfig loads TLS configuration for gRPC clients
func loadClientTLSConfig(tlsCfg config.TLSConfig) (*tls.Config, error) {
	// For client, we need to verify the server's certificate
	// If you have a CA cert, load it here
	certPool := x509.NewCertPool()

	// For now, we'll use system cert pool
	// In production, you should load a specific CA cert
	systemCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to load system cert pool: %w", err)
	}
	certPool = systemCertPool

	return &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
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
	// Start one worker goroutine per follower to avoid spawning multiple
	// overlapping health-check goroutines for the same follower.
	for _, f := range m.followers {
		go func(f *FollowerState) {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-m.stopCh:
					return
				case <-ticker.C:
					m.checkFollowerHealth(f)
				}
			}
		}(f)
	}

	// Block until stop signal so this method remains a blocking call for tests.
	<-m.stopCh
	m.logger.Info("Replication manager stopping")
}

// checkFollowerHealth performs a real health check (gRPC HealthCheck RPC), updates lag, and handles reconnect.
func (m *Manager) checkFollowerHealth(f *FollowerState) {
	// Do not hold the follower mutex while performing network I/O. Only lock
	// to update the follower state to avoid blocking other callers.

	// Create a context with timeout for the health check
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Establish connection to follower using the prepared dial options
	conn, err := grpc.DialContext(ctx, f.Addr, append(m.clientDialOptions, grpc.WithBlock())...)
	if err != nil {
		f.mu.Lock()
		f.Healthy = false
		f.RetryCount++
		f.mu.Unlock()
		m.logger.Warn("Follower unreachable", "addr", f.Addr, "retries", f.RetryCount, "error", err)
		return
	}
	defer conn.Close()

	// Create a replication service client
	client := pb.NewReplicationServiceClient(conn)

	// Call the HealthCheck RPC
	resp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		f.mu.Lock()
		f.Healthy = false
		f.RetryCount++
		f.mu.Unlock()
		m.logger.Warn("Health check RPC failed", "addr", f.Addr, "retries", f.RetryCount, "error", err)
		return
	}

	// Update follower state under lock and compute lag based on previous timestamp.
	now := time.Now()
	var lag time.Duration
	f.mu.Lock()
	prevLastPing := f.LastPing
	f.LastPing = now
	f.LastAckSeq = resp.LastAppliedSequence
	if resp.Status == pb.HealthCheckResponse_HEALTHY {
		f.Healthy = true
		f.RetryCount = 0
	} else {
		f.Healthy = false
		f.RetryCount++
	}
	// compute lag as interval since previous successful ping if available
	if !prevLastPing.IsZero() {
		lag = now.Sub(prevLastPing)
	}
	f.mu.Unlock()

	if f.LagCallback != nil && lag > 0 {
		f.LagCallback(lag)
	}

	m.logger.Info("Follower health check",
		"addr", f.Addr,
		"healthy", f.Healthy,
		"last_seq", f.LastAckSeq,
		"lag", lag,
		"message", resp.Message)
}

// Stop gracefully shuts down the gRPC server and follower monitoring with timeout.
func (m *Manager) Stop() {
	m.stopOnce.Do(func() {
		m.logger.Info("Stopping replication manager...")

		// Signal follower monitoring to stop
		close(m.stopCh)

		// Create a channel to signal when graceful stop completes
		stopped := make(chan struct{})

		// Start graceful stop in a goroutine
		go func() {
			m.grpcServer.GracefulStop()
			close(stopped)
		}()

		// Wait for graceful stop to complete or timeout
		select {
		case <-stopped:
			m.logger.Info("Replication manager stopped gracefully.")
		case <-time.After(m.gracefulStopTimeout):
			m.logger.Warn("Graceful stop timeout exceeded, forcing shutdown", "timeout", m.gracefulStopTimeout)
			m.grpcServer.Stop() // Force stop
			m.logger.Info("Replication manager stopped forcefully.")
		}
	})
}
