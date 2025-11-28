package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"

	"github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/auth"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine2"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/replication"
	"github.com/INLOpen/nexuscore/utils/clock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// AppServer manages all network-facing servers (gRPC, TCP, etc.).
type AppServer struct {
	grpcLis      net.Listener
	tcpLis       net.Listener
	grpcLisOwned bool
	tcpLisOwned  bool
	queryServer  *HTTPServer
	grpcServer   *GRPCServer
	tcpServer    *TCP2Server
	putWorker    *WorkerPool
	batchWorker  *WorkerPool
	cfg          *config.Config
	logger       *slog.Logger
	engine       engine2.StorageEngineExternal
	cancel       context.CancelFunc

	// Replication components
	replicationManager *replication.Manager
	walApplier         *replication.WALApplier
}

// NewAppServer creates and initializes a new application server.
// NewAppServer preserves the original API and delegates to NewAppServerWithListeners
// with nil listeners (meaning it will create real TCP listeners based on config).
func NewAppServer(eng engine2.StorageEngineInterface, cfg *config.Config, logger *slog.Logger) (*AppServer, error) {
	return NewAppServerWithListeners(eng, cfg, logger, nil, nil)
}

// NewAppServerWithListeners creates and initializes a new application server.
// If non-nil listeners are provided they will be used instead of calling
// net.Listen. This makes it easy for tests to inject in-memory listeners.
func NewAppServerWithListeners(eng engine2.StorageEngineInterface, cfg *config.Config, logger *slog.Logger, grpcLis net.Listener, tcpLis net.Listener) (*AppServer, error) {
	var authenticator core.IAuthenticator
	if cfg.Security.Enabled {
		var err error
		authenticator, err = auth.NewAuthenticator(cfg.Security.UserFilePath, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize authenticator: %w", err)
		}
	} else {
		authenticator = auth.NewNonAuthenticator()
	}

	// Create dedicated worker pools
	putWorkerPool := NewWorkerPool(runtime.NumCPU(), 1024, eng, logger.With("pool", "put"))
	batchWorkerPool := NewWorkerPool(runtime.NumCPU(), 512, eng, logger.With("pool", "batch"))

	appSrv := &AppServer{
		cfg:         cfg,
		logger:      logger.With("component", "AppServer"),
		engine:      eng,
		putWorker:   putWorkerPool,
		batchWorker: batchWorkerPool,
	}

	// gRPC: prefer provided listener, otherwise create if port configured > 0
	if grpcLis != nil || cfg.Server.GRPCPort > 0 {
		var err error
		createdGrpcLis := false
		if grpcLis == nil {
			grpcAddr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
			grpcLis, err = net.Listen("tcp", grpcAddr)
			if err != nil {
				return nil, fmt.Errorf("failed to listen on gRPC port %s: %w", grpcAddr, err)
			}
			createdGrpcLis = true
			logger.Info("gRPC server will listen on", "address", grpcLis.Addr().String())
		} else {
			logger.Info("gRPC server will use provided listener", "address", grpcLis.Addr().String())
		}

		grpcSrv, err := NewGRPCServer(eng, putWorkerPool, batchWorkerPool, &cfg.Server, authenticator, logger)
		if err != nil {
			if createdGrpcLis && grpcLis != nil {
				grpcLis.Close()
			}
			return nil, fmt.Errorf("failed to create gRPC server: %w", err)
		}
		appSrv.grpcServer = grpcSrv
		appSrv.grpcLis = grpcLis
		appSrv.grpcLisOwned = createdGrpcLis
	} else {
		logger.Info("gRPC server is disabled (port is 0 or not configured).")
	}

	executor := nbql.NewExecutor(eng, clock.SystemClockDefault)

	// TCP: prefer provided listener, otherwise create if port configured > 0
	if tcpLis != nil || cfg.Server.TCPPort > 0 {
		var err error
		createdTcpLis := false
		if tcpLis == nil {
			tcpAddr := fmt.Sprintf(":%d", cfg.Server.TCPPort)
			tcpLis, err = net.Listen("tcp", tcpAddr)
			if err != nil {
				// only close grpcLis if we created it here
				if appSrv.grpcLis != nil && appSrv.grpcLisOwned {
					appSrv.grpcLis.Close()
				}
				return nil, fmt.Errorf("failed to create TCP server: %w", err)
			}
			createdTcpLis = true
		} else {
			// if a tcp listener was provided, log it
			logger.Info("TCP server will use provided listener", "address", tcpLis.Addr().String())
		}

		tcpServer := NewTCP2Server(executor, authenticator, cfg.Security.Enabled, logger)
		appSrv.tcpServer = tcpServer
		appSrv.tcpLis = tcpLis
		appSrv.tcpLisOwned = createdTcpLis
	}

	if cfg.QueryServer.Enabled {
		queryServer := NewHTTPServer(&cfg.QueryServer, logger, executor)
		appSrv.queryServer = queryServer
	}

	// --- Replication Setup ---
	if cfg.Replication.Mode != "disabled" {
		replicationLogger := logger.With("component", "replication")

		if cfg.Replication.Mode == "leader" {
			// Create the replication gRPC server (Leader side)
			replicationServer := replication.NewServer(
				eng.GetWAL(),
				eng.GetStringStore().(*indexer.StringStore), // Type assertion
				eng.GetSnapshotManager(),
				eng.GetSnapshotsBaseDir(),
				replicationLogger,
			)
			// Provide a function so the replication server can report the latest
			// applied sequence number in HealthCheck responses.
			replicationServer.LatestSeqProvider = func() uint64 { return eng.GetLatestAppliedSeqNum() }

			// Create the replication manager
			replManager, err := replication.NewManager(cfg.Replication, replicationServer, replicationLogger)
			if err != nil {
				return nil, fmt.Errorf("failed to create replication manager: %w", err)
			}
			appSrv.replicationManager = replManager
		}

		if cfg.Replication.Mode == "follower" {
			// Create the WAL applier (Follower side)
			logger.Debug("Leader Address", "addr", cfg.Replication.LeaderAddress)
			walApplier := replication.NewWALApplier(
				cfg.Replication.LeaderAddress,
				eng, // eng now satisfies the ReplicatedEngine interface
				eng.GetSnapshotManager(),
				eng.GetSnapshotsBaseDir(),
				replicationLogger,
			)
			appSrv.walApplier = walApplier
		}
	}

	return appSrv, nil
}

// Start runs all configured servers in parallel. It blocks until all servers stop.
func (s *AppServer) Start() error {
	if s.grpcServer == nil && s.tcpServer == nil && s.queryServer == nil && s.replicationManager == nil && s.walApplier == nil {
		s.logger.Error("No servers or replication components to start.")
		return nil
	}

	g, ctx := errgroup.WithContext(context.Background())
	var appCtx context.Context
	appCtx, s.cancel = context.WithCancel(ctx)

	s.putWorker.Start()
	s.batchWorker.Start()

	if s.grpcServer != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping gRPC server...")
				s.grpcServer.Stop()
			}()
			s.logger.Info("Starting gRPC server...")
			return s.grpcServer.Start(s.grpcLis)
		})
	}

	if s.tcpServer != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping TCP server...")
				s.tcpServer.Stop()
			}()
			s.logger.Info("Starting TCP server...")
			return s.tcpServer.Start(s.tcpLis)
		})
	}

	if s.queryServer != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping Query Server...")
				s.queryServer.Stop()
			}()

			return s.queryServer.Start()
		})
	}

	if s.replicationManager != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping replication manager...")
				s.replicationManager.Stop()
			}()
			s.logger.Info("Starting replication manager...")
			return s.replicationManager.Start(appCtx)
		})
	}

	if s.walApplier != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping WAL applier...")
				s.walApplier.Stop()
			}()
			s.logger.Info("Starting WAL applier...")
			s.walApplier.Start(appCtx)
			return nil
		})
	}

	s.logger.Info("Application server started. Waiting for servers to exit.")
	err := g.Wait()

	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("A server has failed, initiating shutdown.", "error", err)
		return fmt.Errorf("server group failed: %w", err)
	}

	s.logger.Info("Stopping worker pools...")
	s.putWorker.Stop()
	s.batchWorker.Stop()

	s.logger.Info("All servers have stopped gracefully.")
	return nil
}

// Stop gracefully shuts down all servers.
func (s *AppServer) Stop() {
	if s.walApplier != nil {
		s.walApplier.Stop()
	}
	if s.replicationManager != nil {
		s.replicationManager.Stop()
	}

	if s.cancel != nil {
		s.cancel()
	}
}
