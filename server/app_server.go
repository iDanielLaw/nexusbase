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
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexuscore/utils/clock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// AppServer manages all network-facing servers (gRPC, TCP, etc.).
type AppServer struct {
	grpcLis           net.Listener
	tcpLis            net.Listener
	replicationLis    net.Listener
	queryServer       *HTTPServer
	grpcServer        *GRPCServer
	tcpServer         *TCP2Server
	replicationServer *ReplicationGRPCServer
	putWorker         *WorkerPool
	batchWorker       *WorkerPool
	cfg               *config.Config
	logger            *slog.Logger
	engine            engine.StorageEngineInterface
	cancel            context.CancelFunc
}

// NewAppServer creates and initializes a new application server.
func NewAppServer(eng engine.StorageEngineInterface, cfg *config.Config, logger *slog.Logger) (*AppServer, error) {
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
	// Pool for single Puts (high frequency, short jobs)
	putWorkerPool := NewWorkerPool(runtime.NumCPU(), 1024, eng, logger.With("pool", "put"))

	// Pool for Batch Puts (lower frequency, potentially long jobs)
	// We can configure this pool with different resources if needed, e.g., fewer workers.
	batchWorkerPool := NewWorkerPool(runtime.NumCPU(), 512, eng, logger.With("pool", "batch"))

	appSrv := &AppServer{
		cfg:         cfg,
		logger:      logger.With("component", "AppServer"),
		engine:      eng,
		putWorker:   putWorkerPool,
		batchWorker: batchWorkerPool,
	}

	// 1. Initialize the gRPC server if the port is configured.
	if cfg.Server.GRPCPort > 0 {
		grpcAddr := fmt.Sprintf(":%d", cfg.Server.GRPCPort)
		grpcLis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on gRPC port %s: %w", grpcAddr, err)
		}
		logger.Info("gRPC server will listen on", "address", grpcLis.Addr().String())

		grpcSrv, err := NewGRPCServer(eng, putWorkerPool, batchWorkerPool, &cfg.Server, authenticator, logger)
		if err != nil {
			grpcLis.Close()
			return nil, fmt.Errorf("failed to create gRPC server: %w", err)
		}
		appSrv.grpcServer = grpcSrv
		appSrv.grpcLis = grpcLis
	} else {
		logger.Info("gRPC server is disabled (port is 0 or not configured).")
	}

	executor := nbql.NewExecutor(eng, clock.SystemClockDefault)

	// 2. Initialize the TCP (NBQL) server if the port is configured.
	if cfg.Server.TCPPort > 0 {
		tcpAddr := fmt.Sprintf(":%d", cfg.Server.TCPPort)
		tcpLis, err := net.Listen("tcp", tcpAddr)
		if err != nil {
			if appSrv.grpcLis != nil {
				appSrv.grpcLis.Close()
			}
			return nil, fmt.Errorf("failed to create TCP server: %w", err)
		}

		tcpServer := NewTCP2Server(executor, authenticator, cfg.Security.Enabled, logger)
		appSrv.tcpServer = tcpServer
		appSrv.tcpLis = tcpLis
	}

	// 3. Initialize the Replication gRPC server if the port is configured.
	// This assumes `ReplicationPort` is added to `config.ServerConfig`.
	if cfg.Server.ReplicationPort > 0 {
		replicationAddr := fmt.Sprintf(":%d", cfg.Server.ReplicationPort)
		replicationLis, err := net.Listen("tcp", replicationAddr)
		if err != nil {
			// Clean up previously opened listeners
			if appSrv.grpcLis != nil {
				appSrv.grpcLis.Close()
			}
			if appSrv.tcpLis != nil {
				appSrv.tcpLis.Close()
			}
			return nil, fmt.Errorf("failed to listen on replication port %s: %w", replicationAddr, err)
		}
		logger.Info("Replication gRPC server will listen on", "address", replicationLis.Addr().String())

		replicationSrv, err := NewReplicationGRPCServer(eng, &cfg.Server, logger)
		if err != nil {
			// Clean up all listeners
			if appSrv.grpcLis != nil {
				appSrv.grpcLis.Close()
			}
			if appSrv.tcpLis != nil {
				appSrv.tcpLis.Close()
			}
			replicationLis.Close()
			return nil, fmt.Errorf("failed to create replication gRPC server: %w", err)
		}
		appSrv.replicationServer = replicationSrv
		appSrv.replicationLis = replicationLis
	}

	if cfg.QueryServer.Enabled {
		queryServer := NewHTTPServer(&cfg.QueryServer, logger, executor)
		appSrv.queryServer = queryServer
	}
	return appSrv, nil
}

// Start runs all configured servers in parallel. It blocks until all servers stop.
func (s *AppServer) Start() error {
	if s.grpcServer == nil && s.tcpServer == nil && s.queryServer == nil && s.replicationServer == nil {
		s.logger.Error("No servers to start.")
		return nil
	}

	// Create a new context for the errgroup that can be cancelled by Stop().
	g, ctx := errgroup.WithContext(context.Background())
	var appCtx context.Context
	appCtx, s.cancel = context.WithCancel(ctx)

	s.putWorker.Start()
	s.batchWorker.Start()

	// Start gRPC server if it exists
	if s.grpcServer != nil {
		g.Go(func() error {
			// This goroutine waits for the shutdown signal and stops the gRPC server.
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping gRPC server...")
				s.grpcServer.Stop()
			}()
			s.logger.Info("Starting gRPC server...")
			return s.grpcServer.Start(s.grpcLis)
		})
	}

	// Start Replication gRPC server if it exists
	if s.replicationServer != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping Replication gRPC server...")
				s.replicationServer.Stop()
			}()
			s.logger.Info("Starting Replication gRPC server...")
			return s.replicationServer.Start(s.replicationLis)
		})
	}

	// Start TCP (NBQL) server if it exists
	if s.tcpServer != nil {
		g.Go(func() error {
			// This goroutine waits for the shutdown signal and stops the TCP server.
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping TCP server...")
				s.tcpServer.Stop()
			}()
			s.logger.Info("Starting TCP server...")
			return s.tcpServer.Start(s.tcpLis)
		})
	}

	// Start Replication gRPC server if it exists
	if s.replicationServer != nil {
		g.Go(func() error {
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping Replication gRPC server...")
				s.replicationServer.Stop()
			}()
			s.logger.Info("Starting Replication gRPC server...")
			return s.replicationServer.Start(s.replicationLis)
		})
	}

	//Start Query Server if it exist
	if s.queryServer != nil {
		g.Go(func() error {
			// This goroutine waits for the shutdfown signal and stop the Query Server.
			go func() {
				<-appCtx.Done()
				s.logger.Info("Context cancelled, stopping Query Server...")
				s.queryServer.Stop()
			}()

			return s.queryServer.Start()
		})
	}

	s.logger.Info("Application server started. Waiting for servers to exit.")
	// Wait for all servers to stop. g.Wait() returns the first non-nil error.
	err := g.Wait()

	// Differentiate between a graceful shutdown and an actual error.
	// On graceful shutdown, Serve() returns specific errors.
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) && !errors.Is(err, net.ErrClosed) && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Error("A server has failed, initiating shutdown.", "error", err)
		return fmt.Errorf("server group failed: %w", err)
	}

	// Stop the worker pools after all servers have shut down to process in-flight requests.
	s.logger.Info("Stopping worker pools...")
	s.putWorker.Stop()
	s.batchWorker.Stop()

	s.logger.Info("All servers have stopped gracefully.")
	return nil
}

// Stop gracefully shuts down all servers.
func (s *AppServer) Stop() {
	// Trigger the cancellation of the context created in Start().
	// This will cause the goroutines in the errgroup to stop.
	if s.cancel != nil {
		s.cancel()
	}
}

// GetEngine returns the underlying storage engine. This is useful for tests.
func (s *AppServer) GetEngine() engine.StorageEngineInterface {
	return s.engine
}
