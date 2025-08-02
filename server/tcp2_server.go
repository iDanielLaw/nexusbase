package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/core"
)

// TCP2Server handles raw TCP connections using the custom binary protocol.
// This implementation contains a self-managed accept loop and graceful shutdown logic.
type TCP2Server struct {
	listener        net.Listener
	executor        *nbql.Executor
	logger          *slog.Logger
	connWg          sync.WaitGroup // Tracks active connections for graceful shutdown.
	isStarted       bool
	quit            chan struct{}
	mu              sync.Mutex
	authenticator   core.IAuthenticator
	securityEnabled bool
	connHandler     *TCPConnectionHandler
}

// NewTCP2Server creates a new TCP server instance.
func NewTCP2Server(executor *nbql.Executor, authenticator core.IAuthenticator, securityEnabled bool, logger *slog.Logger) *TCP2Server {
	return &TCP2Server{
		executor:        executor,
		authenticator:   authenticator,
		securityEnabled: securityEnabled,
		connHandler:     NewTCPConnectionHandler(executor, authenticator, logger.With("handler", "tcp-conn")),
		logger:          logger.With("component", "TCP2Server"),
		quit:            make(chan struct{}),
	}
}

// Start begins listening for and handling TCP connections.
// This is a blocking call that runs the server's accept loop. It should be run in a goroutine.
func (s *TCP2Server) Start(lis net.Listener) error {
	s.mu.Lock()
	if s.isStarted {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.listener = lis
	s.isStarted = true
	s.mu.Unlock()
	s.logger.Info("TCP server listening", "address", lis.Addr().String())

	// The accept loop is now run directly, making Start a blocking method.
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// When Stop() closes the listener, Accept() returns an error.
			// We check if the server is quitting to distinguish a graceful shutdown
			// from an unexpected error.
			select {
			case <-s.quit:
				s.logger.Info("Server shutting down, stopping accept loop.")
				return nil // This is a graceful shutdown, not an error.
			default:
				// This is a real, unexpected error.
				s.logger.Error("Failed to accept connection", "error", err)
				return fmt.Errorf("failed to accept connection: %w", err)
			}
		}
		s.connWg.Add(1)
		go s.HandleConnection(conn)
	}

}

func (s *TCP2Server) HandleConnection(conn net.Conn) {
	defer s.connWg.Done()
	defer conn.Close()
	s.logger.Info("Accepted new connection", "remote_addr", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer writer.Flush() // Ensure any buffered data is written before closing.

	// Create a context for this connection that is cancelled when the server quits.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-s.quit
		cancel()
	}()

	// Check authentication only if security is enabled
	if s.securityEnabled {
		if !s.connHandler.HandleAuthentication(ctx, conn, reader, writer) {
			s.logger.Info("Authentication failed, closing connection", "remote_addr", conn.RemoteAddr())
			return
		}
	}

	for {
		select {
		case <-s.quit:
			return // Server is shutting down
		case <-ctx.Done():
			return // Connection context cancelled
		default:
		}

		cmdType, payload, err := nbql.ReadFrame(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "use of closed network connection") {
				s.logger.Warn("Failed to read frame, closing connection", "remote_addr", conn.RemoteAddr(), "error", err)
			}
			return
		}

		s.connHandler.HandleCommand(ctx, writer, cmdType, payload)
		if err := writer.Flush(); err != nil {
			s.logger.Warn("Failed to flush writer, closing connection", "remote_addr", conn.RemoteAddr(), "error", err)
			return
		}
	}
}

// Stop gracefully shuts down the TCP server.
func (s *TCP2Server) Stop() {
	s.mu.Lock()
	if !s.isStarted {
		s.mu.Unlock()
		return
	}
	s.isStarted = false
	s.mu.Unlock()

	s.logger.Info("Stopping TCP server...")

	// 1. Signal all goroutines to quit.
	close(s.quit)

	// 2. Close the listener to stop accepting new connections.
	if s.listener != nil {
		s.listener.Close()
	}

	// 3. Wait for all active connections to finish their work.
	s.logger.Info("Waiting for active connections to drain...")
	s.connWg.Wait()
	s.logger.Info("All TCP connections closed. Server stopped.")
}
