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

// TCPServer handles raw TCP connections using the custom binary protocol.
type TCPServer struct {
	listener        net.Listener
	readyCh         chan struct{}
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

// NewTCPServer creates a new TCP server instance.
func NewTCPServer(executor *nbql.Executor, authenticator core.IAuthenticator, securityEnabled bool, logger *slog.Logger) *TCPServer {
	return &TCPServer{
		executor:        executor,
		authenticator:   authenticator,
		securityEnabled: securityEnabled,
		connHandler:     NewTCPConnectionHandler(executor, authenticator, logger.With("handler", "tcp-conn")),
		readyCh:         make(chan struct{}),
		logger:          logger.With("component", "TCPServer"),
		quit:            make(chan struct{}),
	}
}

// Start begins listening for and handling TCP connections.
// This is a blocking call that runs the server's accept loop. It should be run in a goroutine.
func (s *TCPServer) Start(lis net.Listener) error {
	s.mu.Lock()
	if s.isStarted {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.listener = lis
	s.isStarted = true
	s.mu.Unlock()
	close(s.readyCh)
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
		go s.handleConnection(conn)
	}
}

// Stop gracefully shuts down the TCP server.
func (s *TCPServer) Stop() {
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

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer s.connWg.Done()
	defer conn.Close()
	s.logger.Info("Accepted new connection", "remote_addr", conn.RemoteAddr())
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	defer writer.Flush() // Ensure any buffered data is written before closing.

	ctx := context.Background()

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
		default:
		}

		// ReadFrame will block until a full frame is received, or the connection
		// is closed, or an error occurs. This is a simpler and more stable approach.
		cmdType, payload, err := nbql.ReadFrame(reader)
		if err != nil {
			// If the client closes the connection, io.EOF will be returned.
			// A net.ErrClosed can happen on graceful shutdown.
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "use of closed network connection") {
				s.logger.Warn("Failed to read frame, closing connection", "remote_addr", conn.RemoteAddr(), "error", err)
			}
			return
		}

		s.connHandler.HandleCommand(ctx, writer, cmdType, payload)
		// Flush the writer's buffer to send the response immediately.
		if err := writer.Flush(); err != nil {
			s.logger.Warn("Failed to flush writer, closing connection", "remote_addr", conn.RemoteAddr(), "error", err)
			return
		}
	}
}
