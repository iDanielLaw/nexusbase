package tcp

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Handler interface {
	HandlerConnection(ctx context.Context, conn net.Conn)
}

type FuncHandler func(ctx context.Context, conn net.Conn)

var _ Handler = FuncHandler(nil)

func (f FuncHandler) HandlerConnection(ctx context.Context, conn net.Conn) {
	f(ctx, conn)
}

type Middleware func(Handler) Handler

type Option func(*TCPServerOptions)

type TCPServerOptions struct {
	Logger          *slog.Logger
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
}

type TCPServer struct {
	opts         *TCPServerOptions
	listener     net.Listener
	baseHandler  Handler
	chainHandler Handler
	logger       *slog.Logger

	middlewares []Middleware

	wg    sync.WaitGroup
	conns chan net.Conn

	ctx    context.Context
	cancel context.CancelFunc

	chShutdown chan os.Signal
	done       chan struct{}
}

func DefaultOptions() *TCPServerOptions {
	return &TCPServerOptions{
		Logger:          slog.Default(),
		MaxConnections:  0,
		ReadTimeout:     0,
		WriteTimeout:    0,
		ShutdownTimeout: 30 * time.Second,
	}
}

func NewTCPServer(handler Handler, opts ...Option) (*TCPServer, error) {
	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}
	options := DefaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &TCPServer{
		opts:        options,
		baseHandler: handler,
		logger:      slog.Default(),
		conns:       make(chan net.Conn, 30),
		ctx:         ctx,
		cancel:      cancel,
		chShutdown:  make(chan os.Signal, 1),
		done:        make(chan struct{}),
	}

	signal.Notify(s.chShutdown, syscall.SIGINT, syscall.SIGTERM)

	return s, nil
}

func (s *TCPServer) buildChain() {
	s.chainHandler = s.baseHandler
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		s.chainHandler = s.middlewares[i](s.chainHandler)
	}
}

func (s *TCPServer) Use(middlewares ...Middleware) {
	s.middlewares = append(s.middlewares, middlewares...)
}

func (s *TCPServer) Start(lis net.Listener) error {
	s.buildChain()

	s.listener = lis

	go s.handleShutdown()

	for {
		select {
		case <-s.ctx.Done():
			// stop by context
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Op == "accept" && opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
				continue
			}

			if s.opts.MaxConnections > 0 {
				select {
				case s.conns <- conn:
					s.wg.Add(1)
					go s.handleClient(<-s.conns)
				case <-s.ctx.Done():
					conn.Close()
					return nil
				default:
					s.logger.Warn("Max connections reached, rejecting new connection.")
					conn.Close()
				}
			} else {
				s.wg.Add(1)
				go s.handleClient(conn)
			}
		}
	}
}

func (s *TCPServer) handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		s.wg.Done() // Decrement WaitGroup when goroutine finishes
		if s.opts.MaxConnections > 0 {
			<-s.conns // Remove connection from buffered channel
		}
		s.logger.Info("Connection closed.", "remote_addr", conn.RemoteAddr())
	}()

	s.logger.Info("New connection from", "remote_addr", conn.RemoteAddr())

	// Set read/write deadlines
	if s.opts.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(s.opts.ReadTimeout))
	}
	if s.opts.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
	}

	connCtx, connCancel := context.WithCancel(s.ctx)
	defer connCancel()

	// Call the user-defined handler
	s.chainHandler.HandlerConnection(connCtx, conn)
}

func (s *TCPServer) handleShutdown() {
	defer close(s.done) // Signal that shutdown process is complete

	select {
	case sig := <-s.chShutdown:
		s.logger.Info("Received OS signal, initiating graceful shutdown...", "sig: ", sig)
	case <-s.ctx.Done():
		s.logger.Info("Server context cancelled, initiating graceful shutdown...")
	}

	// Close the listener to stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			s.logger.Error("Error closing listener: ", "err", err)
		} else {
			s.logger.Info("Stopped accepting new connections.")
		}
	}

	// Create a context for waiting on active connections with a timeout
	waitCtx, waitCancel := context.WithTimeout(context.Background(), s.opts.ShutdownTimeout)
	defer waitCancel()

	// Wait for all active connection goroutines to finish
	go func() {
		s.wg.Wait()
		waitCancel() // Signal that all goroutines are done
	}()

	select {
	case <-waitCtx.Done():
		if waitCtx.Err() == context.DeadlineExceeded {
			s.logger.Error("Graceful shutdown timeout reached. Forcibly shutting down remaining connections.")
		} else {
			s.logger.Info("All active connections handled. Server gracefully shut down.")
		}
	default:
		s.logger.Info("Waiting for active connections to finish...")
	}
}

// Shutdown attempts to gracefully shut down the server.
func (s *TCPServer) Shutdown() {
	s.cancel() // Signal server context cancellation
	// Send a signal to the shutdown handler goroutine to ensure it runs
	select {
	case s.chShutdown <- syscall.SIGTERM:
	default: // If already sent or channel full, no op
	}
	<-s.done // Wait for the shutdown process to complete
}

// WithReadTimeout sets the read timeout for connections.
func WithReadTimeout(d time.Duration) Option {
	return func(o *TCPServerOptions) {
		o.ReadTimeout = d
	}
}

// WithWriteTimeout sets the write timeout for connections.
func WithWriteTimeout(d time.Duration) Option {
	return func(o *TCPServerOptions) {
		o.WriteTimeout = d
	}
}

// WithMaxConnections sets the maximum number of concurrent connections.
// A value of 0 means no limit.
func WithMaxConnections(max int) Option {
	return func(o *TCPServerOptions) {
		o.MaxConnections = max
	}
}

// WithLogger sets a custom logger for the server.
func WithLogger(l *slog.Logger) Option {
	return func(o *TCPServerOptions) {
		o.Logger = l
	}
}

// WithShutdownTimeout sets the maximum time to wait for active connections to finish during shutdown.
func WithShutdownTimeout(d time.Duration) Option {
	return func(o *TCPServerOptions) {
		o.ShutdownTimeout = d
	}
}
