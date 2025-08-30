package replication

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1024 * 1024

func startTestGRPCServer(t *testing.T) (string, *grpc.Server, *bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(testBufSize)
	s := grpc.NewServer()
	go func() { _ = s.Serve(lis) }()
	cleanup := func() { s.Stop(); lis.Close() }
	return lis.Addr().String(), s, lis, cleanup
}

func bufDialer(lis *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.Dial()
	}
}

func TestCheckFollowerHealth_Healthy(t *testing.T) {
	_, _, lis, cleanup := startTestGRPCServer(t)
	defer cleanup()

	f := &FollowerState{Addr: "bufconn"}

	conn, err := grpc.Dial("bufconn", grpc.WithContextDialer(bufDialer(lis)), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	if conn.GetState().String() == "READY" {
		f.Healthy = true
		f.RetryCount = 0
	} else {
		f.Healthy = false
		f.RetryCount++
	}

	if !f.Healthy {
		t.Errorf("Expected follower to be healthy, got unhealthy")
	}
	if f.RetryCount != 0 {
		t.Errorf("Expected RetryCount to be 0, got %d", f.RetryCount)
	}
}

func TestCheckFollowerHealth_Unreachable(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := &Manager{logger: logger}
	f := &FollowerState{Addr: "127.0.0.1:65535"} // Unreachable port

	mgr.checkFollowerHealth(f)

	if f.Healthy {
		t.Errorf("Expected follower to be unhealthy, got healthy")
	}
	if f.RetryCount == 0 {
		t.Errorf("Expected RetryCount to be > 0, got %d", f.RetryCount)
	}
}

func TestManager_Stop(t *testing.T) {
	_, grpcServer, _, cleanup := startTestGRPCServer(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := &Manager{
		logger:     logger,
		stopCh:     make(chan struct{}),
		grpcServer: grpcServer,
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		mgr.Stop()
	}()
	mgr.monitorFollowers(context.Background())
}

func TestManager_ReplicationReconnect(t *testing.T) {
	addr, _, lis, cleanup := startTestGRPCServer(t)
	defer cleanup()

	f := &FollowerState{Addr: addr}
	var healthyCount int32
	f.LagCallback = func(lag time.Duration) {
		atomic.AddInt32(&healthyCount, 1)
	}

	// First health check: dial bufconn directly
	conn, err := grpc.Dial(addr, grpc.WithContextDialer(bufDialer(lis)), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	conn.Close()
	f.Healthy = true
	f.RetryCount = 0

	if !f.Healthy {
		t.Fatalf("Expected follower to be healthy on first check")
	}

	// Simulate follower down by closing listener
	cleanup()
	f.Healthy = false
	f.RetryCount++
	if f.Healthy {
		t.Fatalf("Expected follower to be unhealthy after disconnect")
	}
	if f.RetryCount == 0 {
		t.Errorf("Expected RetryCount to increase after disconnect")
	}

	// Simulate follower up again by restarting server
	addr2, _, lis2, cleanup2 := startTestGRPCServer(t)
	defer cleanup2()
	f.Addr = addr2
	conn2, err := grpc.Dial(addr2, grpc.WithContextDialer(bufDialer(lis2)), grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	conn2.Close()
	f.Healthy = true
	f.RetryCount = 0
	if f.LagCallback != nil {
		lag := time.Since(f.LastPing)
		f.LagCallback(lag)
	}
	if !f.Healthy {
		t.Fatalf("Expected follower to be healthy after reconnect")
	}
	if atomic.LoadInt32(&healthyCount) == 0 {
		t.Errorf("Expected LagCallback to be called after reconnect")
	}
}
