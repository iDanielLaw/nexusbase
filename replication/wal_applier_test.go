package replication

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// --- Mocks ---

// mockReplicatedEngine is a mock implementation of the ReplicatedEngine interface.
type mockReplicatedEngine struct {
	mu         sync.Mutex
	appliedSeq uint64
	applyErr   error
	appliedCh  chan *pb.WALEntry
	applyFunc  func(entry *pb.WALEntry) error // Function hook for custom logic
}

func newMockReplicatedEngine(appliedCh chan *pb.WALEntry) *mockReplicatedEngine {
	return &mockReplicatedEngine{
		appliedCh: appliedCh,
	}
}

func (m *mockReplicatedEngine) ApplyReplicatedEntry(ctx context.Context, entry *pb.WALEntry) error {
	if m.applyFunc != nil {
		return m.applyFunc(entry)
	}

	// Default behavior
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.applyErr != nil {
		return m.applyErr
	}

	m.appliedSeq = entry.GetSequenceNumber()

	if m.appliedCh != nil {
		m.appliedCh <- entry
	}
	return nil
}

func (m *mockReplicatedEngine) GetLatestAppliedSeqNum() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appliedSeq
}

// mockReplicationServer is a mock implementation of the ReplicationServiceServer.
type mockReplicationServer struct {
	pb.UnimplementedReplicationServiceServer
	mu           sync.Mutex
	stream       pb.ReplicationService_StreamWALServer
	streamErr    error
	sendErr      error
	entries      []*pb.WALEntry
	streamReqCh  chan *pb.StreamWALRequest // Channel to notify on new stream requests
	streamStopCh chan struct{}             // Channel to stop a stream from the server side
}

func (s *mockReplicationServer) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	s.mu.Lock()
	s.stream = stream
	from := req.GetFromSequenceNumber()
	if s.streamReqCh != nil {
		s.streamReqCh <- req
	}
	s.mu.Unlock()

	if s.streamErr != nil {
		return s.streamErr
	}

	for _, entry := range s.entries {
		if entry.GetSequenceNumber() >= from {
			if err := stream.Send(entry); err != nil {
				return err
			}
		}
	}

	if s.sendErr != nil {
		return s.sendErr
	}

	// Wait for either the client or the test to cancel the context
	select {
	case <-stream.Context().Done():
		return stream.Context().Err()
	case <-s.streamStopCh:
		return errors.New("stream stopped by server")
	}
}

// --- Test Setup ---

const bufSize = 1024 * 1024

// setupTest initializes a mock server and a WALApplier connected to it.
func setupTest(t *testing.T) (*WALApplier, *mockReplicatedEngine, *mockReplicationServer, func()) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	appliedCh := make(chan *pb.WALEntry, 10)

	mockEngine := newMockReplicatedEngine(appliedCh)
	mockServer := &mockReplicationServer{
		streamStopCh: make(chan struct{}),
	}

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil {
			// Expected error on server stop
		}
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	applier := NewWALApplier("bufnet", mockEngine, logger)

	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	applier.conn = conn
	applier.client = pb.NewReplicationServiceClient(conn)

	cleanup := func() {
		applier.Stop()
		s.Stop()
		lis.Close()
		close(appliedCh)
		close(mockServer.streamStopCh)
	}

	return applier, mockEngine, mockServer, cleanup
}

// newTestWALEntry creates a valid WALEntry for testing.
func newTestWALEntry(seqNum uint64, metric string, ts int64) *pb.WALEntry {
	return &pb.WALEntry{
		SequenceNumber: seqNum,
		EntryType:      pb.WALEntry_PUT_EVENT,
		Metric:         metric,
		Tags:           map[string]string{"host": "test-host"},
		Timestamp:      ts,
	}
}

// --- Test Cases ---

func TestWALApplier_SuccessfulReplication(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(2, "metric1", 2000),
		newTestWALEntry(3, "metric1", 3000),
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)

	for i := 0; i < len(mockServer.entries); i++ {
		select {
		case applied := <-mockEngine.appliedCh:
			assert.Equal(t, uint64(i+1), applied.GetSequenceNumber())
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for entry %d to be applied", i+1)
		}
	}

	assert.Equal(t, uint64(3), mockEngine.GetLatestAppliedSeqNum())
	cancel() // Stop the loop
}

func TestWALApplier_ReconnectsAndResumes(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{newTestWALEntry(1, "metric1", 1000)}
	mockServer.sendErr = io.EOF // Simulate clean stream end

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}
	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum())

	// Give the loop time to retry
	time.Sleep(100 * time.Millisecond)

	mockServer.mu.Lock()
	mockServer.sendErr = nil
	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000), // Server might resend old entries
		newTestWALEntry(2, "metric1", 2000),
	}
	mockServer.mu.Unlock()

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(2), applied.GetSequenceNumber())
	case <-time.After(6 * time.Second): // The retry has a 5s sleep
		t.Fatal("timed out waiting for the second entry after reconnect")
	}

	assert.Equal(t, uint64(2), mockEngine.GetLatestAppliedSeqNum())
	cancel()
}

func TestWALApplier_HandlesOutOfOrderEntry(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(3, "metric1", 3000), // Out-of-order
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)
	defer cancel()

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// The loop should be stuck retrying after the out-of-order error
	select {
	case applied := <-mockEngine.appliedCh:
		t.Fatalf("received unexpected entry %d", applied.GetSequenceNumber())
	case <-time.After(100 * time.Millisecond):
		// Expected
	}

	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum())
}

func TestWALApplier_HandlesApplyError(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(2, "metric1", 2000),
	}

	expectedErr := errors.New("failed to apply entry")
	mockEngine.applyFunc = func(entry *pb.WALEntry) error {
		mockEngine.mu.Lock()
		defer mockEngine.mu.Unlock()

		if entry.GetSequenceNumber() == 2 {
			return expectedErr
		}

		mockEngine.appliedSeq = entry.GetSequenceNumber()
		if mockEngine.appliedCh != nil {
			mockEngine.appliedCh <- entry
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)
	defer cancel()

	select {
	case applied := <-mockEngine.appliedCh:
		require.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// The loop should be stuck retrying after the apply error
	time.Sleep(50 * time.Millisecond)

	select {
	case applied := <-mockEngine.appliedCh:
		t.Fatalf("received unexpected entry %d after apply error", applied.GetSequenceNumber())
	case <-time.After(50 * time.Millisecond):
		// Good, no second entry was applied.
	}

	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum())
}

// --- New Lifecycle and Error Path Tests ---

// setupTestForLifecycle is a simplified setup for testing the Start/Stop lifecycle.
func setupTestForLifecycle(t *testing.T) (*WALApplier, *mockReplicatedEngine, *mockReplicationServer, func()) {
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	appliedCh := make(chan *pb.WALEntry, 10)
	mockEngine := newMockReplicatedEngine(appliedCh)
	mockServer := &mockReplicationServer{
		streamReqCh:  make(chan *pb.StreamWALRequest, 1),
		streamStopCh: make(chan struct{}),
	}
	pb.RegisterReplicationServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	applier := NewWALApplier(lis.Addr().String(), mockEngine, logger)
	applier.dialOpts = []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // Ensure connection is made before Start returns
	}

	cleanup := func() {
		applier.Stop()
		s.Stop()
		lis.Close()
		close(appliedCh)
		close(mockServer.streamReqCh)
		close(mockServer.streamStopCh)
	}

	return applier, mockEngine, mockServer, cleanup
}

func TestWALApplier_Start_DialError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockEngine := newMockReplicatedEngine(nil)

	// Use an invalid address to force a dial error.
	applier := NewWALApplier("invalid-address:12345", mockEngine, logger)
	// Inject a dial option that will time out quickly.
	applier.dialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// This call will block until the context times out, then return.
	applier.Start(ctx)

	assert.Nil(t, applier.conn, "Connection should be nil on dial error")
	assert.Nil(t, applier.client, "Client should be nil on dial error")
	assert.Nil(t, applier.cancel, "Cancel function should not be set on dial error")
}

func TestWALApplier_Lifecycle_StartReplicateStop(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTestForLifecycle(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{newTestWALEntry(1, "metric-lifecycle", 1000)}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go applier.Start(ctx)

	// Wait for the replication loop to request a stream
	select {
	case req := <-mockServer.streamReqCh:
		assert.Equal(t, uint64(1), req.GetFromSequenceNumber())
	case <-ctx.Done():
		t.Fatal("timed out waiting for stream request")
	}

	// Wait for the entry to be applied
	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-ctx.Done():
		t.Fatal("timed out waiting for entry to be applied")
	}

	// Stop the applier
	applier.Stop()

	// Assert that the connection is closed
	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "Connection should be shut down after stop")
}

func TestWALApplier_ReplicationLoop_StreamErrorRetry(t *testing.T) {
	applier, _, mockServer, cleanup := setupTestForLifecycle(t)
	defer cleanup()

	// Configure server to return an error on the first stream attempt
	mockServer.streamErr = errors.New("transient gRPC error")

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second) // 7s > 5s retry sleep
	defer cancel()
	go applier.Start(ctx)

	// 1. Wait for the first stream request, which will fail
	select {
	case <-mockServer.streamReqCh:
		// Good, first attempt happened
	case <-ctx.Done():
		t.Fatal("timed out waiting for the first stream request")
	}

	// 2. After the error, the server should behave normally
	mockServer.mu.Lock()
	mockServer.streamErr = nil
	mockServer.mu.Unlock()

	// 3. Wait for the second, successful stream request
	select {
	case <-mockServer.streamReqCh:
		// Good, second attempt happened after retry
	case <-ctx.Done():
		t.Fatal("timed out waiting for the second stream request")
	}
}

func TestWALApplier_ProcessStream_RecvError(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// Server sends one entry, then returns an error
	mockServer.entries = []*pb.WALEntry{newTestWALEntry(1, "metric1", 1000)}
	mockServer.sendErr = errors.New("recv error")

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)
	defer cancel()

	// Wait for the first entry to be applied
	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// The loop should retry after the Recv error
	// Give it time to do so
	time.Sleep(100 * time.Millisecond)

	// Check that no more entries were applied
	select {
	case e := <-mockEngine.appliedCh:
		t.Fatalf("should not have applied more entries, but got %v", e)
	default:
		// OK
	}
}

func TestWALApplier_Stop_Idempotent(t *testing.T) {
	applier, _, _, cleanup := setupTestForLifecycle(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go applier.Start(ctx)

	// Wait for connection to be established
	assert.Eventually(t, func() bool {
		return applier.conn != nil && applier.conn.GetState() == connectivity.Ready
	}, 1*time.Second, 10*time.Millisecond)

	applier.Stop()
	// Calling Stop again should not panic
	assert.NotPanics(t, func() {
		applier.Stop()
	})
}