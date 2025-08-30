package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- Mocks ---

// mockReplicatedEngine is a mock implementation of the ReplicatedEngine interface.
type mockReplicatedEngine struct {
	mock.Mock
	mu         sync.Mutex
	appliedSeq uint64
	applyErr   error
	appliedCh  chan *pb.WALEntry
	applyFunc  func(entry *pb.WALEntry) error // Function hook for custom logic
}

func (m *mockReplicatedEngine) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *mockReplicatedEngine) ReplaceWithSnapshot(snapshotDir string) error {
	args := m.Called(snapshotDir)
	return args.Error(0)
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
	// This mock allows both testify mock calls and direct manipulation for simplicity.
	// Check if there's a configured expectation for this method.
	for _, call := range m.Mock.ExpectedCalls {
		if call.Method == "GetLatestAppliedSeqNum" {
			args := m.Called()
			if len(args) > 0 && args.Get(0) != nil {
				return args.Get(0).(uint64)
			}
			// If .Return() is called with no arguments, fall through to return the internal state.
			break
		}
	}

	// If no expectation is set, just return the internal state without panicking.
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appliedSeq
}

// mockReplicationServer is a mock implementation of the ReplicationServiceServer.
type mockReplicationServer struct {
	pb.UnimplementedReplicationServiceServer
	mu                  sync.Mutex
	stream              pb.ReplicationService_StreamWALServer
	streamErr           error
	sendErr             error
	entries             []*pb.WALEntry
	streamReqCh         chan *pb.StreamWALRequest // Channel to notify on new stream requests
	streamStopCh        chan struct{}             // Channel to stop a stream from the server side
	getSnapshotInfoFunc func() (*pb.SnapshotInfo, error)
	streamSnapshotFunc  func(*pb.StreamSnapshotRequest, pb.ReplicationService_StreamSnapshotServer) error
}

func (s *mockReplicationServer) GetLatestSnapshotInfo(ctx context.Context, req *pb.GetLatestSnapshotInfoRequest) (*pb.SnapshotInfo, error) {
	if s.getSnapshotInfoFunc != nil {
		return s.getSnapshotInfoFunc()
	}
	return nil, status.Errorf(codes.NotFound, "no snapshots on mock")
}

func (s *mockReplicationServer) StreamSnapshot(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
	if s.streamSnapshotFunc != nil {
		return s.streamSnapshotFunc(req, stream)
	}
	return status.Errorf(codes.Unimplemented, "not implemented in mock")
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
		time.Sleep(50 * time.Millisecond)
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
		time.Sleep(50 * time.Millisecond)
		return s.sendErr
	}

	return nil
}

// --- Test Setup ---

const bufSize = 1024 * 1024

func setupTest(t *testing.T) (*WALApplier, *mockReplicatedEngine, *mockReplicationServer, func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	appliedCh := make(chan *pb.WALEntry, 10)
	mockEngine := newMockReplicatedEngine(appliedCh)
	mockServer := &mockReplicationServer{
		streamReqCh:  make(chan *pb.StreamWALRequest, 10),
		streamStopCh: make(chan struct{}),
	}

	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, mockServer)

	go func() {
		if err := s.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Logf("gRPC server error: %v", err)
		}
	}()

	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	applier := NewWALApplier(lis.Addr().String(), mockEngine, nil, t.TempDir(), logger)
	applier.retrySleep = 50 * time.Millisecond
	applier.dialOpts = []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
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
	defer cancel()
	go applier.Start(ctx)

	for i := 0; i < len(mockServer.entries); i++ {
		select {
		case applied := <-mockEngine.appliedCh:
			assert.Equal(t, uint64(i+1), applied.GetSequenceNumber())
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for entry %d to be applied", i+1)
		}
	}

	assert.Equal(t, uint64(3), mockEngine.GetLatestAppliedSeqNum())
}

func TestWALApplier_ReconnectsAndResumes(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// --- First Connection ---
	// Server will send one entry, then gracefully close the stream (io.EOF).
	mockServer.entries = []*pb.WALEntry{newTestWALEntry(1, "metric1", 1000)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go applier.Start(ctx)

	// Wait for the first entry to be applied.
	select {
	case applied := <-mockEngine.appliedCh:
		require.Equal(t, uint64(1), applied.GetSequenceNumber(), "first entry should be applied")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}
	require.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum(), "sequence number should be 1")

	// --- Second Connection ---
	// After the first stream ends, the applier should try to reconnect.
	// We'll set up the server to send a new entry for the second connection.
	mockServer.mu.Lock()
	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000), // The server can resend old entries
		newTestWALEntry(2, "metric1", 2000),
	}
	mockServer.mu.Unlock()

	// Wait for the second entry to be applied.
	// The applier will reconnect, ask for seq > 1, and should receive entry 2.
	select {
	case applied := <-mockEngine.appliedCh:
		require.Equal(t, uint64(2), applied.GetSequenceNumber(), "second entry should be applied after reconnect")
	case <-time.After(4 * time.Second): // Give it a bit more time to handle the reconnect logic
		t.Fatal("timed out waiting for the second entry after reconnect")
	}
	require.Equal(t, uint64(2), mockEngine.GetLatestAppliedSeqNum(), "sequence number should be 2")
}

func TestWALApplier_HandlesOutOfOrderEntry(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(3, "metric1", 3000), // Out-of-order (gap)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go applier.Start(ctx)

	// Wait for the first entry
	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// The loop should be stopped by the critical gap error.
	select {
	case applied := <-mockEngine.appliedCh:
		t.Fatalf("received unexpected entry %d", applied.GetSequenceNumber())
	case <-time.After(200 * time.Millisecond):
		// Expected to not receive anything else.
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
	applyCallCount := 0
	mockEngine.applyFunc = func(entry *pb.WALEntry) error {
		mockEngine.mu.Lock()
		defer mockEngine.mu.Unlock()

		if entry.GetSequenceNumber() == 2 {
			applyCallCount++
			if applyCallCount <= 3 { // Simulate failure for the first 3 attempts
				return expectedErr
			}
		}

		mockEngine.appliedSeq = entry.GetSequenceNumber()
		if mockEngine.appliedCh != nil {
			mockEngine.appliedCh <- entry
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the replication loop in a separate goroutine
	go applier.Start(ctx)

	// Wait for the first entry to be applied
	select {
	case applied := <-mockEngine.appliedCh:
		require.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// Now, the applier should try to apply entry 2, fail, and retry.
	// It should retry maxApplyRetries (3) times, with 1s sleep between retries.
	// So, it should take at least 3 seconds for the loop to return an error.
	// We'll wait for the applier to stop, which it should after the critical error.
	assert.Eventually(t, func() bool {
		// The applier should stop after repeated apply failures.
		// We check if the connection is shut down.
		return applier.conn.GetState() == connectivity.Shutdown
	}, 5*time.Second, 100*time.Millisecond, "applier should stop after critical apply error")

	// Assert that the apply function for entry 2 was called maxApplyRetries times
	assert.Equal(t, 3, applyCallCount, "ApplyReplicatedEntry for entry 2 should be called maxApplyRetries times")

	// Assert that the sequence number remains at 1 (entry 2 was never successfully applied)
	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum(), "Sequence number should remain at 1")
}

func TestBootstrap_SnapshotRestoreSuccess(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// --- Setup Mocks ---
	leaderSnapSeqNum := uint64(100)

	// 1. Mock engine state
	mockEngine.On("GetLatestAppliedSeqNum").Return(uint64(10)).Once() // First call in bootstrap
	mockEngine.On("GetLatestAppliedSeqNum").Return(leaderSnapSeqNum)  // Subsequent calls in replicationLoop
	mockEngine.On("Close").Return(nil).Once()
	mockEngine.On("ReplaceWithSnapshot", mock.AnythingOfType("string")).Return(nil).Once().Run(func(args mock.Arguments) {
		// After restoring, the engine's sequence number must be updated to the snapshot's sequence number.
		// Note: In a real implementation, the engine would do this itself after a successful restore.
		mockEngine.mu.Lock()
		mockEngine.appliedSeq = leaderSnapSeqNum
		mockEngine.mu.Unlock()
	})

	// 2. Mock server snapshot info
	snapshotID := "snapshot-123"
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{
			Id:                    snapshotID,
			LastWalSequenceNumber: leaderSnapSeqNum,
			CreatedAt:             timestamppb.Now(),
		}, nil
	}

	// 3. Mock server snapshot streaming behavior
	dummyFileContent := "hello world"
	hasher := sha256.New()
	hasher.Write([]byte(dummyFileContent))
	dummyFileChecksum := hex.EncodeToString(hasher.Sum(nil))

	mockServer.streamSnapshotFunc = func(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
		require.Equal(t, snapshotID, req.Id)

		// Send file info
		fileInfo := &pb.FileInfo{Path: "dummy.txt", Checksum: dummyFileChecksum}
		err := stream.Send(&pb.SnapshotChunk{Content: &pb.SnapshotChunk_FileInfo{FileInfo: fileInfo}})
		require.NoError(t, err)

		// Send file content
		chunk := &pb.SnapshotChunk{Content: &pb.SnapshotChunk_ChunkData{ChunkData: []byte(dummyFileContent)}}
		err = stream.Send(chunk)
		require.NoError(t, err)

		return nil // End of stream
	}

	// 4. Mock WAL streaming after bootstrap
	mockServer.entries = []*pb.WALEntry{newTestWALEntry(101, "metric.after.snapshot", 1000)}

	// --- Act ---
	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)

	// --- Assert ---
	// Wait for the post-snapshot entry to be applied
	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(101), applied.GetSequenceNumber())
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for post-snapshot entry")
	}

	cancel()
	mockEngine.AssertExpectations(t)
}

func TestStart_DialFailure(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mockEngine := newMockReplicatedEngine(nil)

	// Use an invalid, non-routable address to ensure Dial fails.
	applier := NewWALApplier("127.0.0.1:9999", mockEngine, nil, t.TempDir(), logger)

	// Use WithBlock to make the dial synchronous and respect the context timeout.
	applier.dialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}

	// Use a short timeout for the dial context to make the test run faster.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start should now block, fail due to the timeout, and return.
	applier.Start(ctx)

	// Check that the applier did not connect and does not have a client.
	assert.Nil(t, applier.conn, "Connection should be nil")
	assert.Nil(t, applier.client, "Client should be nil")
}

func TestBootstrap_NoSnapshotNeeded(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	leaderSnapSeqNum := uint64(100)

	// Mock engine to report it's already up-to-date.
	mockEngine.On("GetLatestAppliedSeqNum").Return(leaderSnapSeqNum)
	// Mock server to have a snapshot that is not newer than the engine's state.
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{Id: "snapshot-1", LastWalSequenceNumber: leaderSnapSeqNum}, nil
	}

	// Mock WAL streaming to send one entry after the bootstrap check.
	mockServer.entries = []*pb.WALEntry{newTestWALEntry(101, "metric.nostart", 1000)}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	// We expect to receive the WAL entry, proving that snapshotting was skipped.
	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(101), applied.GetSequenceNumber())
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for post-bootstrap entry")
	}

	mockEngine.AssertExpectations(t)
	// We also want to ensure that Close and ReplaceWithSnapshot were never called.
	mockEngine.AssertNotCalled(t, "Close")
	mockEngine.AssertNotCalled(t, "ReplaceWithSnapshot", mock.AnythingOfType("string"))
}

func TestBootstrap_GetSnapshotInfoError(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	expectedErr := status.Error(codes.Internal, "db is on fire")
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return nil, expectedErr
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	// The applier should stop because it can't bootstrap.
	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after failing to get snapshot info")

	// No entries should have been processed.
	assert.Zero(t, mockEngine.GetLatestAppliedSeqNum())
	mockEngine.AssertNotCalled(t, "ApplyReplicatedEntry", mock.Anything, mock.Anything)
}

func TestBootstrap_DownloadFailsOnStreamError(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// 1. Mock engine state (needs a snapshot)
	mockEngine.On("GetLatestAppliedSeqNum").Return(uint64(10)).Once()

	// 2. Mock server snapshot info
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{Id: "snap-1", LastWalSequenceNumber: 100}, nil
	}

	// 3. Mock server to fail during the snapshot stream
	expectedErr := status.Error(codes.Internal, "disk read error on leader")
	mockServer.streamSnapshotFunc = func(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
		// Send one chunk successfully, then fail.
		fileInfo := &pb.FileInfo{Path: "dummy.txt", Checksum: "abc"}
		err := stream.Send(&pb.SnapshotChunk{Content: &pb.SnapshotChunk_FileInfo{FileInfo: fileInfo}})
		require.NoError(t, err)
		return expectedErr
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	// The applier should stop because it can't bootstrap.
	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after snapshot stream error")

	// Ensure we didn't try to restore a partial snapshot
	mockEngine.AssertNotCalled(t, "Close")
	mockEngine.AssertNotCalled(t, "ReplaceWithSnapshot", mock.Anything)
}

func TestBootstrap_DownloadFailsOnChecksumMismatch(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// 1. Mock engine state (needs a snapshot)
	mockEngine.On("GetLatestAppliedSeqNum").Return(uint64(10)).Once()

	// 2. Mock server snapshot info
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{Id: "snap-checksum-mismatch", LastWalSequenceNumber: 100}, nil
	}

	// 3. Mock server to stream a file with a bad checksum
	mockServer.streamSnapshotFunc = func(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
		fileInfo := &pb.FileInfo{Path: "dummy.txt", Checksum: "this-is-a-bad-checksum"}
		err := stream.Send(&pb.SnapshotChunk{Content: &pb.SnapshotChunk_FileInfo{FileInfo: fileInfo}})
		require.NoError(t, err)

		chunk := &pb.SnapshotChunk{Content: &pb.SnapshotChunk_ChunkData{ChunkData: []byte("some data")}}
		err = stream.Send(chunk)
		require.NoError(t, err)

		return nil // End of stream
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	// The applier should stop because it can't bootstrap.
	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after checksum mismatch")

	// Ensure we didn't try to restore a partial/corrupt snapshot
	mockEngine.AssertNotCalled(t, "Close")
	mockEngine.AssertNotCalled(t, "ReplaceWithSnapshot", mock.Anything)
}

func TestDownloadAndRestoreSnapshot_ChunkDataBeforeFileInfo(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockEngine.On("GetLatestAppliedSeqNum").Return(uint64(10)).Once()
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{Id: "snap-chunk-first", LastWalSequenceNumber: 100}, nil
	}

	// Mock server to send a data chunk before a file info chunk.
	mockServer.streamSnapshotFunc = func(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
		chunk := &pb.SnapshotChunk{Content: &pb.SnapshotChunk_ChunkData{ChunkData: []byte("some data")}}
		return stream.Send(chunk)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after receiving chunk data first")
	mockEngine.AssertNotCalled(t, "Close")
	mockEngine.AssertNotCalled(t, "ReplaceWithSnapshot", mock.Anything)
}

func TestDownloadAndRestoreSnapshot_ReplaceFails(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockEngine.On("GetLatestAppliedSeqNum").Return(uint64(10)).Once()
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return &pb.SnapshotInfo{Id: "snap-replace-fails", LastWalSequenceNumber: 100}, nil
	}

	// Mock a successful download
	mockServer.streamSnapshotFunc = func(req *pb.StreamSnapshotRequest, stream pb.ReplicationService_StreamSnapshotServer) error {
		return nil // Empty but successful stream
	}

	// Mock the engine to fail on Close and Replace
	mockEngine.On("Close").Return(nil).Once()
	mockEngine.On("ReplaceWithSnapshot", mock.AnythingOfType("string")).Return(errors.New("disk is full")).Once()

	ctx, cancel := context.WithCancel(context.Background())
	go applier.Start(ctx)
	defer cancel()

	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after replace fails")

	mockEngine.AssertExpectations(t)
}

func TestReplicationLoop_ContextCancellation(t *testing.T) {
	applier, _, mockServer, cleanup := setupTest(t)
	defer cleanup()

	// Bootstrap is successful, no snapshot needed.
	mockServer.getSnapshotInfoFunc = func() (*pb.SnapshotInfo, error) {
		return nil, status.Error(codes.NotFound, "no snapshot")
	}

	ctx, cancel := context.WithCancel(context.Background())

	go applier.Start(ctx)

	// Wait for the replication loop to start and request a stream.
	select {
	case <-mockServer.streamReqCh:
		// Great, it's running. Now cancel the context.
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for replication loop to start")
	}

	// The applier should eventually shut down its connection.
	assert.Eventually(t, func() bool {
		return applier.conn.GetState() == connectivity.Shutdown
	}, 2*time.Second, 50*time.Millisecond, "applier should shut down after context cancellation")
}
