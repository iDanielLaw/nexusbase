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
	mu        sync.Mutex
	stream    pb.ReplicationService_StreamWALServer
	streamErr error
	sendErr   error
	entries   []*pb.WALEntry
}

func (s *mockReplicationServer) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	s.mu.Lock()
	s.stream = stream
	from := req.GetFromSequenceNumber()
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

	<-stream.Context().Done()
	return stream.Context().Err()
}

// --- Test Helpers ---

const bufSize = 1024 * 1024

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

// setupTest initializes a mock server and a WALApplier connected to it.
func setupTest(t *testing.T) (*WALApplier, *mockReplicatedEngine, *mockReplicationServer, func()) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	appliedCh := make(chan *pb.WALEntry, 10)

	mockEngine := newMockReplicatedEngine(appliedCh)
	mockServer := &mockReplicationServer{}

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
	}

	return applier, mockEngine, mockServer, cleanup
}

// --- Test Cases ---

func TestWALApplier_StartStop(t *testing.T) {
	applier, _, _, cleanup := setupTest(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	go applier.replicationLoop(ctx)

	time.Sleep(10 * time.Millisecond)

	cancel()
	applier.Stop()
}

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
}

func TestWALApplier_ReconnectsAndResumes(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
	}
	mockServer.sendErr = io.EOF

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go applier.replicationLoop(ctx)

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}
	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum())

	mockServer.mu.Lock()
	mockServer.sendErr = nil
	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(2, "metric1", 2000),
	}
	mockServer.mu.Unlock()

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(2), applied.GetSequenceNumber())
	case <-time.After(6 * time.Second):
		t.Fatal("timed out waiting for the second entry after reconnect")
	}

	assert.Equal(t, uint64(2), mockEngine.GetLatestAppliedSeqNum())
}

func TestWALApplier_HandlesOutOfOrderEntry(t *testing.T) {
	applier, mockEngine, mockServer, cleanup := setupTest(t)
	defer cleanup()

	mockServer.entries = []*pb.WALEntry{
		newTestWALEntry(1, "metric1", 1000),
		newTestWALEntry(3, "metric1", 3000), // Out-of-order
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go applier.replicationLoop(ctx)

	select {
	case applied := <-mockEngine.appliedCh:
		assert.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

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

	// Program the mock engine to fail on the second entry
	mockEngine.applyFunc = func(entry *pb.WALEntry) error {
		mockEngine.mu.Lock()
		defer mockEngine.mu.Unlock()

		if entry.GetSequenceNumber() == 2 {
			return expectedErr
		}

		// Apply successfully
		mockEngine.appliedSeq = entry.GetSequenceNumber()
		if mockEngine.appliedCh != nil {
			mockEngine.appliedCh <- entry
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go applier.replicationLoop(ctx)

	// Wait for the first entry
	select {
	case applied := <-mockEngine.appliedCh:
		require.Equal(t, uint64(1), applied.GetSequenceNumber())
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for the first entry")
	}

	// Give the loop a moment to process the second (failing) entry
	time.Sleep(50 * time.Millisecond)

	// We should not have received entry 2.
	select {
	case applied := <-mockEngine.appliedCh:
		t.Fatalf("received unexpected entry %d after apply error", applied.GetSequenceNumber())
	case <-time.After(50 * time.Millisecond):
		// Good, no second entry was applied.
	}

	// The latest applied sequence number should still be 1.
	assert.Equal(t, uint64(1), mockEngine.GetLatestAppliedSeqNum())
}
