package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	apiv1 "github.com/INLOpen/nexusbase/api/v1"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexusbase/wal"
)

// mockStreamWALServer is a mock implementation of the gRPC stream server interface.
type mockStreamWALServer struct {
	grpc.ServerStream
	ctx         context.Context
	sentEntries chan *apiv1.WALEntry
}

func (m *mockStreamWALServer) Context() context.Context {
	return m.ctx
}

func (m *mockStreamWALServer) Send(entry *apiv1.WALEntry) error {
	select {
	case m.sentEntries <- entry:
		return nil
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// mockWALReader is a mock implementation of the WALReader interface.
type mockWALReader struct {
	mock.Mock
}

func (m *mockWALReader) Next(ctx context.Context) (*core.WALEntry, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.WALEntry), args.Error(1)
}

func (m *mockWALReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

// mockWAL is a mock implementation of the WALInterface.
type mockWAL struct {
	mock.Mock
}

func (m *mockWAL) OpenReader(fromSeqNum uint64) (wal.WALReader, error) {
	args := m.Called(fromSeqNum)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(wal.WALReader), args.Error(1)
}

// Implement other WALInterface methods as no-ops or with mock calls if needed.
func (m *mockWAL) AppendBatch(entries []core.WALEntry) error { return nil }
func (m *mockWAL) Append(entry core.WALEntry) error          { return nil }
func (m *mockWAL) Sync() error                               { return nil }
func (m *mockWAL) Purge(upToIndex uint64) error              { return nil }
func (m *mockWAL) Close() error                              { return nil }
func (m *mockWAL) Path() string                              { return "" }
func (m *mockWAL) SetTestingOnlyInjectCloseError(err error)  {}
func (m *mockWAL) ActiveSegmentIndex() uint64                { return 0 }
func (m *mockWAL) Rotate() error                             { return nil }

// mockEngineWithWAL is a mock implementation of the engine and walProvider interfaces.
type mockEngineWithWAL struct {
	engine.MockStorageEngine
	mockWAL wal.WALInterface
}

func (m *mockEngineWithWAL) GetWAL() wal.WALInterface {
	return m.mockWAL
}

func TestReplicationGRPCServer_StreamWAL_ContextCancel(t *testing.T) {
	// 1. Setup
	mockReader := new(mockWALReader)
	mockWal := new(mockWAL)
	mockEngine := &mockEngineWithWAL{mockWAL: mockWal}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	server := &ReplicationGRPCServer{
		engine: mockEngine,
		logger: logger,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockServerStream := &mockStreamWALServer{
		ctx:         ctx,
		sentEntries: make(chan *apiv1.WALEntry, 1),
	}

	// 2. Configure mock behavior
	firstEntry := &core.WALEntry{SeqNum: 100, EntryType: core.EntryTypePutEvent, Key: []byte("testkey"), Value: []byte("testvalue")}

	// First call to Next() returns a valid entry.
	mockReader.On("Next", mock.Anything).Return(firstEntry, nil).Once()
	// Second call to Next() will block and then return context.Canceled when the context is cancelled.
	mockReader.On("Next", mock.Anything).Return(nil, context.Canceled).Run(func(args mock.Arguments) {
		// This simulates the real wal.Reader blocking by waiting on the context.
		<-args.Get(0).(context.Context).Done()
	}).Once()

	mockReader.On("Close").Return(nil).Once()
	mockWal.On("OpenReader", uint64(99)).Return(mockReader, nil).Once()

	// 3. Run the server method in a goroutine
	errChan := make(chan error, 1)
	go func() {
		req := &apiv1.StreamWALRequest{FromSequenceNumber: 99}
		errChan <- server.StreamWAL(req, mockServerStream)
	}()

	// 4. Verification
	// Wait for the first entry to be sent.
	select {
	case sent := <-mockServerStream.sentEntries:
		assert.Equal(t, uint64(100), sent.GetSequenceNumber(), "First entry should be sent successfully")
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for the first WAL entry to be sent")
	}

	// Cancel the context while the reader's Next() is blocking.
	t.Log("Cancelling context to unblock the stream...")
	cancel()

	// The StreamWAL goroutine should now exit cleanly.
	select {
	case err := <-errChan:
		assert.NoError(t, err, "StreamWAL should return a nil error on context cancellation")
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for StreamWAL to exit after context cancellation")
	}

	mockReader.AssertExpectations(t)
	mockWal.AssertExpectations(t)
}

