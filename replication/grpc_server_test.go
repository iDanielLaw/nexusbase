package replication

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// MockWAL เป็น mock object สำหรับ wal.WALInterface
type MockWAL struct {
	mock.Mock
}

func (m *MockWAL) NewStreamReader(fromSeqNum uint64) (wal.StreamReader, error) {
	args := m.Called(fromSeqNum)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(wal.StreamReader), args.Error(1)
}

// Implement the rest of wal.WALInterface with mock calls
func (m *MockWAL) AppendBatch(entries []core.WALEntry) error {
	args := m.Called(entries)
	return args.Error(0)
}
func (m *MockWAL) Append(entry core.WALEntry) error {
	args := m.Called(entry)
	return args.Error(0)
}
func (m *MockWAL) Sync() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockWAL) Purge(upToIndex uint64) error {
	args := m.Called(upToIndex)
	return args.Error(0)
}
func (m *MockWAL) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockWAL) Path() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockWAL) SetTestingOnlyInjectCloseError(err error) {
	m.Called(err)
}
func (m *MockWAL) ActiveSegmentIndex() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}
func (m *MockWAL) Rotate() error {
	args := m.Called()
	return args.Error(0)
}

// MockStreamReader เป็น mock object สำหรับ wal.StreamReader
type MockStreamReader struct {
	mock.Mock
}

func (m *MockStreamReader) Next() (*core.WALEntry, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.WALEntry), args.Error(1)
}

func (m *MockStreamReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockReplicationService_StreamWALServer เป็น mock object สำหรับ gRPC stream
type MockStreamServer struct { // This mock implements pb.ReplicationService_StreamWALServer
	mock.Mock
	grpc.ServerStream
	SentEntries chan *pb.WALEntry
	ctx         context.Context
}

func (m *MockStreamServer) Send(entry *pb.WALEntry) error {
	args := m.Called(entry)
	if args.Error(0) == nil {
		m.SentEntries <- entry
	}
	return args.Error(0)
}
func (m *MockStreamServer) Context() context.Context { return m.ctx }

func TestStreamWAL_Success(t *testing.T) {
	// --- Setup ---
	mockWal := new(MockWAL)             // This is now the updated mock
	mockReader := new(MockStreamReader) // This is also updated
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	server := NewServer(mockWal, logger)

	// สร้าง mock stream server
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mockStream := &MockStreamServer{
		SentEntries: make(chan *pb.WALEntry, 5),
		ctx:         ctx,
	}

	// กำหนดพฤติกรรมของ mock
	mockWal.On("NewStreamReader", uint64(100)).Return(mockReader, nil)
	mockReader.On("Next").Return(&core.WALEntry{SeqNum: 101, Key: []byte("key1")}, nil).Once()
	mockReader.On("Next").Return(&core.WALEntry{SeqNum: 102, Key: []byte("key2")}, nil).Once()
	mockReader.On("Next").Return(nil, io.EOF).Once() // สิ้นสุด stream
	mockReader.On("Close").Return(nil)
	mockStream.On("Send", mock.AnythingOfType("*proto.WALEntry")).Return(nil).Times(2)

	// --- Act ---
	err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 100}, mockStream)

	// --- Assert ---
	assert.NoError(t, err)
	close(mockStream.SentEntries)

	var receivedEntries []*pb.WALEntry
	for entry := range mockStream.SentEntries {
		receivedEntries = append(receivedEntries, entry)
	}

	assert.Len(t, receivedEntries, 2)
	assert.Equal(t, uint64(101), receivedEntries[0].GetSequenceNumber())
	assert.Equal(t, uint64(102), receivedEntries[1].GetSequenceNumber())

	mockWal.AssertExpectations(t)
	mockReader.AssertExpectations(t)
	mockStream.AssertExpectations(t)
}
