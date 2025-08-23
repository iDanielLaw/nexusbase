package replication

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// MockWAL is a mock object for wal.WALInterface
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

// MockStreamReader is a mock object for wal.StreamReader
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

// MockStreamServer is a mock object for gRPC stream
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
	tempDir := t.TempDir() // Create a temporary directory for the test
	defer os.RemoveAll(tempDir)

	mockWal := new(MockWAL)
	mockReader := new(MockStreamReader)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil)) // Discard logs for clean test output
	hookManager := hooks.NewHookManager(nil)

	// Correctly initialize StringStore by loading it from a file (even if it's new)
	stringStore := indexer.NewStringStore(logger, hookManager)
	err := stringStore.LoadFromFile(tempDir)
	require.NoError(t, err, "Failed to load string store from temp directory")
	defer stringStore.Close()

	// Create a real server instance with the now correctly initialized dependency
	server := NewServer(mockWal, stringStore, logger)

	// --- Test Data Setup ---
	metric := "cpu.usage"
	tags := map[string]string{"host": "server1", "region": "us-east"}
	timestamp1 := time.Now().UnixNano()
	fields1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.5})

	// Manually encode the data just like the engine would.
	metricID, _ := stringStore.GetOrCreateID(metric)
	encodedTags := make([]core.EncodedSeriesTagPair, 0, len(tags))
	for k, v := range tags {
		kID, _ := stringStore.GetOrCreateID(k)
		vID, _ := stringStore.GetOrCreateID(v)
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: kID, ValueID: vID})
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})

	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, timestamp1)
	encodedKey1 := make([]byte, keyBuf.Len())
	copy(encodedKey1, keyBuf.Bytes())

	valBuf, _ := fields1.Encode()
	valCopy := make([]byte, len(valBuf))
	copy(valCopy, valBuf)

	walEntry1 := &core.WALEntry{
		SeqNum:    101,
		EntryType: core.EntryTypePutEvent,
		Key:       encodedKey1,
		Value:     valCopy,
	}

	// --- Mock Behavior ---
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mockStream := &MockStreamServer{
		SentEntries: make(chan *pb.WALEntry, 5),
		ctx:         ctx,
	}

	mockWal.On("NewStreamReader", uint64(100)).Return(mockReader, nil)
	mockReader.On("Next").Return(walEntry1, nil).Once()
	mockReader.On("Next").Return(nil, io.EOF).Once()
	mockReader.On("Close").Return(nil)
	mockStream.On("Send", mock.Anything).Return(nil).Once()

	// --- Act ---
	err = server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 100}, mockStream)

	// --- Assert ---
	require.NoError(t, err)
	close(mockStream.SentEntries)

	var receivedEntries []*pb.WALEntry
	for entry := range mockStream.SentEntries {
		receivedEntries = append(receivedEntries, entry)
	}

	require.Len(t, receivedEntries, 1, "No entries were sent on the stream")
	received := receivedEntries[0]

	assert.Equal(t, uint64(101), received.GetSequenceNumber())
	assert.Equal(t, pb.WALEntry_PUT_EVENT, received.GetEntryType())
	assert.Equal(t, metric, received.GetMetric())
	assert.Equal(t, tags, received.GetTags())
	assert.Equal(t, timestamp1, received.GetTimestamp())

	// Assert fields
	protoFields := received.GetFields()
	require.NotNil(t, protoFields)
	valueField, ok := protoFields.Fields["value"]
	require.True(t, ok)
	assert.Equal(t, 50.5, valueField.GetNumberValue())

	mockWal.AssertExpectations(t)
	mockReader.AssertExpectations(t)
	mockStream.AssertExpectations(t)
}

func TestConvertWALEntryToProto(t *testing.T) {
	// --- Setup ---
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hookManager := hooks.NewHookManager(nil)
	stringStore := indexer.NewStringStore(logger, hookManager)
	err := stringStore.LoadFromFile(tempDir)
	require.NoError(t, err)
	defer stringStore.Close()

	// We don't need a mock WAL for this test, just the server with the string store
	server := NewServer(nil, stringStore, logger)

	// --- Test Data ---
	metric := "system.disk.usage"
	tags := map[string]string{"device": "/dev/sda1", "fstype": "ext4"}
	metricID, _ := stringStore.GetOrCreateID(metric)
	encodedTags := make([]core.EncodedSeriesTagPair, 0, len(tags))
	for k, v := range tags {
		kID, _ := stringStore.GetOrCreateID(k)
		vID, _ := stringStore.GetOrCreateID(v)
		encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: kID, ValueID: vID})
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})

	seriesKeyBuf := core.GetBuffer()
	core.EncodeSeriesKeyToBuffer(seriesKeyBuf, metricID, encodedTags)
	seriesKeyBytes := make([]byte, seriesKeyBuf.Len())
	copy(seriesKeyBytes, seriesKeyBuf.Bytes())
	core.PutBuffer(seriesKeyBuf)

	// --- Test Cases ---
	t.Run("DELETE_SERIES", func(t *testing.T) {
		walEntry := &core.WALEntry{
			SeqNum:    201,
			EntryType: core.EntryTypeDeleteSeries,
			Key:       seriesKeyBytes, // For DeleteSeries, the key is the series key
			Value:     nil,
		}

		protoEntry, err := server.convertWALEntryToProto(walEntry)
		require.NoError(t, err)
		require.NotNil(t, protoEntry)

		assert.Equal(t, uint64(201), protoEntry.GetSequenceNumber())
		assert.Equal(t, pb.WALEntry_DELETE_SERIES, protoEntry.GetEntryType())
		assert.Equal(t, metric, protoEntry.GetMetric())
		assert.Equal(t, tags, protoEntry.GetTags())
		// These fields should be zero/nil for this type
		assert.Zero(t, protoEntry.GetTimestamp())
		assert.Nil(t, protoEntry.GetFields())
		assert.Zero(t, protoEntry.GetStartTime())
		assert.Zero(t, protoEntry.GetEndTime())
	})

	t.Run("DELETE_RANGE", func(t *testing.T) {
		startTime := time.Now().Add(-1 * time.Hour).UnixNano()
		endTime := time.Now().UnixNano()
		encodedValue := core.EncodeRangeTombstoneValue(startTime, endTime)

		walEntry := &core.WALEntry{
			SeqNum:    202,
			EntryType: core.EntryTypeDeleteRange,
			Key:       seriesKeyBytes, // For DeleteRange, the key is the series key
			Value:     encodedValue,
		}

		protoEntry, err := server.convertWALEntryToProto(walEntry)
		require.NoError(t, err)
		require.NotNil(t, protoEntry)

		assert.Equal(t, uint64(202), protoEntry.GetSequenceNumber())
		assert.Equal(t, pb.WALEntry_DELETE_RANGE, protoEntry.GetEntryType())
		assert.Equal(t, metric, protoEntry.GetMetric())
		assert.Equal(t, tags, protoEntry.GetTags())
		assert.Equal(t, startTime, protoEntry.GetStartTime())
		assert.Equal(t, endTime, protoEntry.GetEndTime())
		// These fields should be zero/nil for this type
		assert.Zero(t, protoEntry.GetTimestamp())
		assert.Nil(t, protoEntry.GetFields())
	})

	t.Run("Unsupported Type", func(t *testing.T) {
		walEntry := &core.WALEntry{
			SeqNum:    203,
			EntryType: core.EntryTypeDelete, // 'D' is not a replicable type
		}

		_, err := server.convertWALEntryToProto(walEntry)
		assert.Error(t, err)
	})
}