package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	if args.Error(0) == nil && m.SentEntries != nil {
		m.SentEntries <- entry
	}
	return args.Error(0)
}
func (m *MockStreamServer) Context() context.Context { return m.ctx }

// MockSnapshotManager is a mock for snapshot.ManagerInterface
type MockSnapshotManager struct {
	mock.Mock
}

func (m *MockSnapshotManager) CreateFull(ctx context.Context, snapshotDir string) error {
	args := m.Called(ctx, snapshotDir)
	return args.Error(0)
}

func (m *MockSnapshotManager) CreateIncremental(ctx context.Context, snapshotsBaseDir string) error {
	args := m.Called(ctx, snapshotsBaseDir)
	return args.Error(0)
}

func (m *MockSnapshotManager) ListSnapshots(snapshotsBaseDir string) ([]snapshot.Info, error) {
	args := m.Called(snapshotsBaseDir)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]snapshot.Info), args.Error(1)
}

func (m *MockSnapshotManager) Validate(snapshotDir string) error {
	args := m.Called(snapshotDir)
	return args.Error(0)
}

func (m *MockSnapshotManager) Prune(ctx context.Context, snapshotsBaseDir string, opts snapshot.PruneOptions) ([]string, error) {
	args := m.Called(ctx, snapshotsBaseDir, opts)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockSnapshotManager) RestoreFrom(ctx context.Context, snapshotPath string) error {
	args := m.Called(ctx, snapshotPath)
	return args.Error(0)
}

// MockSnapshotStreamServer is a mock for the snapshot streaming server
type MockSnapshotStreamServer struct {
	grpc.ServerStream
	SentChunks chan *pb.SnapshotChunk
	ctx        context.Context
}

func (m *MockSnapshotStreamServer) Send(chunk *pb.SnapshotChunk) error {
	if m.SentChunks != nil {
		m.SentChunks <- chunk
	}
	return nil
}

func (m *MockSnapshotStreamServer) Context() context.Context {
	return m.ctx
}

// setupServerTest is a helper to reduce boilerplate in server tests
func setupServerTest(t *testing.T) (*Server, *MockWAL, *indexer.StringStore) {
	t.Helper()
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	hookManager := hooks.NewHookManager(nil)
	stringStore := indexer.NewStringStore(logger, hookManager)
	err := stringStore.LoadFromFile(tempDir)
	require.NoError(t, err)

	t.Cleanup(func() {
		stringStore.Close()
		os.RemoveAll(tempDir)
	})

	mockWal := new(MockWAL)
	server := NewServer(mockWal, stringStore, nil, "", logger)
	return server, mockWal, stringStore
}

func TestGetLatestSnapshotInfo(t *testing.T) {
	server, _, _ := setupServerTest(t)
	mockSnapshotMgr := new(MockSnapshotManager)
	server.snapshotMgr = mockSnapshotMgr
	snapshotDir := t.TempDir()
	server.snapshotDir = snapshotDir

	t.Run("Success", func(t *testing.T) {
		// --- Setup ---
		now := time.Now()
		infos := []snapshot.Info{
			{ID: "snap-1", CreatedAt: now.Add(-1 * time.Hour)},
			{ID: "snap-3-latest", CreatedAt: now},
			{ID: "snap-2", CreatedAt: now.Add(-30 * time.Minute)},
		}
		mockSnapshotMgr.On("ListSnapshots", snapshotDir).Return(infos, nil).Once()

		// Create a dummy manifest file for the latest snapshot
		latestSnapPath := filepath.Join(snapshotDir, "snap-3-latest")
		require.NoError(t, os.MkdirAll(latestSnapPath, 0755))
		manifest := &core.SnapshotManifest{SequenceNumber: 12345}
		manifestFile, err := os.Create(filepath.Join(latestSnapPath, "MANIFEST"))
		require.NoError(t, err)
		err = snapshot.WriteManifestBinary(manifestFile, manifest)
		require.NoError(t, err)
		manifestFile.Close()

		// --- Act ---
		resp, err := server.GetLatestSnapshotInfo(context.Background(), &pb.GetLatestSnapshotInfoRequest{})

		// --- Assert ---
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "snap-3-latest", resp.Id)
		assert.Equal(t, uint64(12345), resp.LastWalSequenceNumber)
		mockSnapshotMgr.AssertExpectations(t)
	})

	t.Run("ListSnapshots fails", func(t *testing.T) {
		expectedErr := errors.New("disk on fire")
		mockSnapshotMgr.On("ListSnapshots", snapshotDir).Return(nil, expectedErr).Once()

		_, err := server.GetLatestSnapshotInfo(context.Background(), &pb.GetLatestSnapshotInfoRequest{})

		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), expectedErr.Error())
		mockSnapshotMgr.AssertExpectations(t)
	})

	t.Run("No snapshots found", func(t *testing.T) {
		mockSnapshotMgr.On("ListSnapshots", snapshotDir).Return([]snapshot.Info{}, nil).Once()

		_, err := server.GetLatestSnapshotInfo(context.Background(), &pb.GetLatestSnapshotInfoRequest{})

		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.NotFound, st.Code())
		mockSnapshotMgr.AssertExpectations(t)
	})

	t.Run("Manifest file missing", func(t *testing.T) {
		infos := []snapshot.Info{{ID: "snap-no-manifest", CreatedAt: time.Now()}}
		mockSnapshotMgr.On("ListSnapshots", snapshotDir).Return(infos, nil).Once()
		// Ensure the directory exists but the manifest does not
		require.NoError(t, os.MkdirAll(filepath.Join(snapshotDir, "snap-no-manifest"), 0755))

		_, err := server.GetLatestSnapshotInfo(context.Background(), &pb.GetLatestSnapshotInfoRequest{})

		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "failed to open snapshot manifest")
		mockSnapshotMgr.AssertExpectations(t)
	})

	t.Run("Manifest file corrupted", func(t *testing.T) {
		infos := []snapshot.Info{{ID: "snap-corrupt-manifest", CreatedAt: time.Now()}}
		mockSnapshotMgr.On("ListSnapshots", snapshotDir).Return(infos, nil).Once()
		corruptSnapPath := filepath.Join(snapshotDir, "snap-corrupt-manifest")
		require.NoError(t, os.MkdirAll(corruptSnapPath, 0755))
		// Write a bad manifest file
		require.NoError(t, os.WriteFile(filepath.Join(corruptSnapPath, "MANIFEST"), []byte("not a real manifest"), 0644))

		_, err := server.GetLatestSnapshotInfo(context.Background(), &pb.GetLatestSnapshotInfoRequest{})

		require.Error(t, err)
		st, _ := status.FromError(err)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), "failed to read snapshot manifest")
		mockSnapshotMgr.AssertExpectations(t)
	})
}

func TestStreamSnapshot(t *testing.T) {
	server, _, _ := setupServerTest(t)
	snapshotDir := t.TempDir()
	server.snapshotDir = snapshotDir

	// --- Setup a dummy snapshot directory ---
	snapID := "test-snap-123"
	snapPath := filepath.Join(snapshotDir, snapID)
	require.NoError(t, os.MkdirAll(snapPath, 0755))

	file1Content := "hello world"
	file1Path := filepath.Join(snapPath, "file1.txt")
	require.NoError(t, os.WriteFile(file1Path, []byte(file1Content), 0644))

	subdirPath := filepath.Join(snapPath, "subdir")
	require.NoError(t, os.MkdirAll(subdirPath, 0755))
	file2Content := "data in subdirectory"
	file2Path := filepath.Join(subdirPath, "file2.log")
	require.NoError(t, os.WriteFile(file2Path, []byte(file2Content), 0644))

	// --- Act ---
	mockStream := &MockSnapshotStreamServer{
		SentChunks: make(chan *pb.SnapshotChunk, 10),
		ctx:        context.Background(),
	}

	err := server.StreamSnapshot(&pb.StreamSnapshotRequest{Id: snapID}, mockStream)
	close(mockStream.SentChunks)

	// --- Assert ---
	require.NoError(t, err)

	// Collect all chunks
	receivedChunks := make([]*pb.SnapshotChunk, 0)
	for chunk := range mockStream.SentChunks {
		receivedChunks = append(receivedChunks, chunk)
	}

	require.GreaterOrEqual(t, len(receivedChunks), 2, "Should have received at least two file_info chunks")

	// Verify File 1
	file1Info, ok := receivedChunks[0].Content.(*pb.SnapshotChunk_FileInfo)
	require.True(t, ok)
	assert.Equal(t, "file1.txt", file1Info.FileInfo.Path)
	hasher1 := sha256.New()
	hasher1.Write([]byte(file1Content))
	assert.Equal(t, hex.EncodeToString(hasher1.Sum(nil)), file1Info.FileInfo.Checksum)

	file1Data, ok := receivedChunks[1].Content.(*pb.SnapshotChunk_ChunkData)
	require.True(t, ok)
	assert.Equal(t, file1Content, string(file1Data.ChunkData))

	// Verify File 2
	file2Info, ok := receivedChunks[2].Content.(*pb.SnapshotChunk_FileInfo)
	require.True(t, ok)
	assert.Equal(t, filepath.Join("subdir", "file2.log"), file2Info.FileInfo.Path)
	hasher2 := sha256.New()
	hasher2.Write([]byte(file2Content))
	assert.Equal(t, hex.EncodeToString(hasher2.Sum(nil)), file2Info.FileInfo.Checksum)

	file2Data, ok := receivedChunks[3].Content.(*pb.SnapshotChunk_ChunkData)
	require.True(t, ok)
	assert.Equal(t, file2Content, string(file2Data.ChunkData))
}

func TestStreamWAL_Success(t *testing.T) {
	// --- Setup ---
	server, mockWal, stringStore := setupServerTest(t)
	mockReader := new(MockStreamReader)

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
	mockReader.On("Next").Return(walEntry1, nil).Once().
		On("Next").Return(nil, wal.ErrNoNewEntries) // This will now be AnyTimes() by default for subsequent calls
	mockReader.On("Close").Return(nil)
	mockStream.On("Send", mock.Anything).Return(nil).Once()

	// --- Act ---
	err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 100}, mockStream)

	// --- Assert ---
	// Expect context.Canceled because we canceled it to stop the loop
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(mockStream.SentEntries)

	var receivedEntries []*pb.WALEntry
	for entry := range mockStream.SentEntries {
		receivedEntries = append(receivedEntries, entry)
	}

	require.Len(t, receivedEntries, 1, "Incorrect number of entries sent on the stream")
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

func TestStreamWAL_ErrorHandling(t *testing.T) {
	server, mockWal, _ := setupServerTest(t)
	mockReader := new(MockStreamReader)

	t.Run("NewStreamReader fails", func(t *testing.T) {
		expectedErr := errors.New("failed to create reader")
		mockWal.On("NewStreamReader", uint64(1)).Return(nil, expectedErr).Once()

		mockStream := &MockStreamServer{ctx: context.Background()}
		err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 1}, mockStream)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok, "Error should be a gRPC status error")
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), expectedErr.Error())
		mockWal.AssertExpectations(t)
	})

	t.Run("Reader Next fails", func(t *testing.T) {
		expectedErr := errors.New("failed to read from wal")
		mockWal.On("NewStreamReader", uint64(1)).Return(mockReader, nil).Once()
		mockReader.On("Next").Return(nil, expectedErr).Once()
		mockReader.On("Close").Return(nil).Once()

		mockStream := &MockStreamServer{ctx: context.Background()}
		err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 1}, mockStream)

		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		assert.Equal(t, codes.Internal, st.Code())
		assert.Contains(t, st.Message(), expectedErr.Error())
		mockReader.AssertExpectations(t)
	})

	t.Run("Stream Send fails", func(t *testing.T) {
		// Need a real entry for this test
		_, _, stringStore := setupServerTest(t)
		server.indexer = stringStore // Re-assign server with a fresh string store
		pointValue, _ := core.NewPointValue(10.0)
		walEntry, _ := createTestWALEntry(t, stringStore, 1, "metric", map[string]string{"host": "server1"}, core.FieldValues{"value": pointValue})

		expectedErr := errors.New("network error")
		mockWal.On("NewStreamReader", uint64(1)).Return(mockReader, nil).Once()
		mockReader.On("Next").Return(walEntry, nil).Once()
		mockReader.On("Close").Return(nil).Once()

		mockStream := &MockStreamServer{ctx: context.Background(), SentEntries: make(chan *pb.WALEntry, 1)}
		mockStream.On("Send", mock.Anything).Return(expectedErr).Once()

		err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 1}, mockStream)

		assert.ErrorIs(t, err, expectedErr)
		mockStream.AssertExpectations(t)
	})

	t.Run("Context is canceled", func(t *testing.T) {
		mockWal.On("NewStreamReader", uint64(1)).Return(mockReader, nil).Once()
		mockReader.On("Close").Return(nil).Once()

		ctx, cancel := context.WithCancel(context.Background())
		mockStream := &MockStreamServer{ctx: ctx}
		cancel() // Cancel the context immediately

		err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 1}, mockStream)

		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("Conversion error skips entry", func(t *testing.T) {
		_, _, stringStore := setupServerTest(t)
		server.indexer = stringStore

		// Entry 1: Malformed, will cause a conversion error
		malformedEntry := &core.WALEntry{SeqNum: 1, EntryType: core.EntryTypePutEvent, Key: []byte("bad-key")}
		// Entry 2: Valid
		pointValueValid, _ := core.NewPointValue(20.0)
		validEntry, _ := createTestWALEntry(t, stringStore, 2, "good.metric", map[string]string{"tag2": "value2"}, core.FieldValues{"value": pointValueValid})

		mockWal.On("NewStreamReader", uint64(1)).Return(mockReader, nil).Once()
		mockReader.On("Next").Return(malformedEntry, nil).Once()
		mockReader.On("Next").Return(validEntry, nil).Once()
		mockReader.On("Next").Return(nil, io.EOF).Once()
		mockReader.On("Close").Return(nil).Once()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		mockStream := &MockStreamServer{ctx: ctx, SentEntries: make(chan *pb.WALEntry, 1)}
		// We only expect one successful send
		mockStream.On("Send", mock.Anything).Return(nil).Once()

		// Act: The error from StreamWAL should be nil because it ends with a clean EOF
		err := server.StreamWAL(&pb.StreamWALRequest{FromSequenceNumber: 1}, mockStream)
		require.NoError(t, err)

		// Assert: Check that only the valid entry was sent
		close(mockStream.SentEntries)
		assert.Len(t, mockStream.SentEntries, 1, "Only the valid entry should have been sent")
		received := <-mockStream.SentEntries
		assert.Equal(t, uint64(2), received.GetSequenceNumber())
		mockStream.AssertExpectations(t)
	})
}

func TestConvertWALEntryToProto(t *testing.T) {
	// --- Setup ---
	server, _, stringStore := setupServerTest(t)

	// --- Test Data ---
	metric := "system.disk.usage"
	tags := map[string]string{"device": "/dev/sda1", "fstype": "ext4"}
	seriesKeyBytes, _ := createTestSeriesKey(t, stringStore, metric, tags)

	// --- Test Cases ---
	t.Run("PUT_EVENT", func(t *testing.T) {
		timestamp := time.Now().UnixNano()
		fieldsPtr, _ := core.NewFieldValuesFromMap(map[string]interface{}{"used_percent": 85.4, "free_bytes": 1234567890})
		walEntry, _ := createTestWALEntry(t, stringStore, 101, metric, tags, fieldsPtr, timestamp)

		protoEntry, err := server.convertWALEntryToProto(walEntry)
		require.NoError(t, err)
		require.NotNil(t, protoEntry)

		assert.Equal(t, uint64(101), protoEntry.GetSequenceNumber())
		assert.Equal(t, pb.WALEntry_PUT_EVENT, protoEntry.GetEntryType())
		assert.Equal(t, metric, protoEntry.GetMetric())
		assert.Equal(t, tags, protoEntry.GetTags())
		assert.Equal(t, timestamp, protoEntry.GetTimestamp())
		require.NotNil(t, protoEntry.GetFields())
		assert.Equal(t, 85.4, protoEntry.GetFields().Fields["used_percent"].GetNumberValue())
		assert.Equal(t, float64(1234567890), protoEntry.GetFields().Fields["free_bytes"].GetNumberValue())
	})

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

	t.Run("StringStore error", func(t *testing.T) {
		// Create a new server with a fresh string store that doesn't have the IDs
		server, _, _ := setupServerTest(t)
		walEntry, _ := createTestWALEntry(t, stringStore, 1, "metric", map[string]string{"a": "b"}, nil)

		// The new server's string store won't have the metric/tag IDs
		_, err := server.convertWALEntryToProto(walEntry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found in string store")
	})
}

// --- Test Helpers ---

// createTestSeriesKey is a helper to create an encoded series key
func createTestSeriesKey(t *testing.T, stringStore *indexer.StringStore, metric string, tags map[string]string) ([]byte, []core.EncodedSeriesTagPair) {
	t.Helper()
	metricID, err := stringStore.GetOrCreateID(metric)
	require.NoError(t, err)

	encodedTags := make([]core.EncodedSeriesTagPair, 0, len(tags))
	if tags != nil {
		for k, v := range tags {
			kID, _ := stringStore.GetOrCreateID(k)
			vID, _ := stringStore.GetOrCreateID(v)
			encodedTags = append(encodedTags, core.EncodedSeriesTagPair{KeyID: kID, ValueID: vID})
		}
	}
	sort.Slice(encodedTags, func(i, j int) bool {
		return encodedTags[i].KeyID < encodedTags[j].KeyID
	})

	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	core.EncodeSeriesKeyToBuffer(keyBuf, metricID, encodedTags)
	seriesKeyBytes := make([]byte, keyBuf.Len())
	copy(seriesKeyBytes, keyBuf.Bytes())
	return seriesKeyBytes, encodedTags
}

// createTestWALEntry is a helper to create a fully-formed WAL entry for testing
func createTestWALEntry(t *testing.T, stringStore *indexer.StringStore, seqNum uint64, metric string, tags map[string]string, fields core.FieldValues, timestamp ...int64) (*core.WALEntry, error) {
	t.Helper()
	_, encodedTags := createTestSeriesKey(t, stringStore, metric, tags)

	var ts int64
	if len(timestamp) > 0 {
		ts = timestamp[0]
	} else {
		ts = time.Now().UnixNano()
	}

	keyBuf := core.GetBuffer()
	defer core.PutBuffer(keyBuf)
	metricID, _ := stringStore.GetID(metric)
	core.EncodeTSDBKeyToBuffer(keyBuf, metricID, encodedTags, ts)
	encodedKey := make([]byte, keyBuf.Len())
	copy(encodedKey, keyBuf.Bytes())

	var encodedValue []byte
	var err error
	if fields != nil {
		encodedValue, err = fields.Encode()
		if err != nil {
			return nil, err
		}
	}

	return &core.WALEntry{
		SeqNum:    seqNum,
		EntryType: core.EntryTypePutEvent,
		Key:       encodedKey,
		Value:     encodedValue,
	}, nil
}
