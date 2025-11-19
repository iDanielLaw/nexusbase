package server

import (
	"bufio"
	"bytes"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	api "github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal/testutil"
	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testInMemServer holds the server and mock engine for a test that uses InMemoryListener.
type testInMemServer struct {
	appServer     *AppServer
	mockEngine    *MockStorageEngine
	serverErrChan chan error
	lis           *testutil.InMemoryListener
}

func (s *testInMemServer) close(t *testing.T) {
	s.appServer.Stop()
	if err, open := <-s.serverErrChan; open {
		require.NoError(t, err)
	}
	s.mockEngine.Close()
	s.mockEngine.AssertExpectations(t)
}

func setupInMemoryServerTest(t *testing.T) *testInMemServer {
	t.Helper()
	mockEngine := new(MockStorageEngine)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: 0,
			TCPPort:  findFreePort(t),
		},
	}

	mockEngine.On("Close").Return(nil).Once()

	lis := testutil.NewInMemoryListener()
	appServer, err := NewAppServerWithListeners(mockEngine, cfg, testLogger, nil, lis)
	require.NoError(t, err)

	serverErrChan := make(chan error, 1)
	go func() {
		if err := appServer.Start(); err != nil {
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	// Wait for the TCP server accept loop to start instead of sleeping.
	// Check the internal `isStarted` flag under the tcpServer mutex to avoid races.
	ready := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if appServer.tcpServer != nil {
			appServer.tcpServer.mu.Lock()
			started := appServer.tcpServer.isStarted
			appServer.tcpServer.mu.Unlock()
			if started {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		// Fallback: if the server didn't report started, fail the setup early.
		require.True(t, ready, "TCP server did not start within timeout")
	}

	return &testInMemServer{
		appServer:     appServer,
		mockEngine:    mockEngine,
		serverErrChan: serverErrChan,
		lis:           lis,
	}
}

// helper to create nbql client from a net.Conn produced by InMemoryListener
func newNBQLClientFromConn(t *testing.T, conn net.Conn) *nbqlTestClient {
	t.Helper()
	return &nbqlTestClient{conn: conn, reader: bufio.NewReader(conn), t: t}
}

func TestInMemoryServer_Push_LegacyFastPath(t *testing.T) {
	server := setupInMemoryServerTest(t)
	defer server.close(t)

	server.mockEngine.On("Put", mock.Anything, mock.MatchedBy(func(dp core.DataPoint) bool {
		if dp.Metric != "cpu.fast" {
			return false
		}
		val, ok := dp.Fields["value"]
		if !ok {
			return false
		}
		f, _ := val.ValueFloat64()
		return ok && f == 50.0
	})).Return(nil).Once()

	// Dial the in-memory listener to get a client conn
	conn, err := server.lis.Dial()
	require.NoError(t, err)
	client := newNBQLClientFromConn(t, conn)
	defer client.close()

	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.0})
	require.NoError(t, err)
	pushReq := api.PushRequest{Metric: "cpu.fast", Fields: fields}
	payload := new(bytes.Buffer)
	err = api.EncodePushRequest(payload, pushReq)
	assert.NoError(t, err)
	client.sendFrame(api.CommandPush, payload.Bytes())

	resp := client.receiveManipulateResponse()
	assert.Equal(t, uint64(1), resp.RowsAffected)
}

func TestInMemoryServer_Query_RawData_WithResults(t *testing.T) {
	server := setupInMemoryServerTest(t)
	defer server.close(t)

	fields1, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "error", "status": int64(500)})
	require.NoError(t, err)
	fields2, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "info", "status": int64(200)})
	require.NoError(t, err)

	points := []*core.QueryResultItem{
		{Metric: "system.logs", Tags: map[string]string{"host": "A"}, Timestamp: 1, Fields: fields1},
		{Metric: "system.logs", Tags: map[string]string{"host": "A"}, Timestamp: 2, Fields: fields2},
	}
	mockIterator := NewMockQueryResultIterator(points, nil)

	server.mockEngine.On("Query", mock.Anything, mock.MatchedBy(func(params core.QueryParams) bool {
		return params.Metric == "system.logs" && params.StartTime == 0 && params.EndTime == 1000
	})).Return(mockIterator, nil).Once()

	conn, err := server.lis.Dial()
	require.NoError(t, err)
	client := newNBQLClientFromConn(t, conn)
	defer client.close()

	command := `QUERY system.logs FROM 0 TO 1000;`
	payload := new(bytes.Buffer)
	err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
	assert.NoError(t, err)
	client.sendFrame(api.CommandQuery, payload.Bytes())

	for i := 0; i < 2; i++ {
		cmdType, payloadResp, err := api.ReadFrame(client.reader)
		assert.NoError(t, err)
		assert.Equal(t, api.CommandQueryResultPart, cmdType)
		resp, err := api.DecodeQueryResponse(bytes.NewReader(payloadResp))
		assert.NoError(t, err)
		assert.Len(t, resp.Results, 1)
		assert.Equal(t, points[i].Timestamp, resp.Results[0].Timestamp)
		assert.Equal(t, points[i].Fields, resp.Results[0].Fields)
	}

	cmdType, payloadResp, err := api.ReadFrame(client.reader)
	assert.NoError(t, err)
	assert.Equal(t, api.CommandQueryEnd, cmdType)
	endResp, err := api.DecodeQueryEndResponse(bytes.NewReader(payloadResp))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), endResp.TotalRows)
}

func TestInMemoryListener_CloseDrainsPendingConns(t *testing.T) {
	lis := testutil.NewInMemoryListener()

	// Dial once to enqueue a server-side connection into lis.conns and get the client conn.
	clientConn, err := lis.Dial()
	require.NoError(t, err)

	// Close the listener; Close should drain pending conns and close them.
	require.NoError(t, lis.Close())

	// Client side of the pipe should be closed by the drain logic.
	// A Read should return an error (EOF or other) when the peer is closed.
	buf := make([]byte, 1)
	clientConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = clientConn.Read(buf)
	require.Error(t, err)
	_ = clientConn.Close()
}

func TestInMemoryListener_AcceptAfterCloseReturnsErr(t *testing.T) {
	lis := testutil.NewInMemoryListener()
	require.NoError(t, lis.Close())

	// Accept should return net.ErrClosed when the listener is closed.
	_, err := lis.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
}

// Test executor timestamp behavior using mock clock (doesn't require network).
func TestExecutor_executePush_AutoTimestamp_InMemory(t *testing.T) {
	mockEngine := new(MockStorageEngine)
	fixedTime := time.Date(2025, time.July, 13, 10, 0, 0, 0, time.UTC)
	mockClock := clock.NewMockClock(fixedTime)

	executor := api.NewExecutor(mockEngine, mockClock)

	pushCmd := &corenbql.PushStatement{
		Metric:    "cpu.usage",
		Fields:    map[string]interface{}{"value": 50.5},
		Tags:      map[string]string{"host": "server1"},
		Timestamp: 0,
	}

	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.5})
	require.NoError(t, err)

	expectedDP := core.DataPoint{
		Metric:    "cpu.usage",
		Timestamp: fixedTime.UnixNano(),
		Fields:    fields,
		Tags:      map[string]string{"host": "server1"},
	}

	mockEngine.On("Put", mock.Anything, expectedDP).Return(nil).Once()

	resp, err := executor.Execute(nil, pushCmd)
	res := resp.(api.ManipulateResponse)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), res.RowsAffected)
	mockEngine.AssertExpectations(t)
}
