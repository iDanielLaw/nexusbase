package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"testing"
	"time"

	api "github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/utils"
	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testTCPServer holds the server and mock engine for a test.
type testTCPServer struct {
	appServer     *AppServer
	mockEngine    *MockStorageEngine
	serverErrChan chan error
}

// close stops the server and asserts mock expectations.
func (s *testTCPServer) close(t *testing.T) {
	s.appServer.Stop()
	// Wait for the server to stop and check for unexpected errors.
	if err, open := <-s.serverErrChan; open {
		// If the channel is still open and we received an error, fail the test.
		require.NoError(t, err, "Server exited with an unexpected error")
	}
	s.mockEngine.Close()
	s.mockEngine.AssertExpectations(t)
}

// setupTCPServerTest is a helper function to initialize a TCP server with a mock engine
// and return it, ready for testing.
func setupTCPServerTest(t *testing.T) *testTCPServer {
	t.Helper()

	mockEngine := new(MockStorageEngine)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	tcpPort := findFreePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort: 0, // Disable gRPC for this test
			TCPPort:  tcpPort,
		},
	}

	// Expect the Close call during cleanup
	mockEngine.On("Close").Return(nil).Once()

	appServer, err := NewAppServer(mockEngine, cfg, testLogger)
	require.NoError(t, err)

	serverErrChan := make(chan error, 1)
	go func() {
		// Start() is blocking, so it runs in a goroutine.
		// We expect a net.ErrClosed or nil error on graceful shutdown.
		if err := appServer.Start(); err != nil && !errors.Is(err, net.ErrClosed) {
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	// Wait for server to be ready
	var conn net.Conn
	for i := 0; i < 10; i++ {
		conn, err = net.Dial("tcp", appServer.tcpLis.Addr().String())
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err, "Failed to connect to test TCP server")

	return &testTCPServer{
		appServer:     appServer,
		mockEngine:    mockEngine,
		serverErrChan: serverErrChan,
	}
}

// nbqlTestClient is a helper for sending and receiving NBQL frames.
type nbqlTestClient struct {
	conn   net.Conn
	reader *bufio.Reader
	t      *testing.T
}

func newNBQLTestClient(t *testing.T, addr string) *nbqlTestClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	return &nbqlTestClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		t:      t,
	}
}

func (c *nbqlTestClient) close() {
	c.conn.Close()
}

func (c *nbqlTestClient) sendFrame(cmdType api.CommandType, payload []byte) {
	err := api.WriteFrame(c.conn, cmdType, payload)
	require.NoError(c.t, err)
}

func (c *nbqlTestClient) receiveManipulateResponse() api.ManipulateResponse {
	cmdType, payload, err := api.ReadFrame(c.reader)
	require.NoError(c.t, err)

	resp, err := api.DecodeManipulateResponse(bytes.NewReader(payload))
	require.Equal(c.t, api.CommandManipulate, cmdType)
	require.NoError(c.t, err)
	return resp
}

// TestTCPServer_PushEvent_QueryPath tests the full string parsing path for a PUSH command with the new structured event model.
// NOTE: This test assumes the NBQL parser has been updated to handle the `SET (key=value, ...)` syntax.
func TestTCPServer_PushEvent_QueryPath(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	expectedFields, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "info", "status": int64(200), "success": true})
	require.NoError(t, err)

	// Mock the engine call
	server.mockEngine.On("Put", mock.Anything, mock.MatchedBy(func(dp core.DataPoint) bool {
		assert.Equal(t, "system.logs", dp.Metric)
		assert.Equal(t, map[string]string{"app": "api", "dc": "us-east-1"}, dp.Tags)
		// We can't check timestamp as it's set by the server with NOW()
		return reflect.DeepEqual(expectedFields, dp.Fields)
	})).Return(nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	// Prepare and send command
	command := `PUSH system.logs TAGGED (app="api", dc="us-east-1") SET (level="info", status=200, success=TRUE);`
	payload := new(bytes.Buffer)
	err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
	assert.NoError(t, err)
	client.sendFrame(api.CommandQuery, payload.Bytes())

	// Receive and verify response
	resp := client.receiveManipulateResponse()
	assert.Equal(t, uint64(1), resp.RowsAffected)
}

// TestTCPServer_PushEvent_QueryPath_Corrected tests the same functionality as TestTCPServer_PushEvent_QueryPath
// but was failing due to an incorrect mock setup. This test is now corrected.
func TestTCPServer_PushEvent_QueryPath_Corrected(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	expectedFields, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "info", "status": int64(200), "success": true})
	require.NoError(t, err)

	// Mock the engine call
	// The original test was failing because it mocked "PutBatch" but the code path for a PUSH query calls "Put".
	server.mockEngine.On("Put", mock.Anything, mock.MatchedBy(func(dp core.DataPoint) bool {
		assert.Equal(t, "system.logs", dp.Metric)
		assert.Equal(t, map[string]string{"app": "api", "dc": "us-east-1"}, dp.Tags)
		return reflect.DeepEqual(expectedFields, dp.Fields)
	})).Return(nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	// Prepare and send command. This is a single PUSH command sent via the QUERY command type.
	command := `PUSH system.logs TAGGED (app="api", dc="us-east-1") SET (level="info", status=200, success=TRUE);`
	payload := new(bytes.Buffer)
	err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
	assert.NoError(t, err)
	client.sendFrame(api.CommandQuery, payload.Bytes())

	resp := client.receiveManipulateResponse()
	assert.Equal(t, uint64(1), resp.RowsAffected)
}

// TestTCPServer_Push_LegacyFastPath tests the binary fast-path for a legacy PUSH command.
func TestTCPServer_Push_LegacyFastPath(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	// Mock the engine call
	server.mockEngine.On("Put", mock.Anything, mock.MatchedBy(func(dp core.DataPoint) bool {
		assert.Equal(t, "cpu.fast", dp.Metric)
		assert.Empty(t, dp.Tags) // Legacy fast path has no tags
		// Check the single "value" field
		val, ok := dp.Fields["value"]
		if !ok {
			return false
		}
		floatVal, ok := val.ValueFloat64()
		return ok && floatVal == 50.0
	})).Return(nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	// Prepare and send command
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.0})
	require.NoError(t, err)
	pushReq := api.PushRequest{Metric: "cpu.fast", Fields: fields}
	payload := new(bytes.Buffer)
	err = api.EncodePushRequest(payload, pushReq)
	assert.NoError(t, err)
	client.sendFrame(api.CommandPush, payload.Bytes())

	// Receive and verify response
	resp := client.receiveManipulateResponse()
	assert.Equal(t, uint64(1), resp.RowsAffected)
}

// TestTCPServer_Pushs_LegacyFastPath tests the binary fast-path for a legacy PUSHS (batch) command.
func TestTCPServer_Pushs_LegacyFastPath(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	// Mock the engine call
	server.mockEngine.On("PutBatch", mock.Anything, mock.MatchedBy(func(points []core.DataPoint) bool {
		if len(points) != 2 {
			return false
		}
		// Check first point
		p1 := points[0]
		assert.Equal(t, "mem.batch", p1.Metric)
		val1, _ := p1.Fields["value"].ValueFloat64()
		assert.Equal(t, float64(1024), val1)
		// Check second point
		p2 := points[1]
		val2, ok := p2.Fields["value"].ValueFloat64()
		return p2.Metric == "mem.batch" && ok && val2 == 2048.0
	})).Return(nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	fields1, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 1024.0})
	require.NoError(t, err)
	fields2, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 2048.0})
	require.NoError(t, err)
	// Prepare and send command
	pushsReq := api.PushsRequest{
		Items: []api.PushItem{
			{Metric: "mem.batch", Fields: fields1},
			{Metric: "mem.batch", Fields: fields2},
		},
	}
	payload := new(bytes.Buffer)
	err = api.EncodePushsRequest(payload, pushsReq)
	assert.NoError(t, err)
	client.sendFrame(api.CommandPushs, payload.Bytes())

	// Receive and verify response
	resp := client.receiveManipulateResponse()
	assert.Equal(t, uint64(2), resp.RowsAffected)
}

// TestTCPServer_Query_RawData tests a raw data query.
func TestTCPServer_Query_RawData(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	// Mock the iterator
	mockIterator := NewMockQueryResultIterator(nil, nil) // No data for simplicity, Next() will return false.

	// Make the mock expectation more specific instead of using mock.Anything.
	server.mockEngine.On("Query", mock.Anything, mock.MatchedBy(func(params core.QueryParams) bool {
		return params.Metric == "cpu.usage" && params.StartTime == 0 && params.EndTime == 1000
	})).Return(mockIterator, nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	// Prepare and send command
	command := `QUERY cpu.usage FROM 0 TO 1000;`
	payload := new(bytes.Buffer)
	err := api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
	assert.NoError(t, err)
	client.sendFrame(api.CommandQuery, payload.Bytes())

	// Receive and verify response
	// We expect a QueryEnd frame immediately since there's no data.
	cmdType, payloadResp, err := api.ReadFrame(client.reader)
	assert.NoError(t, err)
	assert.Equal(t, api.CommandQueryEnd, cmdType)

	endResp, err := api.DecodeQueryEndResponse(bytes.NewReader(payloadResp))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), endResp.TotalRows)
}

// TestTCPServer_Query_RawData_WithResults tests a query that returns multiple rows.
func TestTCPServer_Query_RawData_WithResults(t *testing.T) {
	// Setup
	server := setupTCPServerTest(t)
	defer server.close(t)

	fields1, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "error", "status": int64(500)})
	require.NoError(t, err)
	fields2, err := core.NewFieldValuesFromMap(map[string]interface{}{"level": "info", "status": int64(200)})
	require.NoError(t, err)

	// Mock data to be returned by the iterator
	points := []*core.QueryResultItem{
		{Metric: "system.logs", Tags: map[string]string{"host": "A"}, Timestamp: 1, Fields: fields1},
		{Metric: "system.logs", Tags: map[string]string{"host": "A"}, Timestamp: 2, Fields: fields2},
	}

	// Mock the iterator
	mockIterator := NewMockQueryResultIterator(points, nil)

	// Make the mock expectation more specific
	server.mockEngine.On("Query", mock.Anything, mock.MatchedBy(func(params core.QueryParams) bool {
		return params.Metric == "system.logs" && params.StartTime == 0 && params.EndTime == 1000
	})).Return(mockIterator, nil).Once()

	// Connect client
	client := newNBQLTestClient(t, server.appServer.tcpLis.Addr().String())
	defer client.close()

	// Prepare and send command
	command := `QUERY system.logs FROM 0 TO 1000;`
	payload := new(bytes.Buffer)
	err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
	assert.NoError(t, err)
	client.sendFrame(api.CommandQuery, payload.Bytes())

	// Receive and verify response parts
	for i := 0; i < 2; i++ {
		cmdType, payloadResp, err := api.ReadFrame(client.reader)
		assert.NoError(t, err)
		assert.Equal(t, api.CommandQueryResultPart, cmdType)
		resp, err := api.DecodeQueryResponse(bytes.NewReader(payloadResp))
		assert.NoError(t, err)
		assert.Len(t, resp.Results, 1)
		assert.Equal(t, points[i].Timestamp, resp.Results[0].Timestamp, "Timestamp mismatch for item %d", i)
		assert.Equal(t, points[i].Fields, resp.Results[0].Fields, "Fields mismatch for item %d", i)
	}

	// Receive and verify the end frame
	cmdType, payloadResp, err := api.ReadFrame(client.reader)
	assert.NoError(t, err)
	assert.Equal(t, api.CommandQueryEnd, cmdType)
	endResp, err := api.DecodeQueryEndResponse(bytes.NewReader(payloadResp))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), endResp.TotalRows)
}

func TestExecutor_executePush_AutoTimestamp(t *testing.T) {
	mockEngine := new(MockStorageEngine)
	fixedTime := time.Date(2025, time.July, 13, 10, 0, 0, 0, time.UTC)
	mockClock := utils.NewMockClock(fixedTime) // สร้าง MockClock ด้วยเวลาที่กำหนดเอง

	// สร้าง Executor ด้วย mock engine และ mock clock
	executor := api.NewExecutor(mockEngine, mockClock)

	pushCmd := &corenbql.PushStatement{
		Metric:    "cpu.usage",
		Fields:    map[string]interface{}{"value": 50.5},
		Tags:      map[string]string{"host": "server1"},
		Timestamp: 0, // ตั้งเป็น 0 เพื่อให้ Executor ใส่ timestamp อัตโนมัติ
	}

	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 50.5})
	require.NoError(t, err)

	expectedDP := core.DataPoint{
		Metric:    "cpu.usage",
		Timestamp: fixedTime.UnixNano(), // คาดหวัง timestamp จาก mock clock
		Fields:    fields,
		Tags:      map[string]string{"host": "server1"},
	}

	// กำหนดพฤติกรรมของ mock engine
	mockEngine.On("Put", mock.Anything, expectedDP).Return(nil).Once()

	resp, err := executor.Execute(context.Background(), pushCmd)

	res := resp.(api.ManipulateResponse)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), res.RowsAffected)
	mockEngine.AssertExpectations(t) // ตรวจสอบว่า Put ถูกเรียกตามที่คาดหวัง
}
