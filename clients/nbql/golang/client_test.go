package nbql_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	nbql "github.com/INLOpen/nexusbase/clients/nbql/golang"
	"github.com/INLOpen/nexusbase/clients/nbql/golang/core"
	"github.com/INLOpen/nexusbase/clients/nbql/golang/protocol"
)

// mockServer represents a mock NBQL server for testing purposes.
type mockServer struct {
	t        *testing.T
	listener net.Listener
	wg       sync.WaitGroup
	handler  func(conn net.Conn)
}

// newMockServer creates and starts a mock server.
func newMockServer(t *testing.T, handler func(conn net.Conn)) *mockServer {
	t.Helper()
	// Use port 0 to let the OS choose a free port.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &mockServer{
		t:        t,
		listener: lis,
		handler:  handler,
	}

	s.wg.Add(1)
	go s.run()

	return s
}

// run is the main loop for the mock server, accepting connections.
func (s *mockServer) run() {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check for the specific error that occurs when the listener is closed.
			if errors.Is(err, net.ErrClosed) {
				return // Graceful shutdown
			}
			s.t.Logf("mock server accept error: %v", err)
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			if s.handler != nil {
				s.handler(conn)
			}
		}()
	}
}

// Addr returns the address of the mock server.
func (s *mockServer) Addr() string {
	return s.listener.Addr().String()
}

// Close stops the mock server.
func (s *mockServer) Close() {
	s.listener.Close()
	s.wg.Wait()
}

func TestConnect(t *testing.T) {
	t.Run("Successful connection without auth", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			// Server does nothing, just accepts the connection.
		})
		defer server.Close()

		// Act
		opts := nbql.Options{Address: server.Addr()}
		client, err := nbql.Connect(context.Background(), opts)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, client)
		client.Close()
	})

	t.Run("Successful connection with auth", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			// Mock server must handle the authentication handshake.
			reader := bufio.NewReader(conn)
			header := make([]byte, 4)
			_, err := io.ReadFull(reader, header)
			require.NoError(t, err)

			// Send back a successful response.
			respPacket := &protocol.AuthenticationPacket{
				Version: 1,
				Op:      protocol.ConnectResponseAuthenticationOp,
				Payload: &protocol.ResponseAuthenticationPacket{
					Status:  protocol.ResponseOK,
					Message: "OK",
				},
			}
			respData, err := respPacket.MarshalBinary()
			require.NoError(t, err)
			_, err = conn.Write(respData)
			require.NoError(t, err)
		})
		defer server.Close()

		// Act
		opts := nbql.Options{
			Address:  server.Addr(),
			Username: "user",
			Password: "password",
		}
		client, err := nbql.Connect(context.Background(), opts)

		// Assert
		require.NoError(t, err)
		require.NotNil(t, client)
		client.Close()
	})

	t.Run("Failed authentication", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			// Mock server must handle the authentication handshake and fail it.
			reader := bufio.NewReader(conn)
			header := make([]byte, 4)
			_, err := io.ReadFull(reader, header)
			require.NoError(t, err)

			// Send back a failure response.
			respPacket := &protocol.AuthenticationPacket{
				Version: 1,
				Op:      protocol.ConnectResponseAuthenticationOp,
				Payload: &protocol.ResponseAuthenticationPacket{
					Status:  protocol.ResponseError,
					Message: "Invalid credentials",
				},
			}
			respData, err := respPacket.MarshalBinary()
			require.NoError(t, err)
			_, err = conn.Write(respData)
			require.NoError(t, err)
		})
		defer server.Close()

		// Act
		opts := nbql.Options{
			Address:  server.Addr(),
			Username: "user",
			Password: "wrong-password",
		}
		client, err := nbql.Connect(context.Background(), opts)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server authentication failed: Invalid credentials")
		assert.Nil(t, client)
	})

	t.Run("Connection timeout", func(t *testing.T) {
		// This test doesn't need a running server.
		// We dial an address that will not respond.
		opts := nbql.Options{Address: "10.255.255.1:12345"} // Use a black hole address
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		client, err := nbql.Connect(ctx, opts)

		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Nil(t, client)
	})
}

func TestClient_Push(t *testing.T) {
	t.Run("Successful push", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			cmdType, payload, err := protocol.ReadFrame(reader)
			require.NoError(t, err)
			assert.Equal(t, protocol.CommandPush, cmdType)

			// Verify the received push request
			req, err := protocol.DecodePushRequest(bytes.NewReader(payload))
			require.NoError(t, err)
			assert.Equal(t, "test.metric", req.Metric)
			assert.Equal(t, int64(12345), req.Timestamp)

			// Send back a success response
			respBuf := new(bytes.Buffer)
			err = protocol.EncodeManipulateResponse(respBuf, protocol.ManipulateResponse{RowsAffected: 1})
			require.NoError(t, err)
			err = protocol.WriteFrame(conn, protocol.CommandManipulate, respBuf.Bytes())
			require.NoError(t, err)
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		err = client.Push(context.Background(), "test.metric", nil, map[string]interface{}{"value": 1.0}, 12345)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("Push with server error", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			_, _, err := protocol.ReadFrame(reader) // Read and discard request
			require.NoError(t, err)

			// Send back an error response
			respBuf := new(bytes.Buffer)
			err = protocol.EncodeErrorMessage(respBuf, &protocol.ErrorMessage{Message: "engine is down"})
			require.NoError(t, err)
			err = protocol.WriteFrame(conn, protocol.CommandError, respBuf.Bytes())
			require.NoError(t, err)
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		err = client.Push(context.Background(), "test.metric", nil, map[string]interface{}{"value": 1.0}, 12345)

		// Assert
		require.Error(t, err)
		assert.Equal(t, "engine is down", err.Error())
	})
}

func TestClient_Query(t *testing.T) {
	t.Run("Successful query with multiple rows", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			cmdType, payload, err := protocol.ReadFrame(reader)
			require.NoError(t, err)
			assert.Equal(t, protocol.CommandQuery, cmdType)

			req, err := protocol.DecodeQueryRequest(bytes.NewReader(payload))
			require.NoError(t, err)
			assert.Equal(t, "QUERY test.metric", req.QueryString)

			// Send back two data parts
			fields1, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 10.0})
			resp1 := protocol.QueryResponse{
				Status: protocol.ResponseDataRow,
				Results: []protocol.QueryResultLine{
					{Timestamp: 1, Fields: fields1},
				},
			}
			buf1 := new(bytes.Buffer)
			protocol.EncodeQueryResponse(buf1, resp1)
			protocol.WriteFrame(conn, protocol.CommandQueryResultPart, buf1.Bytes())

			fields2, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 20.0})
			resp2 := protocol.QueryResponse{
				Status: protocol.ResponseDataRow,
				Results: []protocol.QueryResultLine{
					{Timestamp: 2, Fields: fields2},
				},
			}
			buf2 := new(bytes.Buffer)
			protocol.EncodeQueryResponse(buf2, resp2)
			protocol.WriteFrame(conn, protocol.CommandQueryResultPart, buf2.Bytes())

			// Send back the end frame
			endResp := protocol.QueryEndResponse{Status: protocol.ResponseDataEnd, TotalRows: 2}
			endBuf := new(bytes.Buffer)
			protocol.EncodeQueryEndResponse(endBuf, endResp)
			protocol.WriteFrame(conn, protocol.CommandQueryEnd, endBuf.Bytes())
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		result, err := client.Query(context.Background(), "QUERY test.metric")

		// Assert
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, uint64(2), result.TotalRows)
		require.Len(t, result.Rows, 2)
		assert.Equal(t, int64(1), result.Rows[0].Timestamp)
		assert.Equal(t, 10.0, result.Rows[0].Fields["value"])
		assert.Equal(t, int64(2), result.Rows[1].Timestamp)
		assert.Equal(t, 20.0, result.Rows[1].Fields["value"])
	})

	t.Run("Query with server error", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			_, _, err := protocol.ReadFrame(reader) // Read and discard request
			require.NoError(t, err)

			// Send back an error response
			respBuf := new(bytes.Buffer)
			err = protocol.EncodeErrorMessage(respBuf, &protocol.ErrorMessage{Message: "syntax error"})
			require.NoError(t, err)
			err = protocol.WriteFrame(conn, protocol.CommandError, respBuf.Bytes())
			require.NoError(t, err)
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		result, err := client.Query(context.Background(), "QUERY invalid")

		// Assert
		require.Error(t, err)
		assert.Equal(t, "syntax error", err.Error())
		assert.Nil(t, result)
	})

	t.Run("Successful parameterized query", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			cmdType, payload, err := protocol.ReadFrame(reader)
			require.NoError(t, err)
			assert.Equal(t, protocol.CommandQuery, cmdType)

			req, err := protocol.DecodeQueryRequest(bytes.NewReader(payload))
			require.NoError(t, err)
			// The server should receive the fully formatted query
			assert.Equal(t, `QUERY "test.metric" TAGGED (host="server-1", region=123, active=true)`, req.QueryString)

			// Send back the end frame
			endResp := protocol.QueryEndResponse{Status: protocol.ResponseDataEnd, TotalRows: 0}
			endBuf := new(bytes.Buffer)
			protocol.EncodeQueryEndResponse(endBuf, endResp)
			protocol.WriteFrame(conn, protocol.CommandQueryEnd, endBuf.Bytes())
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		queryTemplate := "QUERY ? TAGGED (host=?, region=?, active=?)"
		_, err = client.Query(context.Background(), queryTemplate, "test.metric", "server-1", 123, true)

		// Assert
		require.NoError(t, err)
	})

	t.Run("Parameterized query with placeholder mismatch", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			// This handler is called upon connection, but it should not receive any data
			// because the client's Query() method should fail validation before writing to the network.
			// A read should result in an EOF when the client closes the connection, not data.
			buf := make([]byte, 1)
			if n, err := conn.Read(buf); n > 0 || err == nil {
				t.Errorf("server handler received unexpected data (%d bytes, err: %v)", n, err)
			}
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		queryTemplate := "QUERY ? TAGGED (host=?)"                                // 2 placeholders
		_, err = client.Query(context.Background(), queryTemplate, "test.metric") // 1 param

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to format query")
		assert.Contains(t, err.Error(), "query placeholder mismatch")
	})
}

func TestClient_ConcurrentAccess(t *testing.T) {
	// Arrange
	// This handler needs to be robust enough to handle many concurrent requests.
	// It will read a frame, and based on the command type, send a canned response.
	handler := func(conn net.Conn) {
		reader := bufio.NewReader(conn)
		for {
			cmdType, payload, err := protocol.ReadFrame(reader)
			if err != nil {
				// Client disconnected or connection closed, which is a normal way to end the loop.
				if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
					return
				}
				t.Logf("mock server read error: %v", err)
				return
			}

			var respBuf bytes.Buffer
			var respCmdType protocol.CommandType

			switch cmdType {
			case protocol.CommandPushs:
				// Decode to find out how many points were in the batch
				req, err := protocol.DecodePushsRequest(bytes.NewReader(payload))
				require.NoError(t, err)
				// Send back a success response for Pushs
				err = protocol.EncodeManipulateResponse(&respBuf, protocol.ManipulateResponse{RowsAffected: uint64(len(req.Items))})
				require.NoError(t, err)
				respCmdType = protocol.CommandManipulate
			case protocol.CommandQuery:
				// Send back an empty but successful query response
				endResp := protocol.QueryEndResponse{Status: protocol.ResponseDataEnd, TotalRows: 0}
				err = protocol.EncodeQueryEndResponse(&respBuf, endResp)
				require.NoError(t, err)
				respCmdType = protocol.CommandQueryEnd
			default:
				// If we receive an unexpected command, fail the test.
				t.Errorf("mock server received unexpected command type: %v", cmdType)
				return
			}

			// Write the response back to the client.
			err = protocol.WriteFrame(conn, respCmdType, respBuf.Bytes())
			if err != nil {
				// The client might have closed the connection while we were processing, which is fine.
				return
			}
		}
	}

	server := newMockServer(t, handler)
	defer server.Close()

	client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
	require.NoError(t, err)
	defer client.Close()

	// Act: Spawn multiple goroutines to access the client concurrently.
	var wg sync.WaitGroup
	numGoroutines := 10
	opsPerGoroutine := 50

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of Push and Query operations
				if j%2 == 0 {
					points := []nbql.Point{
						{
							Metric:    "concurrent.metric",
							Tags:      map[string]string{"worker": fmt.Sprintf("%d", workerID)},
							Fields:    map[string]interface{}{"value": float64(j)},
							Timestamp: time.Now().UnixNano(),
						},
					}
					err := client.PushBulk(context.Background(), points)
					assert.NoError(t, err, "PushBulk failed in worker %d", workerID)
				} else {
					_, err := client.Query(context.Background(), "QUERY concurrent.metric")
					assert.NoError(t, err, "Query failed in worker %d", workerID)
				}
			}
		}(i)
	}

	wg.Wait()

	// Assert: The main assertion is that no errors occurred and the test didn't panic,
	// which is handled by the assert.NoError calls inside the goroutines. If we reach
	// this point without the test failing, the concurrent access is considered successful.
}

func TestClient_PushBulk(t *testing.T) {
	t.Run("Successful push bulk", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			cmdType, payload, err := protocol.ReadFrame(reader)
			require.NoError(t, err)
			assert.Equal(t, protocol.CommandPushs, cmdType)

			// Verify the received push request
			req, err := protocol.DecodePushsRequest(bytes.NewReader(payload))
			require.NoError(t, err)
			require.Len(t, req.Items, 2)
			assert.Equal(t, "test.metric.bulk", req.Items[0].Metric)
			assert.Equal(t, "test.metric.bulk", req.Items[1].Metric)

			// Send back a success response
			respBuf := new(bytes.Buffer)
			err = protocol.EncodeManipulateResponse(respBuf, protocol.ManipulateResponse{RowsAffected: 2})
			require.NoError(t, err)
			err = protocol.WriteFrame(conn, protocol.CommandManipulate, respBuf.Bytes())
			require.NoError(t, err)
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		points := []nbql.Point{
			{Metric: "test.metric.bulk", Fields: map[string]interface{}{"value": 1.0}, Timestamp: 12345},
			{Metric: "test.metric.bulk", Fields: map[string]interface{}{"value": 2.0}, Timestamp: 12346},
		}
		err = client.PushBulk(context.Background(), points)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("Push bulk with server error", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			reader := bufio.NewReader(conn)
			_, _, err := protocol.ReadFrame(reader) // Read and discard request
			require.NoError(t, err)

			// Send back an error response
			respBuf := new(bytes.Buffer)
			err = protocol.EncodeErrorMessage(respBuf, &protocol.ErrorMessage{Message: "batch failed"})
			require.NoError(t, err)
			err = protocol.WriteFrame(conn, protocol.CommandError, respBuf.Bytes())
			require.NoError(t, err)
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		points := []nbql.Point{
			{Metric: "test.metric.bulk", Fields: map[string]interface{}{"value": 1.0}},
		}
		err = client.PushBulk(context.Background(), points)

		// Assert
		require.Error(t, err)
		assert.Equal(t, "batch failed", err.Error())
	})

	t.Run("Push bulk with empty points list", func(t *testing.T) {
		// Arrange
		server := newMockServer(t, func(conn net.Conn) {
			// This handler is called upon connection, but it should not receive any data.
			buf := make([]byte, 1)
			if n, err := conn.Read(buf); n > 0 || err == nil {
				t.Errorf("server handler received unexpected data (%d bytes, err: %v)", n, err)
			}
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		err = client.PushBulk(context.Background(), []nbql.Point{})

		// Assert
		assert.NoError(t, err)
	})

	t.Run("Push bulk with invalid field type", func(t *testing.T) {
		server := newMockServer(t, func(conn net.Conn) {
			// This handler is called upon connection, but it should not receive any data
			// because the client's PushBulk() method should fail validation before writing to the network.
			// A read should result in an EOF when the client closes the connection, not data.
			buf := make([]byte, 1)
			if n, err := conn.Read(buf); n > 0 || err == nil {
				t.Errorf("server handler received unexpected data (%d bytes, err: %v)", n, err)
			}
		})
		defer server.Close()

		client, err := nbql.Connect(context.Background(), nbql.Options{Address: server.Addr()})
		require.NoError(t, err)
		defer client.Close()

		// Act
		points := []nbql.Point{
			{Metric: "test.metric.bulk", Fields: map[string]interface{}{"value": make(chan int)}},
		}
		err = client.PushBulk(context.Background(), points)

		// Assert
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported value type")
	})
}
