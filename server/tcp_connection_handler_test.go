package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	api "github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockAuthenticator is a simple mock for core.IAuthenticator for testing purposes.
type mockAuthenticator struct {
	AuthenticateUserPassFunc func(username, password string) error
}

// Implement the core.IAuthenticator interface
func (m *mockAuthenticator) Authenticate(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (m *mockAuthenticator) Authorize(ctx context.Context, requiredRole string) error { return nil }
func (m *mockAuthenticator) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return handler(ctx, req)
}
func (m *mockAuthenticator) StreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return handler(srv, ss)
}
func (m *mockAuthenticator) AuthenticateUserPass(username, password string) error {
	if m.AuthenticateUserPassFunc != nil {
		return m.AuthenticateUserPassFunc(username, password)
	}
	return nil // Default success
}

// MockPacket is a mock implementation of IPacket for testing malformed payloads.
type MockPacket struct {
	Data []byte
}

func (m *MockPacket) MarshalBinary() ([]byte, error) {
	return m.Data, nil
}

func (m *MockPacket) UnmarshalBinary(data []byte) error {
	m.Data = data
	return nil
}

func TestTCPConnectionHandler_HandleAuthentication(t *testing.T) {
	// Setup a TCPConnectionHandler instance for testing.
	mockAuth := &mockAuthenticator{}
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewTCPConnectionHandler(nil, mockAuth, testLogger)

	// Helper to create a valid request packet
	createRequestPacket := func(username, password string) []byte {
		reqPayload := &RequestAuthenticationPacket{
			Username: username,
			Password: password,
		}
		packet := &AuthenticationPacket{
			Version: 1,
			Op:      ConnectRequestAuthenticationOp,
			Payload: reqPayload,
		}
		data, err := packet.MarshalBinary()
		require.NoError(t, err)
		return data
	}

	testCases := []struct {
		name                 string
		setupMock            func()
		clientAction         func(t *testing.T, clientConn net.Conn)
		expectSuccess        bool
		expectResponse       bool
		expectedResponseCode ResponseOp
		expectedResponseMsg  string
	}{
		{
			name: "Successful Authentication",
			setupMock: func() {
				mockAuth.AuthenticateUserPassFunc = func(username, password string) error {
					assert.Equal(t, "testuser", username)
					assert.Equal(t, "goodpass", password)
					return nil
				}
			},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				reqData := createRequestPacket("testuser", "goodpass")
				_, err := clientConn.Write(reqData)
				require.NoError(t, err)
			},
			expectSuccess:        true,
			expectResponse:       true,
			expectedResponseCode: ResponseOK,
			expectedResponseMsg:  "Authentication successful",
		},
		{
			name: "Failed Authentication - Wrong Password",
			setupMock: func() {
				mockAuth.AuthenticateUserPassFunc = func(username, password string) error {
					return status.Error(codes.Unauthenticated, "invalid credentials")
				}
			},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				reqData := createRequestPacket("testuser", "badpass")
				_, err := clientConn.Write(reqData)
				require.NoError(t, err)
			},
			expectSuccess:        false,
			expectResponse:       true,
			expectedResponseCode: ResponseError,
			expectedResponseMsg:  "Invalid username or password",
		},
		{
			name:      "Failed - Invalid Operation Code",
			setupMock: func() {},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				packet := &AuthenticationPacket{
					Version: 1,
					Op:      ConnectResponseAuthenticationOp, // Invalid for a request
					Payload: &RequestAuthenticationPacket{},
				}
				data, err := packet.MarshalBinary()
				require.NoError(t, err)
				_, err = clientConn.Write(data)
				require.NoError(t, err)
			},
			expectSuccess:        false,
			expectResponse:       true,
			expectedResponseCode: ResponseError,
			expectedResponseMsg:  "Invalid operation during authentication",
		},
		{
			name:      "Failed - Short Header Read",
			setupMock: func() {},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				clientConn.Write([]byte{0x01, 0x01})
				clientConn.Close()
			},
			expectSuccess:  false,
			expectResponse: false,
		},
		{
			name:      "Failed - Short Payload Read",
			setupMock: func() {},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				header := []byte{0x01, 0x01, 0x00, 0x0A} // Version 1, Op 1, Len 10
				payload := []byte("hello")
				clientConn.Write(header)
				clientConn.Write(payload)
				clientConn.Close()
			},
			expectSuccess:  false,
			expectResponse: false,
		},
		{
			name:      "Failed - Malformed Payload",
			setupMock: func() {},
			clientAction: func(t *testing.T, clientConn net.Conn) {
				malformedPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
				packet := &AuthenticationPacket{
					Version: 1,
					Op:      ConnectRequestAuthenticationOp,
					Payload: &MockPacket{Data: malformedPayload},
				}
				data, err := packet.MarshalBinary()
				require.NoError(t, err)
				_, err = clientConn.Write(data)
				require.NoError(t, err)
			},
			expectSuccess:        false,
			expectResponse:       true,
			expectedResponseCode: ResponseError,
			expectedResponseMsg:  "Invalid authentication payload format",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMock()

			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()

			var wg sync.WaitGroup
			var success bool
			wg.Add(1)

			// Run the server-side logic in a goroutine, just like a real server would.
			go func() {
				defer wg.Done()
				// IMPORTANT: Close the server-side connection when the handler is done.
				// This signals EOF to the client's io.ReadAll, unblocking it and preventing a deadlock.
				defer serverConn.Close()
				reader := bufio.NewReader(serverConn)
				writer := bufio.NewWriter(serverConn)
				success = handler.HandleAuthentication(context.Background(), serverConn, reader, writer)
			}()

			// The main test goroutine now acts as the client.
			// It sends the request...
			tc.clientAction(t, clientConn)

			// ...and then it MUST read the response to unblock the server's write.
			if tc.expectResponse {
				respData, err := io.ReadAll(clientConn)
				// When a pipe is closed, a read can result in io.EOF or net.ErrClosedPipe.
				// The error message "io: read/write on closed pipe" corresponds to io.ErrClosedPipe.
				// Both are expected outcomes in these test scenarios, so we check for them.
				if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("Failed to read response from server: %v", err)
				}
				require.NotEmpty(t, respData, "Expected a response from the server, but got none")

				respPacket := &AuthenticationPacket{Payload: &ResponseAuthenticationPacket{}}
				err = respPacket.UnmarshalBinary(respData)
				require.NoError(t, err)

				respPayload, ok := respPacket.Payload.(*ResponseAuthenticationPacket)
				require.True(t, ok)
				assert.Equal(t, tc.expectedResponseCode, respPayload.Status)
				assert.Equal(t, tc.expectedResponseMsg, respPayload.Message)
			} else {
				respData, err := io.ReadAll(clientConn)
				if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("Unexpected error when reading for no response: %v", err)
				}
				assert.Empty(t, respData, "Expected no response from the server, but got data")
			}

			// Wait for the server goroutine to finish before asserting its result.
			wg.Wait()
			assert.Equal(t, tc.expectSuccess, success)
		})
	}
}

func TestTCPConnectionHandler_HandleCommand(t *testing.T) {
	mockEngine := new(MockStorageEngine)
	mockClock := clock.NewMockClock(time.Now())
	executor := api.NewExecutor(mockEngine, mockClock)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := NewTCPConnectionHandler(executor, nil, testLogger)

	t.Run("Successful PUSH command", func(t *testing.T) {
		// Arrange
		// The executor calls engine.Put for a PUSH command.
		// The mock should expect a call to Put, which returns an error.
		mockEngine.On("Put", mock.Anything, mock.AnythingOfType("core.DataPoint")).Return(nil).Once()
		var writer bytes.Buffer
		fields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 123.4})
		payloadBuf := new(bytes.Buffer)
		api.EncodePushRequest(payloadBuf, api.PushRequest{Metric: "test.push", Fields: fields})

		// Act
		handler.HandleCommand(context.Background(), &writer, api.CommandPush, payloadBuf.Bytes())

		// Assert
		cmdType, respPayload, err := api.ReadFrame(&writer)
		require.NoError(t, err)
		assert.Equal(t, api.CommandManipulate, cmdType)
		resp, err := api.DecodeManipulateResponse(bytes.NewReader(respPayload))
		require.NoError(t, err)
		assert.Equal(t, uint64(1), resp.RowsAffected)
		mockEngine.AssertExpectations(t)
	})

	t.Run("Successful QUERY command with results", func(t *testing.T) {
		// Arrange
		fields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 123.0})
		mockIterator := engine.NewMockQueryResultIterator([]*core.QueryResultItem{
			{Metric: "test.metric", Timestamp: 1, Fields: fields},
			{Metric: "test.metric", Timestamp: 2, Fields: fields},
		}, nil)
		// The executor's QueryStream method calls engine.Query internally.
		// The mock should expect a call to Query.
		mockEngine.On("Query", mock.Anything, mock.AnythingOfType("core.QueryParams")).Return(mockIterator, nil).Once()
		var writer bytes.Buffer
		payloadBuf := new(bytes.Buffer)
		api.EncodeQueryRequest(payloadBuf, api.QueryRequest{QueryString: "QUERY test.metric FROM 0 TO 10;"})

		// Act
		handler.HandleCommand(context.Background(), &writer, api.CommandQuery, payloadBuf.Bytes())

		// Assert
		reader := bufio.NewReader(&writer)
		// Frame 1: Data
		cmdType1, _, err1 := api.ReadFrame(reader)
		require.NoError(t, err1)
		assert.Equal(t, api.CommandQueryResultPart, cmdType1)
		// Frame 2: Data
		cmdType2, _, err2 := api.ReadFrame(reader)
		require.NoError(t, err2)
		assert.Equal(t, api.CommandQueryResultPart, cmdType2)
		// Frame 3: End
		cmdType3, payload3, err3 := api.ReadFrame(reader)
		require.NoError(t, err3)
		assert.Equal(t, api.CommandQueryEnd, cmdType3)
		endResp, err := api.DecodeQueryEndResponse(bytes.NewReader(payload3))
		require.NoError(t, err)
		assert.Equal(t, uint64(2), endResp.TotalRows)
		mockEngine.AssertExpectations(t)
	})

	t.Run("Command with executor error", func(t *testing.T) {
		// Arrange
		expectedErr := errors.New("engine is down")
		// The mock should expect a call to Put, which returns the error.
		mockEngine.On("Put", mock.Anything, mock.AnythingOfType("core.DataPoint")).Return(expectedErr).Once()
		var writer bytes.Buffer
		payloadBuf := new(bytes.Buffer)
		api.EncodePushRequest(payloadBuf, api.PushRequest{Metric: "test.push"})

		// Act
		handler.HandleCommand(context.Background(), &writer, api.CommandPush, payloadBuf.Bytes())

		// Assert
		cmdType, respPayload, err := api.ReadFrame(&writer)
		require.NoError(t, err)
		assert.Equal(t, api.CommandError, cmdType)
		decodedErr := api.DecodeErrorMessage(bytes.NewReader(respPayload))
		require.Error(t, decodedErr)
		assert.Contains(t, decodedErr.Error(), "executor failed")
		assert.Contains(t, decodedErr.Error(), "engine is down")
		mockEngine.AssertExpectations(t)
	})

	t.Run("Command with malformed payload", func(t *testing.T) {
		// Arrange
		var writer bytes.Buffer
		malformedPayload := []byte{1, 2, 3} // Not a valid PushRequest

		// Act
		handler.HandleCommand(context.Background(), &writer, api.CommandPush, malformedPayload)

		// Assert
		cmdType, _, err := api.ReadFrame(&writer)
		require.NoError(t, err)
		assert.Equal(t, api.CommandError, cmdType)
	})

	t.Run("Unknown command type", func(t *testing.T) {
		// Arrange
		var writer bytes.Buffer

		// Act
		handler.HandleCommand(context.Background(), &writer, api.CommandType(99), nil)

		// Assert
		cmdType, respPayload, err := api.ReadFrame(&writer)
		require.NoError(t, err)
		assert.Equal(t, api.CommandError, cmdType)
		decodedErr := api.DecodeErrorMessage(bytes.NewReader(respPayload))
		require.Error(t, decodedErr)
		assert.Contains(t, decodedErr.Error(), "unknown command type")
	})
}
