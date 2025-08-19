package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	api "github.com/INLOpen/nexusbase/api/nbql"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/engine"
	"github.com/INLOpen/nexuscore/utils/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Helper to create a valid auth request packet
func createRequestPacket(username, password string) []byte {
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
	if err != nil {
		panic(err) // Should not happen in test
	}
	return data
}

// Helper to read and unmarshal an auth response packet
func readAuthResponse(t *testing.T, r io.Reader) *AuthenticationPacket {
	t.Helper()
	// Read header first to get payload length
	header := make([]byte, 4)
	_, err := io.ReadFull(r, header)
	require.NoError(t, err)

	payloadLen := binary.BigEndian.Uint16(header[2:4])
	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		_, err = io.ReadFull(r, payload)
		require.NoError(t, err)
	}

	fullPacketData := append(header, payload...)
	respPacket := &AuthenticationPacket{Payload: &ResponseAuthenticationPacket{}}
	err = respPacket.UnmarshalBinary(fullPacketData)
	require.NoError(t, err)
	return respPacket
}

func TestTCP2Server_HandlerConnection(t *testing.T) {
	// Setup mocks and dependencies
	mockAuth := &mockAuthenticator{}
	mockEngine := new(engine.MockStorageEngine)
	mockClock := clock.NewMockClock(time.Now())
	executor := api.NewExecutor(mockEngine, mockClock)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	// Create the server instance to be tested
	// Set security to true to ensure authentication path is tested.
	server := NewTCP2Server(executor, mockAuth, true, testLogger)

	t.Run("Successful Authentication and Command", func(t *testing.T) {
		// Arrange
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		// Mock authenticator to succeed
		mockAuth.AuthenticateUserPassFunc = func(username, password string) error {
			assert.Equal(t, "testuser", username)
			assert.Equal(t, "goodpass", password)
			return nil
		}

		// Mock engine for the PUSH command
		mockEngine.On("Put", mock.Anything, mock.AnythingOfType("core.DataPoint")).Return(nil).Once()

		// Act: Add to WaitGroup and run server handler in a goroutine
		server.connWg.Add(1)
		go server.HandleConnection(serverConn)

		// Act: Client sends auth packet and reads response
		_, err := clientConn.Write(createRequestPacket("testuser", "goodpass"))
		require.NoError(t, err)
		authRespPacket := readAuthResponse(t, clientConn)
		respPayload, _ := authRespPacket.Payload.(*ResponseAuthenticationPacket)
		assert.Equal(t, ResponseOK, respPayload.Status)

		// Act: Client sends a command
		command := `PUSH cpu.usage SET (value=99.9);`
		payload := new(bytes.Buffer)
		err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
		require.NoError(t, err)
		err = api.WriteFrame(clientConn, api.CommandQuery, payload.Bytes())
		require.NoError(t, err)

		// Act: Client reads command response
		cmdType, respPayloadBytes, err := api.ReadFrame(bufio.NewReader(clientConn))
		require.NoError(t, err)
		assert.Equal(t, api.CommandManipulate, cmdType)
		manipResp, err := api.DecodeManipulateResponse(bytes.NewReader(respPayloadBytes))
		require.NoError(t, err)
		assert.Equal(t, uint64(1), manipResp.RowsAffected)

		// Cleanup
		serverConn.Close()
		mockEngine.AssertExpectations(t)
	})

	t.Run("Failed Authentication", func(t *testing.T) {
		// Arrange
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		// Mock authenticator to fail
		mockAuth.AuthenticateUserPassFunc = func(username, password string) error {
			return status.Error(codes.Unauthenticated, "bad creds")
		}

		// Act: Add to WaitGroup and run server handler
		server.connWg.Add(1)
		go server.HandleConnection(serverConn)

		// Act: Client sends auth packet and reads response
		_, err := clientConn.Write(createRequestPacket("testuser", "badpass"))
		require.NoError(t, err)
		authRespPacket := readAuthResponse(t, clientConn)
		respPayload, _ := authRespPacket.Payload.(*ResponseAuthenticationPacket)
		assert.Equal(t, ResponseError, respPayload.Status)
		assert.Equal(t, "Invalid username or password", respPayload.Message)

		// The connection should be closed by the server. A subsequent read should fail.
		_, err = clientConn.Read(make([]byte, 1))
		assert.ErrorIs(t, err, io.EOF)

		serverConn.Close()
	})

	t.Run("Query with results", func(t *testing.T) {
		// Arrange
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		mockAuth.AuthenticateUserPassFunc = func(username, password string) error { return nil }
		fields, _ := core.NewFieldValuesFromMap(map[string]interface{}{"value": 123.0})
		mockIterator := NewMockQueryResultIterator([]*core.QueryResultItem{
			{Metric: "test.metric", Timestamp: 1, Fields: fields},
			{Metric: "test.metric", Timestamp: 2, Fields: fields},
		}, nil)
		mockEngine.On("Query", mock.Anything, mock.AnythingOfType("core.QueryParams")).Return(mockIterator, nil).Once()

		// Act
		server.connWg.Add(1)
		go server.HandleConnection(serverConn)

		_, err := clientConn.Write(createRequestPacket("", ""))
		require.NoError(t, err)
		_ = readAuthResponse(t, clientConn) // Read and discard OK response

		command := `QUERY test.metric FROM 0 TO 10;`
		payload := new(bytes.Buffer)
		err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
		require.NoError(t, err)
		err = api.WriteFrame(clientConn, api.CommandQuery, payload.Bytes())
		require.NoError(t, err)

		// Assert
		reader := bufio.NewReader(clientConn)
		// Read part 1 & 2
		_, _, err = api.ReadFrame(reader)
		require.NoError(t, err)
		_, _, err = api.ReadFrame(reader)
		require.NoError(t, err)
		// Read end
		cmdType, respPayloadBytes, err := api.ReadFrame(reader)
		require.NoError(t, err)
		assert.Equal(t, api.CommandQueryEnd, cmdType)
		endResp, err := api.DecodeQueryEndResponse(bytes.NewReader(respPayloadBytes))
		require.NoError(t, err)
		assert.Equal(t, uint64(2), endResp.TotalRows)

		// Cleanup
		serverConn.Close()
		mockEngine.AssertExpectations(t)
	})

	t.Run("Command with execution error", func(t *testing.T) {
		// Arrange
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()

		mockAuth.AuthenticateUserPassFunc = func(username, password string) error { return nil }
		expectedErr := errors.New("engine is down")
		mockEngine.On("Put", mock.Anything, mock.AnythingOfType("core.DataPoint")).Return(expectedErr).Once()

		// Act
		server.connWg.Add(1)
		go server.HandleConnection(serverConn)

		_, err := clientConn.Write(createRequestPacket("", ""))
		require.NoError(t, err)
		_ = readAuthResponse(t, clientConn)

		command := `PUSH cpu.usage SET (value=99.9);`
		payload := new(bytes.Buffer)
		err = api.EncodeQueryRequest(payload, api.QueryRequest{QueryString: command})
		require.NoError(t, err)
		err = api.WriteFrame(clientConn, api.CommandQuery, payload.Bytes())
		require.NoError(t, err)

		// Assert
		cmdType, respPayloadBytes, err := api.ReadFrame(bufio.NewReader(clientConn))
		require.NoError(t, err)
		assert.Equal(t, api.CommandError, cmdType)
		// The DecodeErrorMessage function is designed to return the decoded message as an error.
		decodedErr := api.DecodeErrorMessage(bytes.NewReader(respPayloadBytes))
		require.Error(t, decodedErr, "DecodeErrorMessage should return the server's error message, not nil")
		assert.Contains(t, decodedErr.Error(), "executor failed", "The error message should contain the context")
		assert.Contains(t, decodedErr.Error(), "engine is down", "The error message should contain the original error")

		// Cleanup
		serverConn.Close()
		mockEngine.AssertExpectations(t)
	})
}
