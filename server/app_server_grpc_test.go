package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"reflect"

	"path/filepath"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/config"
	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAppServer_StartStop_GRPC(t *testing.T) {
	certFile, keyFile := generateTestCerts(t)

	testCases := []struct {
		name       string
		tlsEnabled bool
		setupCreds func(t *testing.T, certFile string) credentials.TransportCredentials
	}{
		{
			name:       "Without TLS",
			tlsEnabled: false,
			setupCreds: func(t *testing.T, certFile string) credentials.TransportCredentials {
				return insecure.NewCredentials()
			},
		},
		{
			name:       "With TLS",
			tlsEnabled: true,
			setupCreds: func(t *testing.T, certFile string) credentials.TransportCredentials {
				caCert, err := os.ReadFile(certFile)
				require.NoError(t, err)
				certPool := x509.NewCertPool()
				require.True(t, certPool.AppendCertsFromPEM(caCert))
				return credentials.NewTLS(&tls.Config{
					ServerName: "127.0.0.1",
					RootCAs:    certPool,
				})
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Setup
			mockEngine := new(MockStorageEngine)
			testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			grpcPort := findFreePort(t)
			tcpPort := 0 // Disable TCP server for this test

			cfg := &config.Config{
				Server: config.ServerConfig{
					GRPCPort:   grpcPort,
					TCPPort:    tcpPort,
					TLSEnabled: tc.tlsEnabled,
					CertFile:   certFile,
					KeyFile:    keyFile,
				},
			}

			mockEngine.On("Close").Return(nil).Once()

			// Create AppServer
			appServer, err := NewAppServer(mockEngine, cfg, testLogger)
			require.NoError(t, err)
			require.NotNil(t, appServer)

			// Start the server in a goroutine
			serverErrChan := make(chan error, 1)
			go func() {
				t.Logf("Starting server for test '%s' on %s", tc.name, appServer.grpcLis.Addr().String())
				serverErrChan <- appServer.Start()
			}()

			// Wait for the server to be listening on the TCP port before we try to connect.
			// This avoids race conditions where the client tries to dial before the server is ready,
			// which can be more pronounced when TLS setup is involved.
			serverReady := false
			for i := 0; i < 20; i++ { // Retry for up to 2 seconds
				conn, err := net.DialTimeout("tcp", appServer.grpcLis.Addr().String(), 100*time.Millisecond)
				if err == nil {
					conn.Close()
					serverReady = true
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			require.True(t, serverReady, "gRPC server did not start listening in time")

			// Wait for the server to be ready by performing a health check
			var conn *grpc.ClientConn
			var healthCheckSuccessful bool
			creds := tc.setupCreds(t, certFile)

			for i := 0; i < 20; i++ { // Increased retries for robustness, especially with TLS
				dialCtx, dialCancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
				dialAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
				// Use grpc.DialContext for proper timeout handling on the connection attempt.
				// grpc.NewClient is deprecated and doesn't handle contexts for dialing.
				conn, err = grpc.DialContext(dialCtx, dialAddr, grpc.WithTransportCredentials(creds), grpc.WithBlock())
				dialCancel()
				if err == nil {
					healthClient := grpc_health_v1.NewHealthClient(conn)
					// Use a new context for the RPC call itself
					rpcCtx, rpcCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
					resp, rpcErr := healthClient.Check(rpcCtx, &grpc_health_v1.HealthCheckRequest{})
					rpcCancel()
					if rpcErr == nil && resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING {
						healthCheckSuccessful = true
						conn.Close()
						break
					}
					conn.Close() // Close connection on failed health check before retrying
				}
				time.Sleep(100 * time.Millisecond)
			}

			require.True(t, healthCheckSuccessful, "Server did not become healthy in time")
			t.Log("Server is healthy and serving.")

			// Stop the server
			t.Log("Stopping server...")
			appServer.Stop()
			t.Log("Server stop command issued, waiting for shutdown...")

			// Wait for the server goroutine to exit gracefully
			select {
			case err := <-serverErrChan:
				assert.NoError(t, err, "appServer.Start() should return nil on graceful shutdown")
			case <-time.After(2 * time.Second):
				t.Fatal("Timed out waiting for server to stop")
			}

			// Manually call Close on the mock engine as it's no longer part of appServer.Stop()
			mockEngine.Close()
			// Assert that all mock expectations were met
			mockEngine.AssertExpectations(t)
		})
	}
}

// setupTestGRPCServer is a helper function to initialize a server with a mock engine
// and return a gRPC client connected to it, along with a cleanup function.
func setupTestGRPCServer(t *testing.T) (tsdb.TSDBServiceClient, *MockStorageEngine, func()) {
	t.Helper()

	mockEngine := new(MockStorageEngine)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	grpcPort := findFreePort(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort:   grpcPort,
			TCPPort:    0, // Disable TCP for this test
			TLSEnabled: false,
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
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCancel()
	dialAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	conn, err := grpc.DialContext(dialCtx, dialAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err, "Failed to connect to test gRPC server")

	client := tsdb.NewTSDBServiceClient(conn)

	cleanup := func() {
		conn.Close()
		appServer.Stop()
		// Wait for the server to stop and check for unexpected errors.
		if err, open := <-serverErrChan; open {
			require.NoError(t, err, "Server exited with an unexpected error")
		}
		mockEngine.Close()
		// Verify that the mock's Close method was called.
		mockEngine.AssertExpectations(t)
	}

	return client, mockEngine, cleanup
}

func TestAppServer_GRPC_Put(t *testing.T) {
	client, mockEngine, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	metric := "grpc.put.test"
	tags := map[string]string{"source": "grpc"}
	ts := time.Now().UnixNano()
	fields := map[string]interface{}{
		"value":      123.45,
		"is_ok":      true,
		"status_str": "OK",
	}
	fieldsStruct, err := structpb.NewStruct(fields)
	require.NoError(t, err)

	// Setup mock expectation. The gRPC handler for Put sends a job to a worker pool,
	// which then calls engine.PutBatch with a slice containing a single point.
	mockEngine.On("PutBatch", mock.Anything, mock.MatchedBy(func(points []core.DataPoint) bool {
		if len(points) != 1 {
			return false
		}
		p := points[0]
		return p.Metric == metric &&
			reflect.DeepEqual(p.Tags, tags) &&
			p.Timestamp == ts &&
			len(p.Fields) == 3
	})).Return(nil).Once()

	// Call RPC
	req := &tsdb.PutRequest{
		Metric:    metric,
		Tags:      tags,
		Timestamp: ts,
		Fields:    fieldsStruct,
	}
	_, err = client.Put(ctx, req)
	require.NoError(t, err)
}

func TestAppServer_GRPC_Query_RawData(t *testing.T) {
	client, mockEngine, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	metric := "grpc.query.test"
	tags := map[string]string{"source": "grpc"}
	startTime := time.Now().Add(-1 * time.Hour).UnixNano()
	endTime := time.Now().UnixNano()

	// Prepare mock data
	mockFields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": 99.9, "status": "good"})
	require.NoError(t, err)

	mockItem := &core.QueryResultItem{
		Metric:    metric,
		Tags:      tags,
		Timestamp: startTime + 100,
		Fields:    mockFields,
	}
	mockIterator := NewMockQueryResultIterator([]*core.QueryResultItem{mockItem}, nil)

	// Setup mock expectation
	mockEngine.On("Query", mock.Anything, mock.MatchedBy(func(params core.QueryParams) bool {
		return params.Metric == metric &&
			params.StartTime == startTime &&
			params.EndTime == endTime &&
			reflect.DeepEqual(params.Tags, tags)
	})).Return(mockIterator, nil).Once()

	// Call RPC
	req := &tsdb.QueryRequest{
		Metric:    metric,
		Tags:      tags,
		StartTime: startTime,
		EndTime:   endTime,
	}
	stream, err := client.Query(ctx, req)
	require.NoError(t, err)

	// Receive from stream
	results := make([]*tsdb.QueryResult, 0)
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	// Assert results
	require.Len(t, results, 1)
	res := results[0]
	assert.Equal(t, metric, res.Metric)
	assert.Equal(t, tags, res.Tags)
	assert.Equal(t, mockItem.Timestamp, res.Timestamp)
	assert.False(t, res.IsAggregated)

	resFields := res.GetFields().AsMap()
	assert.Equal(t, 99.9, resFields["value"])
	assert.Equal(t, "good", resFields["status"])
}

func TestAppServer_GRPC_Query_AggregatedData(t *testing.T) {
	client, mockEngine, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()
	metric := "grpc.query.agg.test"
	startTime := time.Now().Add(-1 * time.Hour).UnixNano()
	endTime := time.Now().UnixNano()

	// Prepare mock data
	mockItem := &core.QueryResultItem{
		Metric:          metric,
		Tags:            map[string]string{"region": "all"},
		IsAggregated:    true,
		WindowStartTime: startTime,
		WindowEndTime:   endTime,
		AggregatedValues: map[string]float64{
			"avg(value)": 50.5,
			"count(*)":   100.0,
		},
	}
	mockIterator := NewMockQueryResultIterator([]*core.QueryResultItem{mockItem}, nil)

	// Setup mock expectation
	mockEngine.On("Query", mock.Anything, mock.MatchedBy(func(params core.QueryParams) bool {
		return params.Metric == metric &&
			len(params.AggregationSpecs) == 2 &&
			params.AggregationSpecs[0].Function == "avg" &&
			params.AggregationSpecs[0].Field == "value"
	})).Return(mockIterator, nil).Once()

	// Call RPC
	req := &tsdb.QueryRequest{
		Metric:    metric,
		StartTime: startTime,
		EndTime:   endTime,
		AggregationSpecs: []*tsdb.AggregationSpec{
			{Function: tsdb.AggregationSpec_AVERAGE, Field: "value"},
			{Function: tsdb.AggregationSpec_COUNT, Field: "*"},
		},
	}
	stream, err := client.Query(ctx, req)
	require.NoError(t, err)

	// Receive from stream
	results := make([]*tsdb.QueryResult, 0)
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		results = append(results, res)
	}

	// Assert results
	require.Len(t, results, 1)
	res := results[0]
	assert.Equal(t, metric, res.Metric)
	assert.True(t, res.IsAggregated)
	assert.Equal(t, startTime, res.WindowStartTime)
	assert.Equal(t, endTime, res.WindowEndTime)
	assert.InDeltaMapValues(t, map[string]float64{"avg(value)": 50.5, "count(*)": 100.0}, res.AggregatedValues, 1e-9)
}

// findFreePort finds an available TCP port and returns it.
func findFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to resolve TCP address: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen on TCP port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// generateTestCerts creates a self-signed certificate and key for testing purposes.
// It returns the paths to the generated certificate and key files.
func generateTestCerts(t *testing.T) (string, string) {
	t.Helper()

	// Create a temporary directory for certs
	certDir := t.TempDir()
	certFile := filepath.Join(certDir, "cert.pem")
	keyFile := filepath.Join(certDir, "key.pem")

	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// Create a certificate template
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Corp"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	// Create a self-signed certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Write certificate to file
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("Failed to open cert.pem for writing: %v", err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatalf("Failed to write data to cert.pem: %v", err)
	}
	if err := certOut.Close(); err != nil {
		t.Fatalf("Error closing cert.pem: %v", err)
	}

	// Write private key to file
	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatalf("Failed to open key.pem for writing: %v", err)
	}
	privBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("Unable to marshal private key: %v", err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes}); err != nil {
		t.Fatalf("Failed to write data to key.pem: %v", err)
	}
	if err := keyOut.Close(); err != nil {
		t.Fatalf("Error closing key.pem: %v", err)
	}

	return certFile, keyFile
}
func TestAppServer_HealthCheck_Failure(t *testing.T) {
	// 1. Setup
	mockEngine := new(MockStorageEngine)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	grpcPort := findFreePort(t)
	tcpPort := 0 // Disable TCP server for this test

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort:                   grpcPort,
			TCPPort:                    tcpPort,
			TLSEnabled:                 false,
			HealthCheckIntervalSeconds: 1,
		},
	}

	mockEngine.On("Close").Return(nil).Once()

	appServer, err := NewAppServer(mockEngine, cfg, testLogger)
	require.NoError(t, err)
	require.NotNil(t, appServer)

	// 2. Start server
	serverErrChan := make(chan error, 1)
	go func() {
		serverErrChan <- appServer.Start()
	}()

	// 3. Create a gRPC client to check health
	testCtx, testCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer testCancel()
	dialAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	conn, err := grpc.DialContext(testCtx, dialAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	require.NoError(t, err)
	defer conn.Close()
	healthClient := grpc_health_v1.NewHealthClient(conn)

	// 4. Set health to NOT_SERVING and verify
	appServer.grpcServer.healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	resp, err := healthClient.Check(testCtx, &grpc_health_v1.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, grpc_health_v1.HealthCheckResponse_NOT_SERVING, resp.GetStatus())

	// 5. Stop the server and verify mocks
	appServer.Stop()
	select {
	case err := <-serverErrChan:
		assert.NoError(t, err, "appServer.Start() should return nil on graceful shutdown")
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for server to stop")
	}
	// Manually call Close on the mock engine as it's no longer part of appServer.Stop()
	mockEngine.Close()
	mockEngine.AssertExpectations(t)
}

func TestAppServer_TLS_BadCert(t *testing.T) {
	// 1. Setup Server
	mockEngine := new(MockStorageEngine)
	testLogger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	grpcPort := findFreePort(t)
	tcpPort := 0 // Disable TCP server for this test
	certFile, keyFile := generateTestCerts(t)

	cfg := &config.Config{
		Server: config.ServerConfig{
			GRPCPort:   grpcPort,
			TCPPort:    tcpPort,
			TLSEnabled: true,
			CertFile:   certFile,
			KeyFile:    keyFile,
		},
	}

	mockEngine.On("Close").Return(nil).Once()

	appServer, err := NewAppServer(mockEngine, cfg, testLogger)
	require.NoError(t, err)

	serverErrChan := make(chan error, 1)
	go func() {
		// We expect a graceful shutdown, so the error should be nil or a specific server stopped error.
		if err := appServer.Start(); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErrChan <- err
		}
		close(serverErrChan)
	}()

	defer func() {
		appServer.Stop()
		// Wait for server to stop
		if err, open := <-serverErrChan; open {
			require.NoError(t, err, "Server exited with an unexpected error")
		}
		mockEngine.Close()
		mockEngine.AssertExpectations(t)
	}()

	// Wait for the server to be listening on the TCP port before we try to connect.
	// This avoids race conditions where the client tries to dial before the server is ready.
	serverReady := false
	for i := 0; i < 20; i++ { // Retry for up to 2 seconds
		conn, err := net.DialTimeout("tcp", appServer.grpcLis.Addr().String(), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			serverReady = true
			t.Log("Test server is listening.")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, serverReady, "gRPC server did not start listening in time")

	// 2. Setup Client with a DIFFERENT CA it trusts.
	// To test a "bad certificate", we create a TLS config for the client that does NOT
	// trust the server's self-signed certificate. We do this by creating a new, separate
	// CA and telling the client to only trust that one.
	clientCACertFile, _ := generateTestCerts(t) // Generate a dummy CA for the client
	caCert, err := os.ReadFile(clientCACertFile)
	require.NoError(t, err)
	clientCertPool := x509.NewCertPool()                                                                  // Create an empty pool
	require.True(t, clientCertPool.AppendCertsFromPEM(caCert), "Failed to append client's dummy CA cert") // Add ONLY the dummy CA

	// We set the ServerName to match the certificate to ensure the failure is due to trust,
	// not a hostname mismatch.
	creds := credentials.NewTLS(&tls.Config{
		ServerName: "127.0.0.1",
		RootCAs:    clientCertPool, // Trust only our dummy CA, not the server's CA
	})

	// 3. Attempt to connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dialAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	// The dial itself might not fail immediately, as the handshake can be lazy.
	// We remove WithBlock() and check for the error on the first RPC call.
	conn, dialErr := grpc.NewClient(dialAddr, grpc.WithTransportCredentials(creds))
	require.NoError(t, dialErr, "grpc.NewClient should not return an immediate error")
	defer conn.Close()

	// 4. Attempt an RPC call, which should trigger the handshake and fail.
	healthClient := grpc_health_v1.NewHealthClient(conn)
	_, rpcErr := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})

	// 5. Verify the RPC call fails with the appropriate error.
	require.Error(t, rpcErr, "Expected RPC to fail due to bad certificate")
	st, ok := status.FromError(rpcErr)
	require.True(t, ok, "Error should be a gRPC status error")
	assert.Equal(t, codes.Unavailable, st.Code(), "Expected 'Unavailable' code for transport failure")
	assert.Contains(t, st.Message(), "certificate signed by unknown authority", "Error message should indicate a certificate trust issue")

}

func TestAppServer_GRPC_ForceFlush(t *testing.T) {
	client, mockEngine, cleanup := setupTestGRPCServer(t)
	defer cleanup()

	ctx := context.Background()

	// Setup mock expectation.
	mockEngine.On("ForceFlush", mock.Anything, true).Return(nil).Once()

	// Call RPC
	_, err := client.ForceFlush(ctx, &tsdb.ForceFlushRequest{})
	require.NoError(t, err)
}
