package goclient

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"reflect"
	"testing"

	pb "github.com/INLOpen/nexusbase/clients/goclient/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

// mockTSDBServer is a mock implementation of the TSDBServiceServer interface.
type mockTSDBServer struct {
	pb.UnimplementedTSDBServiceServer
	// A function to override the default Query behavior for specific tests.
	queryFunc func(*pb.QueryRequest, pb.TSDBService_QueryServer) error
}

// Query implements the gRPC server's Query method.
func (s *mockTSDBServer) Query(req *pb.QueryRequest, stream pb.TSDBService_QueryServer) error {
	if s.queryFunc != nil {
		return s.queryFunc(req, stream)
	}
	// Default behavior if no override is provided
	return status.Error(codes.Unimplemented, "method Query not implemented")
}

// newTestClient creates a goclient.Client connected to a mock server for testing.
// It returns the client, the mock server instance, and a cleanup function.
func newTestClient(t *testing.T) (*Client, *mockTSDBServer, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	mockServer := &mockTSDBServer{}
	grpcServer := grpc.NewServer()
	pb.RegisterTSDBServiceServer(grpcServer, mockServer)

	go func() {
		if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("server exited with error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := &Client{
		conn:   conn,
		client: pb.NewTSDBServiceClient(conn),
	}

	cleanup := func() {
		grpcServer.Stop()
		client.Close()
	}

	return client, mockServer, cleanup
}

func TestQuery(t *testing.T) {
	ctx := context.Background()

	// Test case 1: Successful query with multiple results
	t.Run("successful query", func(t *testing.T) {
		client, mockServer, cleanup := newTestClient(t)
		defer cleanup()

		// Prepare mock response
		expectedResults := []*QueryResultItem{
			{Metric: "cpu.usage", Tags: map[string]string{"host": "server1"}, Timestamp: 1672531200, Fields: map[string]interface{}{"value": 100.0, "status": "ok"}},
			{Metric: "cpu.usage", Tags: map[string]string{"host": "server1"}, Timestamp: 1672531260, Fields: map[string]interface{}{"value": 101.5, "status": "ok"}},
		}

		// Configure the mock server's behavior for this test case
		mockServer.queryFunc = func(req *pb.QueryRequest, stream pb.TSDBService_QueryServer) error {
			// Stream the results back
			for _, res := range expectedResults {
				fields, err := structpb.NewStruct(res.Fields)
				if err != nil {
					return status.Errorf(codes.Internal, "failed to create structpb: %v", err)
				}
				// สมมติว่า pb.QueryResult มี fields ที่สอดคล้องกับ QueryResultItem
				pbItem := &pb.QueryResult{
					Metric:    res.Metric,
					Tags:      res.Tags,
					Timestamp: res.Timestamp,
					Fields:    fields,
				}
				if err := stream.Send(pbItem); err != nil {
					return status.Errorf(codes.Internal, "failed to send stream message: %v", err)
				}
			}
			return nil
		}

		// Call the function under test
		iterator, err := client.Query(ctx, QueryParams{Metric: "cpu.usage"})
		if err != nil {
			t.Fatalf("Query() returned an unexpected error: %v", err)
		}

		// Iterate and check results
		var gotResults []*QueryResultItem
		for {
			item, err := iterator.Next()
			if err == io.EOF {
				break // สิ้นสุด stream ไม่ใช่ error
			}
			if err != nil {
				t.Fatalf("iterator.Next() returned an unexpected error: %v", err)
			}
			gotResults = append(gotResults, item)
		}
		if !reflect.DeepEqual(gotResults, expectedResults) {
			t.Errorf("Query() results mismatch:\n got: %+v\nwant: %+v", gotResults, expectedResults)
		}
	})

	// Test case 2: Server returns an error during streaming
	t.Run("server stream error", func(t *testing.T) {
		client, mockServer, cleanup := newTestClient(t)
		defer cleanup()

		mockServer.queryFunc = func(req *pb.QueryRequest, stream pb.TSDBService_QueryServer) error {
			fields, err := structpb.NewStruct(map[string]interface{}{"value": 100.0})
			if err != nil {
				return status.Errorf(codes.Internal, "failed to create structpb: %v", err)
			}
			// ส่ง 1 item ที่สำเร็จ
			_ = stream.Send(&pb.QueryResult{Timestamp: 1672531200, Fields: fields})
			return status.Error(codes.Internal, "database connection lost")
		}

		iterator, err := client.Query(ctx, QueryParams{Metric: "test"})
		if err != nil {
			t.Fatalf("Query() returned an unexpected error on initial call: %v", err)
		}

		// item แรกควรจะรับได้ปกติ
		_, err = iterator.Next()
		if err != nil {
			t.Fatalf("Expected first Next() call to succeed, but got error: %v", err)
		}

		// item ที่สองควรจะคืนค่า error
		_, err = iterator.Next()
		if err == nil {
			t.Fatal("Expected an error from the second Next() call, but got nil")
		}
		if st, ok := status.FromError(err); !ok || st.Code() != codes.Internal {
			t.Errorf("expected internal gRPC error, got: %v", err)
		}
	})

	// Test case 3: Initial call to Query fails
	t.Run("initial query error", func(t *testing.T) {
		client, mockServer, cleanup := newTestClient(t)
		defer cleanup()

		expectedErr := status.Error(codes.InvalidArgument, "metric name is required")
		mockServer.queryFunc = func(req *pb.QueryRequest, stream pb.TSDBService_QueryServer) error {
			return expectedErr
		}

		iterator, err := client.Query(ctx, QueryParams{Metric: ""})
		if err != nil {
			t.Fatalf("Query() returned an unexpected error on initial call: %v", err)
		}

		// การเรียก Next() ครั้งแรกควรจะเจอ error ที่ส่งมาจาก server
		_, nextErr := iterator.Next()
		if nextErr == nil {
			t.Fatal("iterator.Next() did not return an error as expected")
		}

		if st, ok := status.FromError(nextErr); !ok || st.Code() != codes.InvalidArgument {
			t.Errorf("expected invalid argument error, got: %v", nextErr)
		}
	})
}
