package replication

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/INLOpen/nexusbase/replication/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestTransport_Repro verifies that a WALEntry sent by a simple gRPC server
// over a real TCP listener arrives at a client via StreamWAL -> Recv(). This
// isolates transport (TCP/gRPC) from the bufconn test harness.
func TestTransport_Repro(t *testing.T) {
	// Prepare a proto entry to send
	fields, _ := structpb.NewStruct(map[string]interface{}{"value": 42})
	protoEntry := &pb.WALEntry{
		SequenceNumber: 1,
		EntryType:      pb.WALEntry_PUT_EVENT,
		Metric:         "m",
		Tags:           map[string]string{"k": "v"},
		Fields:         fields,
		Timestamp:      1234567890,
	}

	// Simple server implementation that sends one entry then waits briefly.
	svc := &testServiceSingle{entry: protoEntry}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterReplicationServiceServer(s, svc)

	go func() {
		_ = s.Serve(lis)
	}()
	defer func() {
		s.Stop()
		lis.Close()
	}()

	// Dial the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, lis.Addr().String(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	client := pb.NewReplicationServiceClient(conn)
	stream, err := client.StreamWAL(ctx, &pb.StreamWALRequest{FromSequenceNumber: 1})
	if err != nil {
		t.Fatalf("StreamWAL call failed: %v", err)
	}

	// Attempt to receive the message
	recvCtx, recvCancel := context.WithTimeout(ctx, 3*time.Second)
	defer recvCancel()
	ch := make(chan *pb.WALEntry, 1)
	errCh := make(chan error, 1)
	go func() {
		e, rerr := stream.Recv()
		if rerr != nil {
			errCh <- rerr
			return
		}
		ch <- e
	}()

	select {
	case e := <-ch:
		if e.GetSequenceNumber() != protoEntry.SequenceNumber {
			t.Fatalf("received sequence mismatch: got %d want %d", e.GetSequenceNumber(), protoEntry.SequenceNumber)
		}
		// success
	case rerr := <-errCh:
		t.Fatalf("stream.Recv returned error: %v", rerr)
	case <-recvCtx.Done():
		t.Fatalf("timed out waiting for stream.Recv()")
	}
}

// testServiceSingle is a tiny Replication service that returns NotFound for
// GetLatestSnapshotInfo and sends a single WALEntry for StreamWAL.
type testServiceSingle struct {
	pb.UnimplementedReplicationServiceServer
	entry *pb.WALEntry
}

func (t *testServiceSingle) GetLatestSnapshotInfo(ctx context.Context, req *pb.GetLatestSnapshotInfoRequest) (*pb.SnapshotInfo, error) {
	return nil, status.Error(codes.NotFound, "no snapshots")
}

func (t *testServiceSingle) StreamWAL(req *pb.StreamWALRequest, stream pb.ReplicationService_StreamWALServer) error {
	// Send the entry
	if err := stream.Send(t.entry); err != nil {
		return err
	}
	// Wait a short time so client has time to receive
	time.Sleep(200 * time.Millisecond)
	return nil
}
