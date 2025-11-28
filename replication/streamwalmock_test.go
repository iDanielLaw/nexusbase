package replication

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	pb "github.com/INLOpen/nexusbase/replication/proto"
)

// This test implements an in-process WAL + StreamWAL sender/stream pair
// to isolate the notify -> send handoff without involving gRPC transport.
func TestInProcessStreamWAL(t *testing.T) {
	// fake WAL: channel of entries
	ch := make(chan core.WALEntry, 8)

	// fake reader that reads from channel
	reader := &fakeStreamReader{ch: ch}

	// fake stream that collects sent protobuf WALEntry messages
	fs := &fakeStream{sent: make(chan *pb.WALEntry, 8)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the simplified server-side loop that reads from the reader and sends
	// protobuf messages via the fake stream. This mirrors the core send path
	// but avoids convertWALEntryToProto complexity; we only verify that
	// Next() -> Send() call chain functions correctly.
	done := make(chan error, 1)
	go func() {
		for {
			e, err := reader.Next(ctx)
			if err != nil {
				done <- err
				return
			}
			// Build a minimal protobuf message and send it
			msg := &pb.WALEntry{SequenceNumber: e.SeqNum}
			if err := fs.Send(msg); err != nil {
				done <- err
				return
			}
		}
	}()

	// Append a test entry into channel (simulate leader notify)
	wantSeq := uint64(42)
	ch <- core.WALEntry{SeqNum: wantSeq}

	// Wait for the fake stream to receive it
	select {
	case got := <-fs.sent:
		if got.SequenceNumber != wantSeq {
			t.Fatalf("unexpected seq: got %d want %d", got.SequenceNumber, wantSeq)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for fake stream to receive message")
	}

	// Close reader to finish goroutine
	reader.Close()
	// Wait for goroutine to exit (it may return context.Canceled or nil)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
	}
}

// fakeStreamReader implements a minimal wal.StreamReader-like API for the test.
type fakeStreamReader struct {
	ch chan core.WALEntry
}

func (f *fakeStreamReader) Next(ctx context.Context) (*core.WALEntry, error) {
	select {
	case e := <-f.ch:
		// return a copy
		cp := e
		return &cp, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeStreamReader) Close() error { close(f.ch); return nil }

// fakeStream implements the minimal subset of methods the server loop calls.
type fakeStream struct {
	sent chan *pb.WALEntry
}

func (f *fakeStream) Send(e *pb.WALEntry) error {
	f.sent <- e
	return nil
}

// Context is not used by the simplified loop but implement it for compatibility
func (f *fakeStream) Context() context.Context { return context.Background() }
