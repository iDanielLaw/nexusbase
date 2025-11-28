package replication

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	pb "github.com/INLOpen/nexusbase/replication/proto"
	"github.com/INLOpen/nexusbase/wal"
)

// TestRealWALReaderSend verifies the real wal implementation notifies a
// StreamReader and that a server-like loop can read entries and forward them
// to a fake stream (no gRPC transport involved).
func TestRealWALReaderSend(t *testing.T) {
	dir, err := os.MkdirTemp("", "waltest-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	opts := wal.Options{Dir: dir}
	w, recovered, err := wal.Open(opts)
	if err != nil {
		t.Fatalf("wal.Open failed: %v", err)
	}
	defer w.Close()
	if len(recovered) != 0 {
		t.Fatalf("expected no recovered entries, got %d", len(recovered))
	}

	// Register a real stream reader starting from seq 0
	sr, err := w.NewStreamReader(0)
	if err != nil {
		t.Fatalf("NewStreamReader failed: %v", err)
	}
	defer sr.Close()

	// fake stream to receive protobuf messages
	fs := &fakeStream{sent: make(chan *pb.WALEntry, 4)}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start a goroutine that continuously reads from the StreamReader and
	// forwards entries to the fake stream. Note: `streamReader.Next()` will
	// return `wal.ErrNoNewEntries` when it finishes reading all closed
	// segments during catch-up; this is not a fatal error — it signals the
	// caller to switch to tailing mode. The test must therefore continue
	// looping on `ErrNoNewEntries` so it remains waiting for live notifications
	// from the active segment rather than exiting the goroutine prematurely.
	go func() {
		for {
			e, err := sr.Next(ctx)
			if err != nil {
				if errors.Is(err, wal.ErrNoNewEntries) {
					// finished catch-up, keep waiting for live notifications
					continue
				}
				return
			}
			// forward minimal proto with sequence only
			_ = fs.Send(&pb.WALEntry{SequenceNumber: e.SeqNum})
		}
	}()

	// Append an entry to the WAL and expect the reader->stream to observe it
	wantSeq := uint64(12345)
	if err := w.Append(core.WALEntry{EntryType: core.EntryTypePutEvent, Key: []byte("k"), Value: []byte("v"), SeqNum: wantSeq}); err != nil {
		t.Fatalf("wal.Append failed: %v", err)
	}

	select {
	case got := <-fs.sent:
		if got.SequenceNumber != wantSeq {
			t.Fatalf("unexpected seq: got %d want %d", got.SequenceNumber, wantSeq)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for stream to receive message from real WAL")
	}

	// Close reader
	_ = sr.Close()
}
