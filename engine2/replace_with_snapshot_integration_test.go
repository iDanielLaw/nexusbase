package engine2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal/testutil"
)

// TestReplaceWithSnapshot_E2E verifies that creating a full snapshot and then
// restoring it via ReplaceWithSnapshot yields the same data after engine restart.
func TestReplaceWithSnapshot_E2E(t *testing.T) {
	ctx := context.Background()

	// leader data dir
	leaderDir := filepath.Join(t.TempDir(), "leader")
	leaderEngine, err := NewEngine2(leaderDir)
	if err != nil {
		t.Fatalf("failed to create leader engine: %v", err)
	}
	leaderAdapter := NewEngine2AdapterWithHooks(leaderEngine, nil)
	if err := leaderAdapter.Start(); err != nil {
		t.Fatalf("failed to start leader adapter: %v", err)
	}

	// write some datapoints
	fv, err := core.NewFieldValuesFromMap(map[string]interface{}{"v": 1.0})
	if err != nil {
		t.Fatalf("failed to build FieldValues: %v", err)
	}
	dp := core.DataPoint{Metric: "m1", Tags: map[string]string{"host": "a"}, Timestamp: 12345, Fields: fv}
	if err := leaderAdapter.Put(context.Background(), dp); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	// Acquire provider lock and flush memtables like snapshot creation caller would do
	leaderAdapter.Lock()
	mems, _ := leaderAdapter.GetMemtablesForFlush()
	leaderAdapter.Unlock()
	for _, mem := range mems {
		if err := leaderAdapter.FlushMemtableToL0(mem, context.Background()); err != nil {
			t.Fatalf("flush memtable failed: %v", err)
		}
	}

	// create full snapshot
	snapDir := filepath.Join(leaderAdapter.GetSnapshotsBaseDir(), "full1")
	mgr := leaderAdapter.GetSnapshotManager()
	if err := mgr.CreateFull(ctx, snapDir); err != nil {
		t.Fatalf("CreateFull snapshot failed: %v", err)
	}

	// Close adapter and replace data dir from snapshot
	if err := leaderAdapter.Close(); err != nil {
		t.Fatalf("failed to close leader adapter: %v", err)
	}

	// Now call ReplaceWithSnapshot on the same adapter instance (simulates restore)
	if err := leaderAdapter.ReplaceWithSnapshot(snapDir); err != nil {
		t.Fatalf("ReplaceWithSnapshot failed: %v", err)
	}

	// Before reopening the engine, list and assert restored files for diagnostics.
	// sstables/ should exist and contain at least one SSTable.
	sstDir := filepath.Join(leaderDir, "sstables")
	sstEntries, sstErr := os.ReadDir(sstDir)
	if sstErr != nil {
		t.Fatalf("expected sstables directory after restore, stat error: %v", sstErr)
	}
	if len(sstEntries) == 0 {
		t.Fatalf("expected at least one sstable file in %s after restore", sstDir)
	}
	// list restored sstables (no debug logs)

	// WAL presence: configurable via internal test helper
	if testutil.WALStrictEnabled() {
		testutil.RequireWALPresent(t, leaderDir)
	} else {
		// permissive: do not assert WAL presence; skip debug logging.
	}

	// private mapping logs should be present
	strMap := filepath.Join(leaderDir, "string_mapping.log")
	if _, err := os.Stat(strMap); err != nil {
		t.Fatalf("expected string_mapping.log after restore, stat error: %v", err)
	}

	seriesMap := filepath.Join(leaderDir, "series_mapping.log")
	if _, err := os.Stat(seriesMap); err != nil {
		t.Fatalf("expected series_mapping.log after restore, stat error: %v", err)
	}

	// After restore, construct a fresh engine instance over the same data dir to validate contents
	newEngine, err := NewEngine2(leaderDir)
	if err != nil {
		t.Fatalf("failed to open engine after restore: %v", err)
	}
	newAdapter := NewEngine2AdapterWithHooks(newEngine, nil)
	if err := newAdapter.Start(); err != nil {
		t.Fatalf("failed to start adapter after restore: %v", err)
	}

	var got core.FieldValues
	got, err = newAdapter.Get(context.Background(), "m1", map[string]string{"host": "a"}, 12345)
	if err != nil {
		t.Fatalf("get after restore failed: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected field values after restore, got empty")
	}
}
