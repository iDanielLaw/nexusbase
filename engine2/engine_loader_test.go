package engine2

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/require"
)

// The original engine tests accessed internal fields. For engine2 we exercise
// the same recovery scenarios via the public StorageEngineInterface APIs.

func TestStateLoader_Load_FreshStart(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")

	eng, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, eng.Start())
	defer eng.Close()

	// Fresh engine should start with sequence number 0 and a WAL present.
	seq := eng.GetSequenceNumber()
	require.Equal(t, uint64(0), seq)
	require.NotNil(t, eng.GetWAL())
}

func TestStateLoader_Load_FromManifest(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")

	// Phase 1: create engine and write data, then close cleanly to persist manifest
	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine1.Start())

	dp1 := HelperDataPoint(t, "metric.manifest", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})
	require.NoError(t, engine1.Put(context.Background(), dp1))

	dp2 := HelperDataPoint(t, "metric.wal", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})
	require.NoError(t, engine1.Put(context.Background(), dp2))

	require.NoError(t, engine1.Close())

	// Phase 2: create a new engine instance which should recover from manifest
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number should be restored (two puts -> seq 2)
	require.Equal(t, uint64(2), engine2.GetSequenceNumber())

	// Data should be queryable via public Get
	val1, err := engine2.Get(context.Background(), "metric.manifest", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))

	val2, err := engine2.Get(context.Background(), "metric.wal", map[string]string{"id": "b"}, 200)
	require.NoError(t, err)
	require.Equal(t, 2.0, HelperFieldValueValidateFloat64(t, val2, "value"))
}

func TestStateLoader_Load_FallbackScan(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	dataDir := opts.DataDir

	// Phase 1: create a valid engine state with SSTables but no manifest
	engine1, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine1.Start())

	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	require.NoError(t, engine1.Put(context.Background(), HelperDataPoint(t, "metric.fallback", map[string]string{"id": "b"}, 200, map[string]interface{}{"value": 2.0})))
	require.NoError(t, engine1.Close())

	// Remove manifest file(s) to force fallback scan
	_ = sys.Remove(filepath.Join(dataDir, core.CurrentFileName))
	files, _ := os.ReadDir(dataDir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "MANIFEST") {
			_ = sys.Remove(filepath.Join(dataDir, f.Name()))
		}
	}

	// Phase 2: start new engine which should fallback-scan SSTables
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number may be reset to 0 on fallback
	require.Equal(t, uint64(0), engine2.GetSequenceNumber())

	// Data should still be queryable
	val1, err := engine2.Get(context.Background(), "metric.fallback", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}

func TestStateLoader_Load_WithWALRecovery(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")

	// Write to WAL and then crash
	crashEngine(t, opts, func(e StorageEngineInterface) {
		require.NoError(t, e.Put(context.Background(), HelperDataPoint(t, "metric.wal.recovery", map[string]string{"id": "a"}, 100, map[string]interface{}{"value": 1.0})))
	})

	// Load and recover
	engine2, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine2.Start())
	defer engine2.Close()

	// Sequence number should be restored from WAL
	require.Equal(t, uint64(1), engine2.GetSequenceNumber())

	val1, err := engine2.Get(context.Background(), "metric.wal.recovery", map[string]string{"id": "a"}, 100)
	require.NoError(t, err)
	require.Equal(t, 1.0, HelperFieldValueValidateFloat64(t, val1, "value"))
}
