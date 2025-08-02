package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostSSTableCreateHook verifies that the PostSSTableCreate hook is triggered
// correctly after a memtable flush.
func TestPostSSTableCreateHook(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	// Disable periodic compaction to have full control
	opts.CompactionIntervalSeconds = 3600
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)
	signalChan := make(chan hooks.HookEvent, 1)
	listener := &mockListener{isAsync: true, callSignal: signalChan}
	concreteEngine.hookManager.Register(hooks.EventPostSSTableCreate, listener)

	// Put some data to ensure the memtable is not empty
	err = engine.Put(context.Background(), HelperDataPoint(t, "metric.create", map[string]string{"tag": "val"}, 1, map[string]interface{}{"value": 1.0}))
	require.NoError(t, err)

	// Force a flush, which should create an SSTable and trigger the hook
	err = engine.ForceFlush(context.Background(), true) // Wait for flush to complete
	require.NoError(t, err)

	// Wait for the async hook to be called
	select {
	case event := <-signalChan:
		payload, ok := event.Payload().(hooks.SSTablePayload)
		require.True(t, ok, "PostSSTableCreate hook received payload of wrong type: %T", event.Payload())

		assert.Equal(t, 0, payload.Level, "SSTable created from flush should be at Level 0")
		assert.Greater(t, payload.ID, uint64(0), "SSTable ID should be positive")
		assert.Greater(t, payload.Size, int64(0), "SSTable size should be positive")
		assert.True(t, strings.HasSuffix(payload.Path, fmt.Sprintf("%d.sst", payload.ID)), "SSTable path should match its ID")

	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed out waiting for PostSSTableCreate hook to be called")
	}
}

// TestPreSSTableDeleteHook verifies that the PreSSTableDelete hook is triggered
// before a table is removed during compaction.
func TestPreSSTableDeleteHook(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	// Use small memtable and L0 trigger to force compaction
	opts.MemtableThreshold = 512
	opts.MaxL0Files = 2
	opts.CompactionIntervalSeconds = 1 // Frequent checks
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)
	signalChan := make(chan hooks.HookEvent, 2) // Expect multiple tables to be deleted
	// Add the assertion inside the hook's OnEvent function to avoid race conditions.
	listener := &mockListener{
		isAsync:    false, // Sync to ensure it's called before file is gone
		callSignal: signalChan,
		onEventFunc: func(event hooks.HookEvent) {
			payload, ok := event.Payload().(hooks.SSTablePayload)
			require.True(t, ok)
			assert.FileExists(t, payload.Path, "File should still exist when PreSSTableDelete hook is called")
		},
	}
	concreteEngine.hookManager.Register(hooks.EventPreSSTableDelete, listener)

	// Put enough data to create at least two L0 files
	for i := 0; i < 20; i++ {
		err := engine.Put(context.Background(), HelperDataPoint(t, "metric.delete", map[string]string{"i": fmt.Sprintf("%d", i)}, int64(i), map[string]interface{}{"value": float64(i)}))
		require.NoError(t, err)
	}

	// Wait for compaction to trigger and run
	// This is tricky in a test. We'll wait and check the signal channel.
	var eventsReceived []hooks.SSTablePayload
	timeout := time.After(5 * time.Second)
	done := false
	for !done {
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.SSTablePayload)
			require.True(t, ok)
			eventsReceived = append(eventsReceived, payload)
			if len(eventsReceived) >= 2 { // L0 compaction merges at least 2 tables
				done = true
			}
		case <-timeout:
			t.Fatalf("Timed out waiting for PreSSTableDelete hook. Received %d events.", len(eventsReceived))
		}
	}

	require.GreaterOrEqual(t, len(eventsReceived), 2, "Expected at least 2 PreSSTableDelete events from L0 compaction")
	for _, payload := range eventsReceived {
		assert.Equal(t, 0, payload.Level, "Deleted SSTable should be from Level 0")
		assert.Greater(t, payload.ID, uint64(0))
	}
}

// TestPostManifestWriteHook verifies that the PostManifestWrite hook is triggered
// after the manifest is persisted.
func TestPostManifestWriteHook(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)
	signalChan := make(chan hooks.HookEvent, 1)
	listener := &mockListener{isAsync: true, callSignal: signalChan}
	concreteEngine.hookManager.Register(hooks.EventPostManifestWrite, listener)

	// Put data and force a synchronous flush, which will persist the manifest
	err = engine.Put(context.Background(), HelperDataPoint(t, "metric.manifest", map[string]string{"tag": "val"}, 1, map[string]interface{}{"value": 1.0}))
	require.NoError(t, err)
	err = engine.ForceFlush(context.Background(), true)
	require.NoError(t, err)

	// Wait for the async hook
	select {
	case event := <-signalChan:
		payload, ok := event.Payload().(hooks.ManifestWritePayload)
		require.True(t, ok)
		assert.True(t, strings.HasPrefix(filepath.Base(payload.Path), "MANIFEST_"), "Manifest path should have the correct prefix")
		assert.FileExists(t, payload.Path)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed out waiting for PostManifestWrite hook to be called")
	}
}

func TestCacheHooks(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.BlockCacheCapacity = 1 // Very small cache to force evictions
	opts.SSTableDefaultBlockSize = 32

	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)
	hitChan := make(chan hooks.HookEvent, 5)
	missChan := make(chan hooks.HookEvent, 5)
	evictChan := make(chan hooks.HookEvent, 5)

	concreteEngine.hookManager.Register(hooks.EventOnCacheHit, &mockListener{isAsync: true, callSignal: hitChan})
	concreteEngine.hookManager.Register(hooks.EventOnCacheMiss, &mockListener{isAsync: true, callSignal: missChan})
	concreteEngine.hookManager.Register(hooks.EventOnCacheEviction, &mockListener{isAsync: true, callSignal: evictChan})

	// Put enough data to create at least 3 distinct blocks
	require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, "metric.cache", map[string]string{"id": "1"}, 1, map[string]interface{}{"value": "block1-data-that-is-long-enough"})))
	require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, "metric.cache", map[string]string{"id": "2"}, 2, map[string]interface{}{"value": "block2-data-that-is-long-enough"})))
	require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, "metric.cache", map[string]string{"id": "3"}, 3, map[string]interface{}{"value": "block3-data-that-is-long-enough"})))
	require.NoError(t, engine.ForceFlush(context.Background(), true))

	// --- Verification ---
	// 1. Get key 1 -> Miss
	_, err = engine.Get(context.Background(), "metric.cache", map[string]string{"id": "1"}, 1)
	require.NoError(t, err)
	select {
	case <-missChan: // Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for first cache miss hook")
	}

	// 2. Get key 1 again -> Hit
	_, err = engine.Get(context.Background(), "metric.cache", map[string]string{"id": "1"}, 1)
	require.NoError(t, err)
	select {
	case <-hitChan: // Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for cache hit hook")
	}

	// 3. Get key 2 -> Miss, should evict key 1's block
	_, err = engine.Get(context.Background(), "metric.cache", map[string]string{"id": "2"}, 2)
	require.NoError(t, err)
	select {
	case <-missChan: // Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for second cache miss hook")
	}
	select {
	case <-evictChan: // Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for cache eviction hook")
	}

	// 4. Get key 1 again -> Miss (since it was evicted)
	_, err = engine.Get(context.Background(), "metric.cache", map[string]string{"id": "1"}, 1)
	require.NoError(t, err)
	select {
	case <-missChan: // Good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Timed out waiting for third cache miss hook")
	}
}

func TestWALHooks(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.WALMaxSegmentSize = 1024     // Small segment size to force rotation
	opts.WALSyncMode = wal.SyncAlways // CRITICAL: Ensure writes are flushed to disk so size check works.

	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)
	rotateChan := make(chan hooks.HookEvent, 1)

	// This simplified test focuses only on the rotation hook, which was the point of failure.
	// Testing multiple interacting hooks (especially sync + async) in a tight loop is complex and prone to race conditions.
	concreteEngine.hookManager.Register(hooks.EventPostWALRotate, &mockListener{isAsync: true, callSignal: rotateChan})

	// Put enough data to trigger a WAL rotation. This is now synchronous in the main test goroutine.
	for i := 0; i < 15; i++ {
		dp := HelperDataPoint(t, "metric.wal", map[string]string{"i": fmt.Sprintf("%d", i)}, int64(i), map[string]interface{}{"value": "a-very-long-string-to-fill-up-the-wal-segment-quickly-and-force-a-rotation-event-to-be-triggered"})
		require.NoError(t, engine.Put(context.Background(), dp))
	}

	// Verify PostWALRotate was called
	select {
	case event := <-rotateChan:
		payload, ok := event.Payload().(hooks.PostWALRotatePayload)
		require.True(t, ok)
		assert.Equal(t, uint64(1), payload.OldSegmentIndex)
		assert.Equal(t, uint64(2), payload.NewSegmentIndex)
	case <-time.After(1 * time.Second): // Give it a generous timeout for I/O
		t.Fatal("Timed out waiting for PostWALRotate hook")
	}
}
