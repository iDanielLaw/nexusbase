package engine2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/stretchr/testify/require"
)

const localMaxFlushRetries = 3

// Test MoveToDLQ behavior
func Test_MoveToDLQ(t *testing.T) {
	t.Run("Success_WithData", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable(1024, a.clk)
		// Use adapter string store to create a realistic key
		metricID, _ := a.stringStore.GetOrCreateID("dlq.metric.1")
		key := core.EncodeTSDBKey(metricID, nil, 100)
		mem.Put(key, []byte("value1"), core.EntryTypePutEvent, 10)

		err = a.MoveToDLQ(mem)
		require.NoError(t, err)

		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)

		// Ensure file is non-empty
		st, err := os.Stat(filepath.Join(a.GetDLQDir(), files[0].Name()))
		require.NoError(t, err)
		require.Greater(t, st.Size(), int64(0))
	})

	t.Run("Success_EmptyMemtable", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable(1024, a.clk)

		err = a.MoveToDLQ(mem)
		require.NoError(t, err)

		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)

		st, err := os.Stat(filepath.Join(a.GetDLQDir(), files[0].Name()))
		require.NoError(t, err)
		require.Equal(t, int64(0), st.Size())
	})

	t.Run("Failure_DLQDirNotConfigured", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		// Simulate not configured by clearing engine data root
		a.Engine2 = &Engine2{} // zero-value Engine2 -> GetDataRoot() should be empty

		mem := memtable.NewMemtable(1024, a.clk)
		mem.Put([]byte("key"), []byte("val"), core.EntryTypePutEvent, 1)

		err = a.MoveToDLQ(mem)
		require.Error(t, err)
	})
}

// Test processing immutable memtables with retries and DLQ behavior using adapter's FlushMemtableToL0
func Test_ProcessImmutableUsingAdapter(t *testing.T) {
	t.Run("Success_FirstTry", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		mem := memtable.NewMemtable(1024, a.clk)
		metricID, _ := a.stringStore.GetOrCreateID("metric.test")
		key := core.EncodeTSDBKey(metricID, nil, 12345)
		mem.Put(key, []byte("v"), core.EntryTypePutEvent, 1)

		// Attempt flush: should succeed on first try
		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		require.NoError(t, err)

		// ensure SSTable exists
		files, err := os.ReadDir(filepath.Join(dir, "sstables"))
		require.NoError(t, err)
		require.Greater(t, len(files), 0)
	})

	t.Run("Success_AfterOneRetry", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		// Fail once, then succeed
		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(1)

		mem := memtable.NewMemtable(1024, a.clk)
		metricID, _ := a.stringStore.GetOrCreateID("metric.retry")
		key := core.EncodeTSDBKey(metricID, nil, 67890)
		mem.Put(key, []byte("v"), core.EntryTypePutEvent, 1)

		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		require.NoError(t, err)
		require.Equal(t, 1, mem.FlushRetries)
	})

	t.Run("Failure_MovesToDLQ", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		// Fail max times
		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(int32(localMaxFlushRetries))

		mem := memtable.NewMemtable(1024, a.clk)
		metricID, _ := a.stringStore.GetOrCreateID("metric.dlq")
		key := core.EncodeTSDBKey(metricID, nil, 11111)
		mem.Put(key, []byte("v"), core.EntryTypePutEvent, 1)

		err = processImmutableUsingAdapter(a, mem, localMaxFlushRetries)
		// Should return error after exhausting retries
		require.Error(t, err)
		require.Equal(t, localMaxFlushRetries, mem.FlushRetries)

		// Explicitly move to DLQ as the legacy process would
		require.NoError(t, a.MoveToDLQ(mem))
		files, err := os.ReadDir(a.GetDLQDir())
		require.NoError(t, err)
		require.Len(t, files, 1)
	})

	t.Run("Failure_RequeuedOnShutdown", func(t *testing.T) {
		dir := t.TempDir()
		e, err := NewEngine2(dir)
		require.NoError(t, err)
		a := NewEngine2AdapterWithHooks(e, nil)
		require.NoError(t, a.Start())
		defer a.Close()

		a.TestingOnlyFailFlushCount = new(atomic.Int32)
		a.TestingOnlyFailFlushCount.Store(5) // fail more than max

		mem := memtable.NewMemtable(1024, a.clk)
		metricID, _ := a.stringStore.GetOrCreateID("metric.shutdown")
		key := core.EncodeTSDBKey(metricID, nil, 22222)
		mem.Put(key, []byte("v"), core.EntryTypePutEvent, 1)

		// Simulate background processing with shutdown requeue
		var requeue []*memtable.Memtable
		shutdownCh := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// simple loop that retries and respects shutdown signal
			for {
				select {
				default:
					err := a.FlushMemtableToL0(mem, context.Background())
					if err == nil {
						return
					}
					mem.FlushRetries++
					if mem.FlushRetries >= localMaxFlushRetries {
						// would move to DLQ in real engine; we stop here
						return
					}
					// continue retrying (no delay for test)
				case <-shutdownCh:
					// requeue at front
					requeue = append([]*memtable.Memtable{mem}, requeue...)
					return
				}
			}
		}()

		// Wait a little to ensure first failure occurs, then signal shutdown
		time.Sleep(10 * time.Millisecond)
		close(shutdownCh)
		wg.Wait()

		require.Len(t, requeue, 1)
		require.Equal(t, mem, requeue[0])
	})
}

// processImmutableUsingAdapter attempts to flush the memtable using the adapter
// and performs retries up to maxRetries. It increments mem.FlushRetries on each failure.
func processImmutableUsingAdapter(a *Engine2Adapter, mem *memtable.Memtable, maxRetries int) error {
	for {
		if err := a.FlushMemtableToL0(mem, context.Background()); err == nil {
			return nil
		}
		mem.FlushRetries++
		if mem.FlushRetries >= maxRetries {
			return fmt.Errorf("failed after max retries")
		}
	}
}
