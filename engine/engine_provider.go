package engine

import (
	"context"
	"log/slog"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexuscore/utils/clock"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// This file contains the implementation of the snapshot.EngineProvider interface
// for the storageEngine struct. This acts as a bridge between the snapshot
// manager and the engine's internal state, decoupling the two packages.

// --- State & Config Getters ---

func (e *storageEngine) GetClock() clock.Clock {
	return e.clock
}

func (e *storageEngine) GetLogger() *slog.Logger {
	return e.logger
}

func (e *storageEngine) GetTracer() trace.Tracer {
	return e.tracer
}

func (e *storageEngine) GetLevelsManager() levels.Manager {
	return e.levelsManager
}

func (e *storageEngine) GetTagIndexManager() indexer.TagIndexManagerInterface {
	return e.tagIndexManager
}

func (e *storageEngine) GetStringStore() indexer.StringStoreInterface {
	return e.stringStore
}

func (e *storageEngine) GetPrivateStringStore() internal.PrivateManagerStore {
	ss, ok := e.stringStore.(internal.PrivateManagerStore)
	if !ok {
		panic("stringStore is not a PrivateManagerStore")
	}
	return ss
}

func (e *storageEngine) GetSeriesIDStore() indexer.SeriesIDStoreInterface {
	return e.seriesIDStore
}

func (e *storageEngine) GetPrivateSeriesIDStore() internal.PrivateManagerStore {
	ss, ok := e.seriesIDStore.(internal.PrivateManagerStore)
	if !ok {
		panic("seriesIDStore is not a PrivateManagerStore")
	}
	return ss
}

func (e *storageEngine) GetSSTableCompressionType() string {
	return e.opts.SSTableCompressor.Type().String()
}

func (e *storageEngine) GetSequenceNumber() uint64 {
	return e.sequenceNumber.Load()
}

// --- Locking & State Manipulation ---

func (e *storageEngine) Lock() {
	e.mu.Lock()
}

func (e *storageEngine) Unlock() {
	e.mu.Unlock()
}

func (e *storageEngine) GetMemtablesForFlush() (memtables []*memtable.Memtable, newMemtable *memtable.Memtable) {
	// NOTE: This method MUST be called while holding e.mu.Lock().
	// The snapshot manager is responsible for acquiring and releasing the lock.
	memtablesToFlush := make([]*memtable.Memtable, 0, len(e.immutableMemtables)+1)
	memtablesToFlush = append(memtablesToFlush, e.immutableMemtables...)
	if e.mutableMemtable != nil && e.mutableMemtable.Size() > 0 {
		memtablesToFlush = append(memtablesToFlush, e.mutableMemtable)
	}

	// Reset the engine's memtables so it can continue accepting writes.
	e.immutableMemtables = make([]*memtable.Memtable, 0)
	newMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
	e.mutableMemtable = newMemtable

	return memtablesToFlush, newMemtable
}

func (e *storageEngine) FlushMemtableToL0(memToFlush *memtable.Memtable, parentCtx context.Context) error {
	// This method is specifically for the snapshot process to synchronously flush memtables.
	if memToFlush == nil || memToFlush.Size() == 0 {
		return nil
	}

	_, span := e.tracer.Start(parentCtx, "EngineProvider.FlushMemtableToL0")
	defer span.End()
	span.SetAttributes(attribute.Int64("memtable.size_bytes", memToFlush.Size()))

	flushedSST, err := e._flushMemtableToL0SSTable(memToFlush, parentCtx)
	if err != nil {
		return err // Error is already wrapped and traced by the helper
	}

	e.levelsManager.AddL0Table(flushedSST)
	e.logger.Info("Memtable explicitly flushed to L0 for snapshot.", "sstable_id", flushedSST.ID(), "path", flushedSST.FilePath())
	return nil
}

// --- State Access ---

func (e *storageEngine) GetDeletedSeries() map[string]uint64 {
	// NOTE: The snapshot manager holds the main engine lock (e.mu) when calling this.
	// However, `deletedSeries` has its own mutex. To ensure thread safety against
	// concurrent delete operations, we must lock its specific mutex.
	e.deletedSeriesMu.RLock()
	defer e.deletedSeriesMu.RUnlock()

	// Return a copy to prevent race conditions if the caller uses it after the lock is released.
	copied := make(map[string]uint64, len(e.deletedSeries))
	for k, v := range e.deletedSeries {
		copied[k] = v
	}
	return copied
}

func (e *storageEngine) GetRangeTombstones() map[string][]core.RangeTombstone {
	// NOTE: See comment in GetDeletedSeries. We lock the specific mutex for safety.
	e.rangeTombstonesMu.RLock()
	defer e.rangeTombstonesMu.RUnlock()

	// Return a deep copy to prevent race conditions.
	copied := make(map[string][]core.RangeTombstone, len(e.rangeTombstones))
	for k, v := range e.rangeTombstones {
		// Also copy the inner slice
		innerCopy := make([]core.RangeTombstone, len(v))
		copy(innerCopy, v)
		copied[k] = innerCopy
	}
	return copied
}
