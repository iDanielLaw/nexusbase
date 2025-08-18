package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/INLOpen/nexusbase/checkpoint"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/memtable"
)

// ServiceManager is responsible for managing all background goroutines
// for the storage engine, such as flushing, compaction, and metadata sync.
type ServiceManager struct {
	engine *storageEngine
	logger *slog.Logger
}

// NewServiceManager creates a new manager for background services.
func NewServiceManager(engine *storageEngine) *ServiceManager {
	return &ServiceManager{
		engine: engine,
		logger: engine.logger.With("component", "ServiceManager"),
	}
}

// Start starts all background services.
func (sm *ServiceManager) Start() {
	sm.logger.Info("Starting background services...")
	sm.engine.startMetrics(context.Background()) // Self-monitoring is a background service
	sm.startFlushLoop()
	sm.startCheckpointLoop() // New: Start the periodic checkpoint loop
	if sm.engine.tagIndexManager != nil {
		sm.engine.tagIndexManager.Start()
	}
	sm.startMetadataSyncLoop()
	if sm.engine.compactor != nil {
		sm.engine.compactor.Start(&sm.engine.wg)
	}
}

// Stop signals all background services to shut down and waits for them to complete.
func (sm *ServiceManager) Stop() {
	sm.logger.Info("Stopping background services...")

	// 1. Signal all background loops to stop
	func() {
		defer func() {
			if r := recover(); r != nil {
				sm.logger.Warn("Shutdown channel was already closed.", "recover_info", r)
			}
		}()
		close(sm.engine.shutdownChan)
	}()

	// 2. Stop the compactor from initiating NEW compactions.
	// This will also wait for any IN-FLIGHT compaction tasks to finish.
	if sm.engine.compactor != nil {
		sm.engine.compactor.Stop()
	}
	sm.engine.tagIndexManagerMu.Lock()
	if sm.engine.tagIndexManager != nil {
		sm.engine.tagIndexManager.Stop()
	}
	sm.engine.tagIndexManagerMu.Unlock()

	// 3. Wait for the main background loops (flush loop, compaction loop) to exit.
	sm.engine.wg.Wait()
	sm.logger.Info("All background services stopped.")
}

// startFlushLoop starts the background goroutine that processes immutable memtables.
func (sm *ServiceManager) startFlushLoop() {
	sm.engine.wg.Add(1)
	go func() {
		defer sm.engine.wg.Done()

		var flushTicker *time.Ticker
		var tickerChan <-chan time.Time

		if sm.engine.opts.MemtableFlushIntervalMs > 0 {
			interval := time.Duration(sm.engine.opts.MemtableFlushIntervalMs) * time.Millisecond
			flushTicker = time.NewTicker(interval)
			tickerChan = flushTicker.C
			sm.logger.Info("Periodic memtable flush enabled.", "interval", interval)
			defer flushTicker.Stop()
		}

		for {
			select {
			case <-sm.engine.flushChan: // Asynchronous flush signal (from Put/Delete)
				// This is a non-blocking, fire-and-forget flush. It processes whatever is in the
				// immutable queue and writes a conservative checkpoint.
				sm.engine.processImmutableMemtablesFunc(true)

			case completionChan := <-sm.engine.forceFlushChan: // Synchronous flush signal (from ForceFlush)
				// 1. Rotate the current mutable memtable to ensure what the user wants to flush is included.
				sm.engine.mu.Lock()
				if sm.engine.mutableMemtable != nil && sm.engine.mutableMemtable.Size() > 0 {
					// This is the correct place to set the LastWALSegmentIndex
					sm.engine.mutableMemtable.LastWALSegmentIndex = sm.engine.wal.ActiveSegmentIndex()
					sm.engine.immutableMemtables = append(sm.engine.immutableMemtables, sm.engine.mutableMemtable)
					sm.engine.mutableMemtable = memtable.NewMemtable(sm.engine.opts.MemtableThreshold, sm.engine.clock)
				}
				sm.engine.mu.Unlock()

				// 2. Process all immutable memtables until the queue is empty.
				for sm.engine.hasImmutableMemtables() {
					sm.engine.processImmutableMemtablesFunc(false)
				}

				// 3. After all data is flushed, rotate the WAL to seal the last segment.
				// This ensures no new data can be written to the segment we are about to checkpoint.
				if err := sm.engine.wal.Rotate(); err != nil {
					completionChan <- fmt.Errorf("failed to rotate WAL during synchronous flush: %w", err)
					continue
				}

				// 4. The last "safe" segment is now the one *before* the new active segment.
				lastSafeSegment := sm.engine.wal.ActiveSegmentIndex() - 1

				// 5. After all flushes, write an authoritative checkpoint for the highest segment that was flushed.
				if lastSafeSegment > 0 {
					cp := core.Checkpoint{LastSafeSegmentIndex: lastSafeSegment}
					if writeErr := checkpoint.Write(sm.engine.opts.DataDir, cp); writeErr != nil {
						completionChan <- writeErr
						continue // continue the select loop
					}
					sm.engine.purgeWALSegments(lastSafeSegment)
				}

				completionChan <- nil // 6. Signal completion.
			case <-tickerChan: // Periodic flush signal
				sm.engine.triggerPeriodicFlushFunc()
			case <-sm.engine.shutdownChan:
				sm.logger.Info("Flush loop shutting down.")
				return
			}
		}
	}()
	sm.logger.Info("Started background flush loop.")
}

// startCheckpointLoop starts a background goroutine to periodically create durable checkpoints.
// A checkpoint involves flushing all in-memory data to SSTables and then recording the state.
func (sm *ServiceManager) startCheckpointLoop() {
	if sm.engine.opts.CheckpointIntervalSeconds <= 0 {
		sm.logger.Info("Periodic checkpointing is disabled.")
		return // Feature disabled
	}

	sm.engine.wg.Add(1)
	go func() {
		defer sm.engine.wg.Done()
		interval := time.Duration(sm.engine.opts.CheckpointIntervalSeconds) * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		sm.logger.Info("Periodic checkpointing enabled.", "interval", interval)

		for {
			select {
			case <-ticker.C:
				sm.logger.Info("Triggering periodic checkpoint...")
				// ForceFlush(true) is a synchronous operation that ensures all data is on disk.
				if err := sm.engine.ForceFlush(context.Background(), true); err != nil && !errors.Is(err, ErrFlushInProgress) {
					sm.logger.Error("Error during periodic checkpoint.", "error", err)
				}
			case <-sm.engine.shutdownChan:
				sm.logger.Info("Checkpoint loop shutting down.")
				return
			}
		}
	}()
}

// startMetadataSyncLoop starts a background goroutine to periodically persist critical metadata.
func (sm *ServiceManager) startMetadataSyncLoop() {
	if sm.engine.opts.MetadataSyncIntervalSeconds <= 0 {
		return // Feature disabled
	}

	sm.engine.wg.Add(1)
	go func() {
		defer sm.engine.wg.Done()
		interval := time.Duration(sm.engine.opts.MetadataSyncIntervalSeconds) * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		sm.logger.Info("Periodic metadata sync enabled.", "interval", interval)

		for {
			select {
			case <-ticker.C:
				sm.engine.syncMetadataFunc()
			case <-sm.engine.shutdownChan:
				sm.logger.Info("Metadata sync loop shutting down.")
				return
			}
		}
	}()
}
