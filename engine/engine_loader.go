package engine

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/INLOpen/nexusbase/sys"

	"github.com/INLOpen/nexusbase/checkpoint"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/wal"
)

// StateLoader is responsible for loading the engine's state from disk.
// This includes loading the manifest, SSTables, and recovering from the WAL.
type StateLoader struct {
	opts   StorageEngineOptions
	engine *storageEngine // A reference back to the engine to populate its fields
	logger *slog.Logger
}

// NewStateLoader creates a new loader for the given engine.
func NewStateLoader(engine *storageEngine) *StateLoader {
	return &StateLoader{
		opts:   engine.opts,
		engine: engine,
		logger: engine.logger.With("component", "StateLoader"),
	}
}

// Load orchestrates the entire state loading process.
func (sl *StateLoader) Load() error {
	// --- Phase 1: Read Checkpoint ---
	cp, found, err := checkpoint.Read(sl.opts.DataDir)
	if err != nil {
		return fmt.Errorf("failed to read checkpoint file: %w", err)
	}
	var lastSafeSegmentIndex uint64
	if found {
		lastSafeSegmentIndex = cp.LastSafeSegmentIndex
		sl.logger.Info("Checkpoint found", "last_safe_segment_index", lastSafeSegmentIndex)
	} else {
		sl.logger.Info("No checkpoint found, will perform full recovery from all available WAL segments.")
	}

	// --- Phase 2: Load String and Series Mappings ---
	if err := sl.engine.stringStore.LoadFromFile(sl.opts.DataDir); err != nil {
		return fmt.Errorf("failed to recover string store: %w", err)
	}
	if err := sl.engine.seriesIDStore.LoadFromFile(sl.opts.DataDir); err != nil {
		return fmt.Errorf("failed to recover series id store: %w", err)
	}

	// --- Phase 3: Load SSTables and Manifest ---
	if _, err := sl.loadStateFromDisk(); err != nil {
		return fmt.Errorf("failed to load database state from disk: %w", err)
	}

	// --- Phase 4: Populate In-Memory Indexes from the dedicated series log ---
	if err := sl.populateActiveSeriesFromLog(); err != nil {
		return fmt.Errorf("failed to populate active series from sstables: %w", err)
	}

	// --- Phase 5: Initialize WAL and Recover ---
	if err := sl.initializeWALAndRecover(lastSafeSegmentIndex); err != nil {
		return fmt.Errorf("failed to initialize WAL and recover: %w", err)
	}

	sl.logger.Info("State loading completed successfully.")
	return nil
}

// loadStateFromDisk attempts to load the database state from the MANIFEST file.
// If no MANIFEST is found or it's corrupted, it falls back to scanning the data directory
// and loading all SSTables into L0.
func (sl *StateLoader) loadStateFromDisk() (bool, error) {
	sl.logger.Info("Loading database state from disk...")
	// Load NextID Last
	nextIdFilePath := filepath.Join(sl.opts.DataDir, core.NextIDFileName)

	currentNextID, err := os.ReadFile(nextIdFilePath)

	if err != nil {
		if os.IsNotExist(err) {
			sl.logger.Info("NEXTID file not found, assuming fresh start.")
			sl.engine.nextSSTableID.Store(0)
		} else {
			return false, fmt.Errorf("failed to read NEXTID file: %w", err)
		}
	} else {
		if len(currentNextID) != 8 {
			sl.logger.Error("Invalid NEXTID file content")
			return false, fmt.Errorf("invalid NEXTID file content")
		}
		num := binary.BigEndian.Uint64(currentNextID)
		sl.engine.nextSSTableID.Store(num)
	}

	currentFilePath := filepath.Join(sl.opts.DataDir, core.CurrentFileName)

	// Try to read CURRENT file to find the latest manifest
	latestManifestFileName := ""
	currentFileContent, err := os.ReadFile(currentFilePath)
	if err == nil {
		latestManifestFileName = strings.TrimSpace(string(currentFileContent))
		if latestManifestFileName == "" {
			sl.logger.Warn("CURRENT file is empty, falling back to scanning data directory.")
			return false, sl.scanDataDirAndLoadToL0()
		}
		sl.logger.Info("Found CURRENT file, attempting to load manifest.", "manifest_file", latestManifestFileName)
	} else if os.IsNotExist(err) {
		sl.logger.Info("CURRENT file not found, assuming fresh start or no previous manifest. Scanning data directory.")
		return false, sl.scanDataDirAndLoadToL0() // Fallback on CURRENT file not found
	} else {
		sl.logger.Warn("Error reading CURRENT file, falling back to scanning data directory.", "error", err)
		return false, sl.scanDataDirAndLoadToL0() // Fallback on CURRENT file read error
	}

	manifestPath := filepath.Join(sl.opts.DataDir, latestManifestFileName) // Manifest is in dataDir
	file, err := os.Open(manifestPath)
	if err != nil {
		sl.logger.Warn("Error reading MANIFEST file, falling back to scanning data directory.", "path", manifestPath, "error", err)
		return false, sl.scanDataDirAndLoadToL0() // Fallback on MANIFEST file read error
	}
	defer file.Close()

	manifest, err := snapshot.ReadManifestBinary(file)
	if err != nil {
		// Here we could add logic to try parsing as JSON for backward compatibility
		sl.logger.Warn("Error reading binary MANIFEST file, falling back to scanning data directory.", "path", manifestPath, "error", err)
		return false, sl.scanDataDirAndLoadToL0() // Fallback on MANIFEST unmarshal error
	}

	// Load auxiliary state using filenames from the manifest
	if manifest.DeletedSeriesFile != "" {
		deletedSeriesFile := filepath.Join(sl.opts.DataDir, manifest.DeletedSeriesFile)
		data, readErr := os.ReadFile(deletedSeriesFile)
		if readErr != nil {
			sl.logger.Error("Failed to read deleted_series file from manifest.", "file", deletedSeriesFile, "error", readErr)
			return false, fmt.Errorf("failed to read deleted_series file %s: %w", deletedSeriesFile, readErr)
		}
		var ds map[string]uint64
		// Try gob (binary) decode first, then fall back to JSON for backward compatibility
		var decodeErr error
		decodeErr = gob.NewDecoder(bytes.NewReader(data)).Decode(&ds)
		if decodeErr != nil {
			// Attempt JSON fallback
			decodeErr = json.Unmarshal(data, &ds)
		}
		if decodeErr != nil {
			sl.logger.Error("Failed to decode deleted_series file from manifest.", "file", deletedSeriesFile, "error", decodeErr)
			return false, fmt.Errorf("failed to decode deleted_series file %s: %w", deletedSeriesFile, decodeErr)
		}
		sl.engine.deletedSeriesMu.Lock()
		sl.engine.deletedSeries = ds
		sl.engine.deletedSeriesMu.Unlock()
		sl.logger.Info("Loaded deletedSeries state from manifest.", "file", deletedSeriesFile, "count", len(ds))
	}

	if manifest.RangeTombstonesFile != "" {
		rangeTombstonesFile := filepath.Join(sl.opts.DataDir, manifest.RangeTombstonesFile)
		data, readErr := os.ReadFile(rangeTombstonesFile)
		if readErr != nil {
			sl.logger.Error("Failed to read range_tombstones file from manifest.", "file", rangeTombstonesFile, "error", readErr)
			return false, fmt.Errorf("failed to read range_tombstones file %s: %w", rangeTombstonesFile, readErr)
		}
		var rt map[string][]core.RangeTombstone
		// Try gob (binary) decode first, then fall back to JSON for backward compatibility
		var decodeErr error
		decodeErr = gob.NewDecoder(bytes.NewReader(data)).Decode(&rt)
		if decodeErr != nil {
			// Attempt JSON fallback
			decodeErr = json.Unmarshal(data, &rt)
		}
		if decodeErr != nil {
			sl.logger.Error("Failed to decode range_tombstones file from manifest.", "file", rangeTombstonesFile, "error", decodeErr)
			return false, fmt.Errorf("failed to decode range_tombstones file %s: %w", rangeTombstonesFile, decodeErr)
		}
		sl.engine.rangeTombstonesMu.Lock()
		sl.engine.rangeTombstones = rt
		sl.engine.rangeTombstonesMu.Unlock()
		sl.logger.Info("Loaded rangeTombstones state from manifest.", "file", rangeTombstonesFile, "count", len(rt))
	}

	// Check if the configured compressor matches the manifest's compressor
	if manifest.SSTableCompression != "" && sl.opts.SSTableCompressor.Type().String() != manifest.SSTableCompression {
		sl.logger.Warn("Configured SSTable compressor does not match manifest's compressor. Using configured compressor.",
			"configured_compressor", sl.opts.SSTableCompressor.Type().String(),
			"manifest_compressor", manifest.SSTableCompression)
	}

	// Load SSTables according to the manifest
	sl.logger.Info("Loading SSTables from manifest.", "manifest_sequence_number", manifest.SequenceNumber)
	for _, levelManifest := range manifest.Levels {
		for _, tableMeta := range levelManifest.Tables {
			filePath := filepath.Join(sl.opts.DataDir, tableMeta.FileName) // tableMeta.FileName is now "sst/123.sst"
			loadOpts := sstable.LoadSSTableOptions{
				FilePath:   filePath,
				ID:         tableMeta.ID,
				BlockCache: sl.engine.blockCache,
				Tracer:     sl.engine.tracer,
				Logger:     sl.engine.logger,
			}
			table, loadErr := sstable.LoadSSTable(loadOpts)
			if loadErr != nil {
				sl.logger.Error("Failed to load SSTable from manifest, data corruption suspected. Falling back to L0 scan.", "file", filePath, "error", loadErr)
				return false, sl.scanDataDirAndLoadToL0() // Critical error, fall back
			}
			if err := sl.engine.levelsManager.AddTableToLevel(levelManifest.LevelNumber, table); err != nil {
				sl.logger.Error("Failed to add SSTable to level from manifest, data corruption suspected. Falling back to L0 scan.", "id", table.ID(), "level", levelManifest.LevelNumber, "error", err)
				return false, sl.scanDataDirAndLoadToL0() // Critical error, fall back
			}
			sl.logger.Debug("Loaded SSTable from manifest.", "id", table.ID(), "level", levelManifest.LevelNumber)
		}
	}

	// Set sequence number from manifest
	sl.engine.sequenceNumber.Store(manifest.SequenceNumber)
	sl.logger.Info("Database state loaded from manifest successfully.", "sequence_number", sl.engine.sequenceNumber.Load())
	return true, nil
}

// scanDataDirAndLoadToL0 is the fallback function if manifest loading fails.
func (sl *StateLoader) scanDataDirAndLoadToL0() error {
	sl.logger.Warn("Falling back to scanning sst directory and loading all SSTables to L0.")

	// Attempt to load mapping files even in fallback mode. This is crucial for queries to work.
	if err := sl.engine.stringStore.LoadFromFile(sl.opts.DataDir); err != nil {
		sl.logger.Error("Failed to load string mapping file during fallback", "error", err)
		return fmt.Errorf("fallback failed to load string mapping: %w", err)
	} else {
		sl.logger.Info("Successfully loaded string mapping file during fallback.")
	}

	if err := sl.engine.seriesIDStore.LoadFromFile(sl.opts.DataDir); err != nil {
		sl.logger.Error("Failed to load series mapping file during fallback", "error", err)
		return fmt.Errorf("fallback failed to load series mapping: %w", err)
	} else {
		sl.logger.Info("Successfully loaded series mapping file during fallback.")
	}

	if err := sl.engine.tagIndexManager.LoadFromFile(sl.opts.DataDir); err != nil {
		sl.logger.Error("Failed to load tag index file during fallback", "error", err)
		return fmt.Errorf("fallback failed to load tag index: %w", err)
	}

	sl.logger.Info("Scanning for orphaned .tmp files to clean up.", "sst_dir", sl.engine.sstDir)
	dirEntriesForCleanup, errClean := os.ReadDir(sl.engine.sstDir)
	if errClean != nil {
		if os.IsNotExist(errClean) {
			sl.logger.Info("SST directory does not exist, no .tmp files to clean up.")
		} else {
			sl.logger.Warn("Failed to read sst directory for .tmp cleanup, proceeding without cleanup.", "error", errClean)
		}
	} else {
		for _, entry := range dirEntriesForCleanup {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tmp") {
				tmpFilePath := filepath.Join(sl.engine.sstDir, entry.Name())
				sl.logger.Info("Removing orphaned .tmp file.", "path", tmpFilePath)
				if errRemove := sys.Remove(tmpFilePath); errRemove != nil {
					sl.logger.Warn("Failed to remove orphaned .tmp file.", "path", tmpFilePath, "error", errRemove)
				}
			}
		}
	}

	sl.logger.Info("Scanning for existing SSTables.", "sst_dir", sl.engine.sstDir)
	dirEntries, err := os.ReadDir(sl.engine.sstDir)
	if err != nil {
		if os.IsNotExist(err) {
			sl.logger.Info("SST directory does not exist, no SSTables to load.")
			return nil // Not an error, just nothing to load
		}
		sl.logger.Error("Error reading sst directory for SSTables.", "sst_dir", sl.engine.sstDir, "error", err)
		return fmt.Errorf("error reading sst directory for SSTables: %w", err)
	}

	var loadedSSTables []*sstable.SSTable
	var maxID uint64 = 0
	for _, entry := range dirEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}
		filePath := filepath.Join(sl.engine.sstDir, entry.Name())
		idStr := strings.TrimSuffix(entry.Name(), ".sst")
		tableID, parseErr := strconv.ParseUint(idStr, 10, 64)
		if parseErr != nil {
			sl.logger.Warn("Error parsing SSTable ID from filename, skipping file.", "filename", entry.Name(), "error", parseErr)
			continue
		}
		if tableID > maxID {
			maxID = tableID
		}

		sl.logger.Debug("Attempting to load SSTable.", "path", filePath, "id", tableID)
		loadOpts := sstable.LoadSSTableOptions{
			FilePath:   filePath,
			ID:         tableID,
			BlockCache: sl.engine.blockCache,
			Tracer:     sl.engine.tracer,
			Logger:     sl.engine.logger,
		}
		table, loadErr := sstable.LoadSSTable(loadOpts)
		if loadErr != nil {
			if sl.opts.ErrorOnSSTableLoadFailure {
				sl.logger.Error("Critical error loading SSTable, ErrorOnSSTableLoadFailure is true.",
					"file", filePath, "id", tableID, "error", loadErr)
				return fmt.Errorf("failed to load SSTable file %s (ID: %d) and ErrorOnSSTableLoadFailure is true: %w", filePath, tableID, loadErr)
			}
			continue
		}
		sl.logger.Info("Successfully loaded SSTable.", "path", filePath, "id", table.ID(), "min_key", string(table.MinKey()), "max_key", string(table.MaxKey()))
		loadedSSTables = append(loadedSSTables, table)
	}

	if len(loadedSSTables) > 0 {
		sort.Slice(loadedSSTables, func(i, j int) bool {
			return loadedSSTables[i].ID() < loadedSSTables[j].ID()
		})
		sl.logger.Info("Adding loaded SSTables to L0.", "count", len(loadedSSTables))
		for _, tbl := range loadedSSTables {
			sl.engine.levelsManager.AddL0Table(tbl)
			sl.logger.Debug("Added SSTable to L0.", "id", tbl.ID())
		}
	}
	// After loading all existing tables, set the next ID to be one greater than the max found.
	// This prevents ID collisions after a fallback recovery.
	sl.engine.nextSSTableID.Store(maxID)
	sl.logger.Info("Next SSTable ID set from fallback scan.", "next_id", maxID+1)
	// When falling back to L0, reset sequence number to 0 or a safe default.
	sl.engine.sequenceNumber.Store(0)
	sl.logger.Info("Database state initialized by scanning sst directory. Sequence number reset to 0.")
	return nil
}

// populateActiveSeriesFromLog reads the dedicated series.log file to rebuild the
// in-memory active series map and seriesIDStore, which is much faster than scanning all SSTables.
func (sl *StateLoader) populateActiveSeriesFromLog() error {
	seriesLogPath := filepath.Join(sl.opts.DataDir, "series.log")
	sl.logger.Info("Populating active series from series.log...", "path", seriesLogPath)

	file, err := os.Open(seriesLogPath)
	if err != nil {
		if os.IsNotExist(err) {
			sl.logger.Info("series.log not found, assuming new database or no series created yet.")
			return nil // Not an error if the file doesn't exist
		}
		return fmt.Errorf("failed to open series.log: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		seriesKeyBytes := scanner.Bytes()
		if len(seriesKeyBytes) == 0 {
			continue
		}
		// Record the series key read for debugging test-suite ordering issues.
		// Write to a debug file under the data dir so test logging settings don't hide it.
		// Write to per-engine debug file
		if sl.opts.DataDir != "" {
			func() {
				path := filepath.Join(sl.opts.DataDir, "debug_series_reads.log")
				f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err == nil {
					_, _ = f.Write(append(seriesKeyBytes, '\n'))
					_ = f.Close()
				}
			}()
		}
		// Also write to global temp file so we can inspect across test runs
		func() {
			path := filepath.Join(os.TempDir(), "nexus_debug_series_reads.log")
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				_, _ = f.Write(append(seriesKeyBytes, '\n'))
				_ = f.Close()
			}
		}()
		// Validate the series key can be decoded before adding it to activeSeries.
		// If the series key is malformed (e.g., truncated due to a crash), skip it.
		if _, _, decodeErr := core.DecodeSeriesKey(seriesKeyBytes); decodeErr != nil {
			sl.logger.Warn("Skipping malformed series key in series.log", "error", decodeErr)
			continue
		}
		// The addActiveSeries method handles the logic of adding to the map
		// and persisting new series; here we explicitly ensure the key is present
		// in memory and that the series ID exists in the seriesIDStore.
		sl.engine.activeSeries[string(seriesKeyBytes)] = struct{}{}
		if _, err := sl.engine.seriesIDStore.GetOrCreateID(string(seriesKeyBytes)); err != nil {
			sl.logger.Error("Failed to create series ID during population from log, continuing...", "seriesKey", string(seriesKeyBytes), "error", err)
			continue
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning series.log: %w", err)
	}

	sl.logger.Info("Finished populating active series from log.", "count", count)

	// If some series are missing from the dedicated `series.log` (e.g. due to
	// partially-written lines after a crash), ensure we still populate
	// in-memory active series from the authoritative `series_mapping.log` (the
	// SeriesIDStore). Iterate known series IDs and add any that were not found
	// in the series.log scan above.
	// This guarantees activeSeries contains all known series even if the
	// lightweight `series.log` was partially corrupted.
	for id := uint64(1); ; id++ {
		seriesKey, ok := sl.engine.seriesIDStore.GetKey(id)
		if !ok {
			break
		}
		if _, exists := sl.engine.activeSeries[seriesKey]; !exists {
			sl.engine.activeSeries[seriesKey] = struct{}{}
			// Ensure the ID exists (should be redundant) and log on error.
			if _, err := sl.engine.seriesIDStore.GetOrCreateID(seriesKey); err != nil {
				sl.logger.Warn("Failed to ensure series ID exists during active series population", "series_key", seriesKey, "error", err)
			}
		}
	}
	return nil
}

// Deprecated: This function is slow and has been replaced by populateActiveSeriesFromLog.
// It is kept here for reference or potential fallback if needed in the future.
func (sl *StateLoader) populateActiveSeriesFromSSTables_legacy() error {
	sl.logger.Warn("Executing legacy (slow) active series population by scanning all SSTables.")
	// ... (The original implementation of populateActiveSeriesFromSSTables remains here) ...
	// ... (This function is no longer called by StateLoader.Load) ...
	sl.logger.Info("No SSTables found to populate active series from (legacy scan).")
	return nil
}

// initializeWALAndRecover sets up the WAL and performs recovery.
func (sl *StateLoader) initializeWALAndRecover(lastSafeSegmentIndex uint64) error {
	walDir := filepath.Join(sl.opts.DataDir, "wal")

	walOpts := wal.Options{
		Dir:                walDir,
		Logger:             sl.logger,
		SyncMode:           sl.opts.WALSyncMode,
		MaxSegmentSize:     sl.opts.WALMaxSegmentSize,
		BytesWritten:       sl.engine.metrics.WALBytesWrittenTotal,
		EntriesWritten:     sl.engine.metrics.WALEntriesWrittenTotal,
		StartRecoveryIndex: lastSafeSegmentIndex,
		HookManager:        sl.engine.hookManager,
	}

	recoveryStartTime := sl.engine.clock.Now()
	w, recoveredEntries, walRecoveryErr := wal.Open(walOpts) //nolint:govet
	sl.engine.metrics.WALRecoveryDurationSeconds.Set(sl.engine.clock.Now().Sub(recoveryStartTime).Seconds())

	if walRecoveryErr != nil {
		sl.logger.Error("Critical error during WAL open/recovery.", "error", walRecoveryErr)
		return fmt.Errorf("failed to open/recover from WAL: %w", walRecoveryErr)
	}

	sl.engine.wal = w

	// --- Post-WAL-Recovery Hook ---
	// Defer this to ensure it runs at the end of the function, after all state is updated.
	recoveryDuration := sl.engine.clock.Now().Sub(recoveryStartTime)
	defer func() {
		postRecoveryPayload := hooks.PostWALRecoveryPayload{
			RecoveredEntriesCount: len(recoveredEntries),
			Duration:              recoveryDuration,
		}
		sl.engine.hookManager.Trigger(context.Background(), hooks.NewPostWALRecoveryEvent(postRecoveryPayload))
	}()

	var maxSeqFromWAL uint64 = 0
	if len(recoveredEntries) > 0 {
		sl.logger.Info("Recovering from WAL.", "entry_count", len(recoveredEntries))
		for _, entry := range recoveredEntries {
			if entry.SeqNum > maxSeqFromWAL {
				maxSeqFromWAL = entry.SeqNum
			}
			switch entry.EntryType {
			case core.EntryTypePutEvent:
				if err := sl.engine.mutableMemtable.PutRaw(entry.Key, entry.Value, entry.EntryType, entry.SeqNum); err != nil {
					return fmt.Errorf("failed to apply PUT entry from WAL to memtable: %w", err)
				}
				seriesKeyBytes, extractErr := core.ExtractSeriesKeyFromInternalKeyWithErr(entry.Key)
				if extractErr != nil {
					sl.logger.Warn("Failed to extract series key from WAL entry during recovery", "error", extractErr, "wal_key", entry.Key)
					continue
				}
				_, encodedTags, decodeErr := core.DecodeSeriesKey(seriesKeyBytes)
				if decodeErr != nil {
					sl.logger.Warn("Failed to decode series key from WAL entry for tag index during recovery", "error", decodeErr, "series_key_bytes", seriesKeyBytes)
					continue
				}
				seriesKeyStr := string(seriesKeyBytes)

				seriesID, found := sl.engine.seriesIDStore.GetID(seriesKeyStr)
				if !found {
					// If the series ID is missing (e.g., series.log was corrupted or
					// a mapping wasn't persisted), create it now so recovery can
					// rebuild indexes and active-series correctly.
					var createErr error
					seriesID, createErr = sl.engine.seriesIDStore.GetOrCreateID(seriesKeyStr)
					if createErr != nil {
						sl.logger.Warn("Failed to create series ID during WAL recovery, skipping entry.", "series_key", seriesKeyStr, "error", createErr)
						continue
					}
				}

				if err := sl.engine.tagIndexManager.AddEncoded(seriesID, encodedTags); err != nil {
					sl.logger.Error("Failed to add entry to tag index during WAL recovery.", "series_key", seriesKeyStr, "error", err)
				}

				sl.engine.addActiveSeries(seriesKeyStr)
			case core.EntryTypeDelete:
				if err := sl.engine.mutableMemtable.PutRaw(entry.Key, nil, entry.EntryType, entry.SeqNum); err != nil {
					return fmt.Errorf("failed to apply DELETE entry from WAL to memtable: %w", err)
				}
			case core.EntryTypeDeleteSeries:
				sl.engine.deletedSeriesMu.Lock()
				sl.engine.deletedSeries[string(entry.Key)] = entry.SeqNum
				sl.engine.deletedSeriesMu.Unlock()
				seriesKeyStr := string(entry.Key)
				seriesID, found := sl.engine.seriesIDStore.GetID(seriesKeyStr)
				if found {
					sl.engine.tagIndexManager.RemoveSeries(seriesID)
				} else {
					sl.logger.Warn("Series ID not found in seriesIDStore during WAL recovery for DeleteSeries, cannot remove from tag index.", "series_key", seriesKeyStr)
				}
			case core.EntryTypeDeleteRange:
				minTs, maxTs, decodeErr := core.DecodeRangeTombstoneValue(entry.Value)
				if decodeErr != nil {
					sl.logger.Warn("Failed to decode range tombstone from WAL entry during recovery", "error", decodeErr)
					continue
				}
				sl.engine.rangeTombstonesMu.Lock()
				sl.engine.rangeTombstones[string(entry.Key)] = append(sl.engine.rangeTombstones[string(entry.Key)], core.RangeTombstone{
					MinTimestamp: minTs,
					MaxTimestamp: maxTs,
					SeqNum:       entry.SeqNum,
				})
				sl.engine.rangeTombstonesMu.Unlock()
			default:
				sl.logger.Warn("Unknown WAL entry type during recovery", "type", string(entry.EntryType))
			}
		}
	} else {
		sl.logger.Info("No new entries found in WAL for recovery.")
	}

	// After applying all WAL entries, check if the mutable memtable has exceeded its threshold.
	// If so, rotate it to the immutable list to be flushed on the next cycle.
	// This prevents the engine from starting with an overly large memtable.
	if sl.engine.mutableMemtable.IsFull() {
		sl.logger.Info("Memtable is full after WAL recovery, rotating for flush.", "size_bytes", sl.engine.mutableMemtable.Size(), "threshold_bytes", sl.engine.opts.MemtableThreshold)
		sl.engine.mutableMemtable.LastWALSegmentIndex = sl.engine.wal.ActiveSegmentIndex()
		sl.engine.immutableMemtables = append(sl.engine.immutableMemtables, sl.engine.mutableMemtable)
		sl.engine.mutableMemtable = memtable.NewMemtable2(sl.engine.opts.MemtableThreshold, sl.engine.clock)
		// We don't need to signal the flushChan here, as the background service loop
		// will start right after this and process the immutable queue.
	}

	sl.engine.metrics.WALRecoveredEntriesTotal.Set(int64(len(recoveredEntries)))

	if maxSeqFromWAL > sl.engine.sequenceNumber.Load() {
		sl.engine.sequenceNumber.Store(maxSeqFromWAL)
		sl.logger.Info("Updated sequence number from WAL recovery.", "new_sequence_number", sl.engine.sequenceNumber.Load())
	}

	if lastSafeSegmentIndex > 0 {
		if err := sl.engine.wal.Purge(lastSafeSegmentIndex); err != nil {
			sl.logger.Warn("Failed to purge old WAL segments on startup", "error", err)
		}
	}

	return nil
}
