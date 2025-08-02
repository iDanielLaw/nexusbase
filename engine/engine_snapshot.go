package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/memtable"
	"go.opentelemetry.io/otel/attribute"
)

// flushMemtableToL0 is a helper to synchronously flush a specific memtable to a new L0 SSTable.
// It creates the SSTable, loads it, and adds it to the levels manager.
func (e *storageEngine) flushMemtableToL0(memToFlush *memtable.Memtable, parentCtx context.Context) error {
	if memToFlush == nil || memToFlush.Size() == 0 {
		return nil
	}

	_, span := e.tracer.Start(parentCtx, "StorageEngine.flushMemtableToL0")
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

// CreateSnapshot creates a snapshot of the current database state.
// This implementation takes control of flushing memtables to ensure a consistent state.
func (e *storageEngine) CreateSnapshot(snapshotDir string) (err error) {
	if err := e.checkStarted(); err != nil {
		return err
	}
	ctx, span := e.tracer.Start(context.Background(), "StorageEngine.CreateSnapshot")
	defer span.End()
	span.SetAttributes(attribute.String("snapshot.dir", snapshotDir))

	// --- Pre-Snapshot Hook ---
	preSnapshotPayload := hooks.PreCreateSnapshotPayload{SnapshotDir: snapshotDir}
	if hookErr := e.hookManager.Trigger(ctx, hooks.NewPreCreateSnapshotEvent(preSnapshotPayload)); hookErr != nil {
		e.logger.Info("CreateSnapshot operation cancelled by PreCreateSnapshot hook", "error", hookErr)
		return fmt.Errorf("operation cancelled by pre-hook: %w", hookErr)
	}

	// 1. Prepare snapshot directory: ensure it's clean.
	if _, statErr := os.Stat(snapshotDir); !os.IsNotExist(statErr) {
		if removeErr := os.RemoveAll(snapshotDir); removeErr != nil {
			return fmt.Errorf("failed to clean existing snapshot directory %s: %w", snapshotDir, removeErr)
		}
	}
	if mkdirErr := os.MkdirAll(snapshotDir, 0755); mkdirErr != nil {
		return fmt.Errorf("failed to create snapshot directory %s: %w", snapshotDir, mkdirErr)
	}

	concreteStringStore, ok := e.stringStore.(internal.PrivateManagerStore)
	if !ok {
		return fmt.Errorf("string store is nil")
	}

	concreteSeriesIDStore, ok := e.seriesIDStore.(internal.PrivateManagerStore)
	if !ok {
		return fmt.Errorf("series id store is nil or does not implement private interface")
	}

	defer func() {
		if err != nil {
			e.logger.Warn("Snapshot creation failed, cleaning up snapshot directory.", "snapshot_dir", snapshotDir, "error", err)
			os.RemoveAll(snapshotDir)
		}
	}()

	e.logger.Info("Starting to create snapshot.", "snapshot_dir", snapshotDir)

	// 2. Acquire lock to get a consistent view of memtables and other state.
	e.mu.Lock()

	// Collect all memtables (mutable and immutable) to be flushed for the snapshot.
	memtablesToFlush := make([]*memtable.Memtable, 0, len(e.immutableMemtables)+1)
	memtablesToFlush = append(memtablesToFlush, e.immutableMemtables...)
	if e.mutableMemtable != nil && e.mutableMemtable.Size() > 0 {
		memtablesToFlush = append(memtablesToFlush, e.mutableMemtable)
	}

	// Reset the engine's memtables so it can continue accepting writes.
	e.immutableMemtables = make([]*memtable.Memtable, 0)
	e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock) // Reset mutable memtable to a new empty one

	// Get a consistent view of other state under the lock.
	currentSeqNum := e.sequenceNumber.Load()

	e.deletedSeriesMu.RLock()
	deletedSeriesToSave := e.deletedSeries
	e.deletedSeriesMu.RUnlock()

	e.rangeTombstonesMu.RLock()
	rangeTombstonesToSave := e.rangeTombstones
	e.rangeTombstonesMu.RUnlock()

	// Release the main engine lock before performing I/O.
	e.mu.Unlock()

	// 3. Synchronously flush all collected memtables to L0.
	// This is the key part to prevent race conditions with the background flush loop.
	if len(memtablesToFlush) > 0 {
		e.logger.Info("Snapshot: Flushing all in-memory data.", "memtable_count", len(memtablesToFlush))
		for _, mem := range memtablesToFlush {
			if flushErr := e.flushMemtableToL0(mem, ctx); flushErr != nil {
				return fmt.Errorf("failed to flush memtable (size: %d) during snapshot creation: %w", mem.Size(), flushErr)
			}
		}
	}

	// 4. Get the final, consistent list of SSTables AFTER the synchronous flush.
	levelStates, unlockFunc := e.levelsManager.GetSSTablesForRead()
	defer unlockFunc() // Defer unlock to ensure it's called even on error.

	e.logger.Debug("State of levels before creating snapshot manifest")
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		tableIDs := make([]uint64, len(tablesInLevel))
		for i, t := range tablesInLevel {
			tableIDs[i] = t.ID()
		}
		e.logger.Debug("Snapshot manifest source", "level", levelNum, "tables", tableIDs)
	}

	// 5. Create Manifest and copy/link all necessary files.
	manifest := core.SnapshotManifest{
		SequenceNumber:     currentSeqNum,
		Levels:             make([]core.SnapshotLevelManifest, 0, e.levelsManager.MaxLevels()),
		SSTableCompression: e.opts.SSTableCompressor.Type().String(),
	}

	// Copy SSTables
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelNum, Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			baseFileName := filepath.Base(table.FilePath())
			// The destination path within the snapshot directory
			destPath := filepath.Join(snapshotDir, "sst", baseFileName)
			// Ensure the 'sst' subdirectory exists in the snapshot
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return fmt.Errorf("failed to create sst subdirectory in snapshot: %w", err)
			}
			if copyErr := CopyFile(table.FilePath(), destPath); copyErr != nil {
				return fmt.Errorf("failed to copy SSTable %s to %s for snapshot: %w", table.FilePath(), destPath, copyErr)
			}
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
				ID:       table.ID(),
				FileName: filepath.Join("sst", baseFileName), // Store relative path
				MinKey:   table.MinKey(),
				MaxKey:   table.MaxKey(),
			})
		}
		if len(levelManifest.Tables) > 0 {
			manifest.Levels = append(manifest.Levels, levelManifest)
		}
	}

	// 5. Create a snapshot of the Tag Index Manager state.
	// It will create its own subdirectory and manifest within the main snapshot directory.
	indexSnapshotDir := filepath.Join(snapshotDir, "index")
	if err := os.MkdirAll(indexSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create subdirectory for index snapshot: %w", err)
	}
	if err := e.tagIndexManager.CreateSnapshot(indexSnapshotDir); err != nil {
		return fmt.Errorf("failed to create tag index snapshot: %w", err)
	}

	// 6. Serialize and save auxiliary state files.
	if len(deletedSeriesToSave) > 0 {
		manifest.DeletedSeriesFile = "deleted_series.json"
		if err := saveJSON(deletedSeriesToSave, filepath.Join(snapshotDir, manifest.DeletedSeriesFile)); err != nil {
			return fmt.Errorf("failed to save deleted_series for snapshot: %w", err)
		}
	}
	if len(rangeTombstonesToSave) > 0 {
		manifest.RangeTombstonesFile = "range_tombstones.json"
		if err := saveJSON(rangeTombstonesToSave, filepath.Join(snapshotDir, manifest.RangeTombstonesFile)); err != nil {
			return fmt.Errorf("failed to save range_tombstones for snapshot: %w", err)
		}
	}

	// 7. Copy mapping and WAL files.
	if err := copyAuxiliaryFile(concreteStringStore.GetLogFilePath(), indexer.StringMappingLogName, snapshotDir, &manifest.StringMappingFile, e.logger); err != nil {
		return err
	}
	if err := copyAuxiliaryFile(concreteSeriesIDStore.GetLogFilePath(), indexer.SeriesMappingLogName, snapshotDir, &manifest.SeriesMappingFile, e.logger); err != nil {
		return err
	}
	// Copy WAL directory instead of a single file
	srcWALDir := e.wal.Path()
	if _, statErr := os.Stat(srcWALDir); !os.IsNotExist(statErr) {
		destWALDirName := "wal" // Standard name for the WAL directory in snapshots
		destWALDir := filepath.Join(snapshotDir, destWALDirName)
		if err := os.MkdirAll(destWALDir, 0755); err != nil {
			return fmt.Errorf("failed to create wal directory in snapshot: %w", err)
		}
		if err := copyDirectoryContents(srcWALDir, destWALDir); err != nil {
			return fmt.Errorf("failed to copy WAL directory to snapshot: %w", err)
		}
		manifest.WALFile = destWALDirName // Store the directory name, e.g., "wal"
		e.logger.Info("Copied WAL directory to snapshot.", "source", srcWALDir, "destination", destWALDir)
	}

	// 8. Write the final manifest and CURRENT file.
	uniqueManifestFileName := fmt.Sprintf("%s_%d.bin", MANIFEST_FILE_PREFIX, e.clock.Now().UnixNano())
	manifestPath := filepath.Join(snapshotDir, uniqueManifestFileName)
	manifestFile, createErr := os.Create(manifestPath)
	if createErr != nil {
		return fmt.Errorf("failed to create snapshot manifest file %s: %w", manifestPath, createErr)
	}
	if writeErr := writeManifestBinary(manifestFile, &manifest); writeErr != nil {
		manifestFile.Close() // Best effort
		return fmt.Errorf("failed to write binary snapshot manifest: %w", writeErr)
	}
	manifestFile.Close()

	if err := os.WriteFile(filepath.Join(snapshotDir, CURRENT_FILE_NAME), []byte(uniqueManifestFileName), 0644); err != nil {
		return fmt.Errorf("failed to write CURRENT file to snapshot directory: %w", err)
	}

	// --- Post-Snapshot Hook ---
	postSnapshotPayload := hooks.PostCreateSnapshotPayload{
		SnapshotDir:  snapshotDir,
		ManifestPath: manifestPath,
	}
	e.hookManager.Trigger(ctx, hooks.NewPostCreateSnapshotEvent(postSnapshotPayload))

	e.logger.Info("Snapshot created successfully.", "snapshot_dir", snapshotDir, "manifest_file", uniqueManifestFileName)
	return nil
}

// saveJSON is a helper to marshal a struct to JSON and write it to a file.
func saveJSON(v interface{}, path string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// copyAuxiliaryFile is a helper to copy a file and update the manifest field.
func copyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		logger.Warn("Source file does not exist, skipping copy for snapshot.", "path", srcPath)
		return nil
	}
	destPath := filepath.Join(snapshotDir, destFileName)
	if err := CopyFile(srcPath, destPath); err != nil {
		return fmt.Errorf("failed to copy %s to snapshot: %w", destFileName, err)
	}
	*manifestField = destFileName
	logger.Info("Copied auxiliary file to snapshot.", "source", srcPath, "destination", destPath)
	return nil
}

// RestoreFromSnapshot restores the database state from a given snapshot directory.
func RestoreFromSnapshot(opts StorageEngineOptions, snapshotDir string) error {
	restoreLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})).With("component", "RestoreFromSnapshot")
	restoreLogger.Info("Starting restore from snapshot.", "snapshot_dir", snapshotDir, "target_data_dir", opts.DataDir)

	// 1. Safety checks
	if _, err := os.Stat(snapshotDir); os.IsNotExist(err) {
		return fmt.Errorf("snapshot directory %s does not exist", snapshotDir)
	}
	currentFileInSnapshotPath := filepath.Join(snapshotDir, CURRENT_FILE_NAME)
	manifestFileNameBytes, err := os.ReadFile(currentFileInSnapshotPath)
	if err != nil {
		return fmt.Errorf("failed to read CURRENT file from snapshot directory %s: %w", snapshotDir, err)
	}
	manifestFileName := strings.TrimSpace(string(manifestFileNameBytes))
	manifestFilePath := filepath.Join(snapshotDir, manifestFileName)
	if _, err := os.Stat(manifestFilePath); os.IsNotExist(err) {
		return fmt.Errorf("snapshot manifest file %s (from CURRENT) not found in %s", manifestFileName, snapshotDir)
	}

	// 2. Create a temporary directory for restore.
	tempRestoreDir, err := os.MkdirTemp(filepath.Dir(opts.DataDir), filepath.Base(opts.DataDir)+".restore-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary restore directory: %w", err)
	}
	restoreLogger.Info("Created temporary directory for restore.", "temp_dir", tempRestoreDir)
	defer func() {
		if _, statErr := os.Stat(tempRestoreDir); !os.IsNotExist(statErr) {
			restoreLogger.Info("Cleaning up temporary restore directory.", "temp_dir", tempRestoreDir)
			os.RemoveAll(tempRestoreDir)
		}
	}()

	// 3. Read manifest and copy all files listed.
	manifestFile, err := os.Open(manifestFilePath)
	if err != nil {
		return fmt.Errorf("failed to read snapshot manifest file %s: %w", manifestFilePath, err)
	}
	defer manifestFile.Close()
	manifest, err := readManifestBinary(manifestFile)
	if err != nil {
		return fmt.Errorf("failed to read binary snapshot manifest: %w", err)
	}
	restoreLogger.Info("Snapshot manifest loaded.", "sequence_number", manifest.SequenceNumber)

	// Create the index subdirectory in the target
	tempIndexDir := filepath.Join(tempRestoreDir, "index_sst")
	if err := os.MkdirAll(tempIndexDir, 0755); err != nil {
		return fmt.Errorf("failed to create temporary index directory %s: %w", tempIndexDir, err)
	}

	// Restore the tag index by copying its files directly. This avoids the double-directory bug.
	indexSnapshotDir := filepath.Join(snapshotDir, "index")
	if _, err := os.Stat(indexSnapshotDir); !os.IsNotExist(err) {
		// Only copy if the index directory exists in the snapshot
		if err := copyDirectoryContents(indexSnapshotDir, tempIndexDir); err != nil {
			return fmt.Errorf("failed to copy tag index files from snapshot: %w", err)
		}
	}

	// List of files to copy from the manifest
	filesToCopy := []string{manifestFileName, manifest.DeletedSeriesFile, manifest.RangeTombstonesFile, manifest.StringMappingFile, manifest.SeriesMappingFile, CURRENT_FILE_NAME}
	for _, level := range manifest.Levels {
		for _, table := range level.Tables {
			filesToCopy = append(filesToCopy, table.FileName) // This is now "sst/123.sst"
		}
	}

	for _, fileName := range filesToCopy {
		if fileName == "" {
			continue
		}
		srcPath := filepath.Join(snapshotDir, fileName)
		destPath := filepath.Join(tempRestoreDir, fileName)
		// Ensure destination subdirectory exists (e.g., 'sst')
		if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
			return fmt.Errorf("failed to create destination subdirectory %s: %w", filepath.Dir(destPath), err)
		}
		if _, err := os.Stat(srcPath); os.IsNotExist(err) {
			restoreLogger.Warn("File listed in manifest not found in snapshot directory, skipping.", "file", fileName)
			continue
		}
		if err := CopyFile(srcPath, destPath); err != nil {
			return fmt.Errorf("failed to copy file %s to temporary restore directory: %w", srcPath, err)
		}
		restoreLogger.Debug("Restored file to temporary directory.", "file", fileName)
	}

	// Handle WAL directory separately to support the new segmented WAL format
	if manifest.WALFile != "" {
		srcWALPath := filepath.Join(snapshotDir, manifest.WALFile)
		destWALPath := filepath.Join(tempRestoreDir, manifest.WALFile)
		if stat, err := os.Stat(srcWALPath); err == nil {
			if stat.IsDir() {
				if err := os.MkdirAll(destWALPath, 0755); err != nil {
					return fmt.Errorf("failed to create destination WAL directory %s: %w", destWALPath, err)
				}
				// New format: WAL is a directory
				if err := copyDirectoryContents(srcWALPath, destWALPath); err != nil {
					return fmt.Errorf("failed to copy WAL directory from snapshot: %w", err)
				}
			} else {
				// Legacy format: WAL is a single file
				if err := CopyFile(srcWALPath, destWALPath); err != nil {
					return fmt.Errorf("failed to copy legacy WAL file from snapshot: %w", err)
				}
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat source WAL path %s in snapshot: %w", srcWALPath, err)
		}
	}

	// 4. Final swap
	if _, err := os.Stat(opts.DataDir); !os.IsNotExist(err) {
		restoreLogger.Info("Removing original data directory before replacing.", "original_data_dir", opts.DataDir)
		if err := os.RemoveAll(opts.DataDir); err != nil {
			return fmt.Errorf("failed to remove original data directory %s: %w", opts.DataDir, err)
		}
	}
	if err := os.Rename(tempRestoreDir, opts.DataDir); err != nil {
		return fmt.Errorf("failed to rename temporary restore directory %s to %s: %w", tempRestoreDir, opts.DataDir, err)
	}

	restoreLogger.Info("Snapshot restoration complete. Data directory replaced.", "data_dir", opts.DataDir)
	return nil
}

// copyDirectoryContents copies files from src to dst.
// It's a helper for the restore process.
func copyDirectoryContents(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read source directory %s: %w", src, err)
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := os.MkdirAll(dstPath, 0755); err != nil {
				return fmt.Errorf("failed to create destination subdirectory %s: %w", dstPath, err)
			}
			if err := copyDirectoryContents(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := CopyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to copy file from %s to %s: %w", srcPath, dstPath, err)
			}
		}
	}
	return nil
}
