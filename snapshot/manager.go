package snapshot

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"go.opentelemetry.io/otel/attribute"
)

const (
	CURRENT_FILE_NAME    = "CURRENT"
	MANIFEST_FILE_PREFIX = "MANIFEST"
)

// manager implements the ManagerInterface.
type manager struct {
	provider                    EngineProvider
	writeManifestFunc           func(w io.Writer, manifest *core.SnapshotManifest) error
	writeManifestAndCurrentFunc func(snapshotDir string, manifest *core.SnapshotManifest) (string, error)

	wrapper internal.PrivateSnapshotHelper
}

// NewManager creates a new snapshot manager.
func NewManager(provider EngineProvider) ManagerInterface {
	return NewManagerWithTesting(provider, newHelperSnapshot())
}

func NewManagerWithTesting(provider EngineProvider, wrapper internal.PrivateSnapshotHelper) ManagerInterface {
	if wrapper == nil {
		wrapper = newHelperSnapshot()
	}

	m := &manager{
		provider:          provider,
		writeManifestFunc: WriteManifestBinary,
		wrapper:           wrapper,
	}
	m.writeManifestAndCurrentFunc = m.writeManifestAndCurrent
	return m
}

func (m *manager) CreateFull(ctx context.Context, snapshotDir string) (err error) {
	p := m.provider
	if err := p.CheckStarted(); err != nil {
		return err
	}
	ctx, span := p.GetTracer().Start(ctx, "SnapshotManager.CreateFull")
	defer span.End()
	span.SetAttributes(attribute.String("snapshot.dir", snapshotDir))

	// --- Pre-Snapshot Hook ---
	preSnapshotPayload := hooks.PreCreateSnapshotPayload{SnapshotDir: snapshotDir}
	if hookErr := p.GetHookManager().Trigger(ctx, hooks.NewPreCreateSnapshotEvent(preSnapshotPayload)); hookErr != nil {
		p.GetLogger().Info("CreateFull operation cancelled by PreCreateSnapshot hook", "error", hookErr)
		return fmt.Errorf("operation cancelled by pre-hook: %w", hookErr)
	}

	// 1. Prepare snapshot directory: ensure it's clean.
	if _, statErr := m.wrapper.Stat(snapshotDir); !os.IsNotExist(statErr) {
		if removeErr := m.wrapper.RemoveAll(snapshotDir); removeErr != nil {
			return fmt.Errorf("failed to clean existing snapshot directory %s: %w", snapshotDir, removeErr)
		}
	}
	if mkdirErr := m.wrapper.MkdirAll(snapshotDir, 0755); mkdirErr != nil {
		return fmt.Errorf("failed to create snapshot directory: %s", mkdirErr)
	}

	defer func() {
		if err != nil {
			p.GetLogger().Warn("Snapshot creation failed, cleaning up snapshot directory.", "snapshot_dir", snapshotDir, "error", err)
			os.RemoveAll(snapshotDir)
		}
	}()

	p.GetLogger().Info("Starting to create full snapshot.", "snapshot_dir", snapshotDir)

	// 2. Acquire lock to get a consistent view of memtables and other state.

	// 3. Synchronously flush all collected memtables to L0.
	p.Lock()
	memtablesToFlush, _ := p.GetMemtablesForFlush()
	p.Unlock()
	if len(memtablesToFlush) > 0 {
		p.GetLogger().Info("Snapshot: Flushing all in-memory data.", "memtable_count", len(memtablesToFlush))
		for _, mem := range memtablesToFlush {
			if flushErr := p.FlushMemtableToL0(mem, ctx); flushErr != nil {
				return fmt.Errorf("failed to flush memtable (size: %d) during snapshot creation: %w", mem.Size(), flushErr)
			}
		}
	}

	// Re-acquire lock to get final state after flush
	p.Lock()
	currentSeqNum := p.GetSequenceNumber()
	p.Unlock()

	// 4. Get the final, consistent list of SSTables AFTER the synchronous flush.
	levelStates, unlockFunc := p.GetLevelsManager().GetSSTablesForRead()
	defer unlockFunc()

	// Get the latest WAL segment index *after* any potential flushes.
	wal := p.GetWAL()
	lastWALIndex := wal.ActiveSegmentIndex()

	// 5. Create Manifest and copy/link all necessary files.
	manifest := core.SnapshotManifest{
		SequenceNumber:     currentSeqNum,
		Levels:             make([]core.SnapshotLevelManifest, 0, p.GetLevelsManager().MaxLevels()),
		SSTableCompression: p.GetSSTableCompressionType(),
	}
	manifest.Type = core.SnapshotTypeFull
	manifest.CreatedAt = p.GetClock().Now()
	manifest.LastWALSegmentIndex = lastWALIndex

	// Copy SSTables
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelNum, Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			baseFileName := filepath.Base(table.FilePath())
			destPath := filepath.Join(snapshotDir, "sst", baseFileName)
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return fmt.Errorf("failed to create sst subdirectory in snapshot: %w", err)
			}
			if copyErr := m.wrapper.LinkOrCopyFile(table.FilePath(), destPath); copyErr != nil {
				return fmt.Errorf("failed to link or copy SSTable %s to %s for snapshot: %w", table.FilePath(), destPath, copyErr)
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

	// 6. Copy auxiliary files and write manifest
	if err := m.copyAuxiliaryAndWALFiles(snapshotDir, &manifest); err != nil {
		return err
	}
	manifestPath, err := m.writeManifestAndCurrentFunc(snapshotDir, &manifest)
	if err != nil {
		return err
	}

	// --- Post-Snapshot Hook ---
	postSnapshotPayload := hooks.PostCreateSnapshotPayload{
		SnapshotDir:  snapshotDir,
		ManifestPath: manifestPath,
	}
	p.GetHookManager().Trigger(ctx, hooks.NewPostCreateSnapshotEvent(postSnapshotPayload))

	p.GetLogger().Info("Snapshot created successfully.", "snapshot_dir", snapshotDir)
	return nil
}

func (m *manager) CreateIncremental(ctx context.Context, snapshotsBaseDir string) (err error) {
	p := m.provider
	if err := p.CheckStarted(); err != nil {
		return err
	}
	ctx, span := p.GetTracer().Start(ctx, "SnapshotManager.CreateIncremental")
	defer span.End()
	span.SetAttributes(attribute.String("snapshot.base_dir", snapshotsBaseDir))

	// 1. Find the parent snapshot
	parentID, parentPath, err := findLatestSnapshot(snapshotsBaseDir, m.wrapper)
	if err != nil {
		return fmt.Errorf("failed to find latest snapshot: %w", err)
	}
	if parentID == "" {
		return fmt.Errorf("cannot create incremental snapshot: no parent snapshot found in %s. Please create a full snapshot first", snapshotsBaseDir)
	}
	p.GetLogger().Info("Found parent snapshot for incremental creation.", "parent_id", parentID)

	// 2. Read parent manifest
	parentManifest, _, err := readManifestFromDir(parentPath, m.wrapper)
	if err != nil {
		return fmt.Errorf("failed to read parent snapshot manifest from %s: %w", parentPath, err)
	}

	// 3. Check if there are any new changes
	currentSeqNum := p.GetSequenceNumber()
	if currentSeqNum <= parentManifest.SequenceNumber {
		p.GetLogger().Info("Skipping incremental snapshot: no new data since parent.", "current_seq", currentSeqNum, "parent_seq", parentManifest.SequenceNumber)
		return nil
	}

	// 4. Create new snapshot directory
	newSnapshotID := fmt.Sprintf("%d_incr", p.GetClock().Now().UnixNano())
	snapshotDir := filepath.Join(snapshotsBaseDir, newSnapshotID)
	if err := m.wrapper.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create incremental snapshot directory: %w", err)
	}
	defer func() {
		if err != nil {
			p.GetLogger().Warn("Incremental snapshot creation failed, cleaning up.", "snapshot_dir", snapshotDir, "error", err)
			os.RemoveAll(snapshotDir)
		}
	}()

	// 5. Flush memtables (same as full snapshot)
	p.Lock()
	memtablesToFlush, _ := p.GetMemtablesForFlush()
	p.Unlock()
	if len(memtablesToFlush) > 0 {
		for _, mem := range memtablesToFlush {
			if flushErr := p.FlushMemtableToL0(mem, ctx); flushErr != nil {
				return fmt.Errorf("failed to flush memtable during incremental snapshot: %w", flushErr)
			}
		}
	}

	// 6. Identify and copy new/changed files
	p.GetLogger().Info("Creating incremental snapshot.", "id", newSnapshotID, "parent_id", parentID)

	// Build set of parent SSTable file paths for quick lookup
	parentSSTables := make(map[string]struct{})
	for _, level := range parentManifest.Levels {
		for _, table := range level.Tables {
			parentSSTables[table.FileName] = struct{}{}
		}
	}

	// Get current SSTables
	levelStates, unlockFunc := p.GetLevelsManager().GetSSTablesForRead()
	defer unlockFunc()

	newManifest := core.SnapshotManifest{
		Type:                core.SnapshotTypeIncremental,
		ParentID:            parentID,
		CreatedAt:           p.GetClock().Now(),
		SequenceNumber:      currentSeqNum,
		LastWALSegmentIndex: p.GetWAL().ActiveSegmentIndex(),
		Levels:              make([]core.SnapshotLevelManifest, 0),
		SSTableCompression:  p.GetSSTableCompressionType(),
	}

	// Copy only new SSTables
	for _, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelState.LevelNumber(), Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			relativeFileName := filepath.Join("sst", filepath.Base(table.FilePath()))
			if _, exists := parentSSTables[relativeFileName]; !exists {
				destPath := filepath.Join(snapshotDir, relativeFileName)
				if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
					return fmt.Errorf("failed to create sst subdirectory in incremental snapshot: %w", err)
				}
				if copyErr := m.wrapper.LinkOrCopyFile(table.FilePath(), destPath); copyErr != nil {
					return fmt.Errorf("failed to copy new SSTable %s for incremental snapshot: %w", table.FilePath(), copyErr)
				}
				levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
					ID: table.ID(), FileName: relativeFileName, MinKey: table.MinKey(), MaxKey: table.MaxKey(),
				})
			}
		}
		if len(levelManifest.Tables) > 0 {
			newManifest.Levels = append(newManifest.Levels, levelManifest)
		}
	}

	// For simplicity and correctness, we copy the entire current state of WAL, index, and auxiliary files.
	// The "increment" is that we only copied new SSTables. The restore process will overlay this state.
	if err := m.copyAuxiliaryAndWALFiles(snapshotDir, &newManifest); err != nil {
		return err
	}

	// 7. Write the final manifest and CURRENT file for the new snapshot.
	if _, err := m.writeManifestAndCurrentFunc(snapshotDir, &newManifest); err != nil {
		return err
	}
	return nil
}

func (m *manager) ListSnapshots(snapshotsBaseDir string) ([]Info, error) {
	// Implementation for ListSnapshots would be moved here from the snapshot-util.
	// This is a placeholder for brevity.
	return nil, fmt.Errorf("ListSnapshots not implemented yet")
}

// copyAuxiliaryAndWALFiles is a helper for CreateFull and CreateIncremental.
func (m *manager) copyAuxiliaryAndWALFiles(snapshotDir string, manifest *core.SnapshotManifest) error {
	p := m.provider
	concreteStringStore := p.GetPrivateStringStore()

	p.Lock()
	deletedSeriesToSave := p.GetDeletedSeries()
	rangeTombstonesToSave := p.GetRangeTombstones()
	p.Unlock()
	concreteSeriesIDStore := p.GetPrivateSeriesIDStore()

	// Serialize and save auxiliary state files.
	if len(deletedSeriesToSave) > 0 {
		manifest.DeletedSeriesFile = "deleted_series.json"
		if err := m.wrapper.SaveJSON(deletedSeriesToSave, filepath.Join(snapshotDir, manifest.DeletedSeriesFile)); err != nil {
			return fmt.Errorf("failed to save deleted_series for snapshot: %w", err)
		}
	}
	if len(rangeTombstonesToSave) > 0 {
		manifest.RangeTombstonesFile = "range_tombstones.json"
		if err := m.wrapper.SaveJSON(rangeTombstonesToSave, filepath.Join(snapshotDir, manifest.RangeTombstonesFile)); err != nil {
			return fmt.Errorf("failed to save range_tombstones for snapshot: %w", err)
		}
	}

	// Copy mapping files.
	if err := m.wrapper.CopyAuxiliaryFile(concreteStringStore.GetLogFilePath(), "string_mapping.log", snapshotDir, &manifest.StringMappingFile, p.GetLogger()); err != nil {
		return err
	}
	if err := m.wrapper.CopyAuxiliaryFile(concreteSeriesIDStore.GetLogFilePath(), "series_mapping.log", snapshotDir, &manifest.SeriesMappingFile, p.GetLogger()); err != nil {
		return err
	}

	// Create a snapshot of the Tag Index Manager state.
	indexSnapshotDir := filepath.Join(snapshotDir, "index")
	if err := os.MkdirAll(indexSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create subdirectory for index snapshot: %w", err)
	}
	if err := p.GetTagIndexManager().CreateSnapshot(indexSnapshotDir); err != nil {
		return fmt.Errorf("failed to create tag index snapshot: %w", err)
	}

	// Copy WAL directory.
	srcWALDir := p.GetWAL().Path()
	if _, statErr := os.Stat(srcWALDir); !os.IsNotExist(statErr) {
		destWALDirName := "wal"
		destWALDir := filepath.Join(snapshotDir, destWALDirName)
		if err := os.MkdirAll(destWALDir, 0755); err != nil {
			return fmt.Errorf("failed to create wal directory in snapshot: %w", err)
		}
		if err := m.wrapper.LinkOrCopyDirectoryContents(srcWALDir, destWALDir); err != nil {
			return fmt.Errorf("failed to copy WAL directory to snapshot: %w", err)
		}
		manifest.WALFile = destWALDirName
	}
	return nil
}

// writeManifestAndCurrent finalizes a snapshot by writing the manifest and CURRENT file.
func (m *manager) writeManifestAndCurrent(snapshotDir string, manifest *core.SnapshotManifest) (string, error) {
	uniqueManifestFileName := fmt.Sprintf("%s_%d.bin", MANIFEST_FILE_PREFIX, manifest.CreatedAt.UnixNano())
	manifestPath := filepath.Join(snapshotDir, uniqueManifestFileName) // Local variable, not a field
	manifestFile, createErr := m.wrapper.Create(manifestPath)
	if createErr != nil {
		return "", fmt.Errorf("failed to create snapshot manifest file %s: %w", manifestPath, createErr)
	}
	if writeErr := m.writeManifestFunc(manifestFile, manifest); writeErr != nil {
		manifestFile.Close()
		return "", fmt.Errorf("failed to write binary snapshot manifest: %w", writeErr)
	}
	manifestFile.Close()

	if err := m.wrapper.WriteFile(filepath.Join(snapshotDir, CURRENT_FILE_NAME), []byte(uniqueManifestFileName), 0644); err != nil {
		return "", fmt.Errorf("failed to write CURRENT file to snapshot directory: %w", err)
	}
	m.provider.GetLogger().Info("Snapshot manifest created successfully.", "snapshot_dir", snapshotDir, "manifest_file", uniqueManifestFileName)
	return manifestPath, nil
}

// RestoreFromFull restores the database state from a given snapshot directory.
func RestoreFromFull(opts RestoreOptions, snapshotDir string) error {
	restoreLogger := opts.Logger
	if restoreLogger == nil {
		restoreLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if opts.wrapper == nil {
		opts.wrapper = newHelperSnapshot()
	}

	restoreLogger = restoreLogger.With("component", "RestoreFromSnapshot")
	restoreLogger.Info("Starting restore from snapshot.", "snapshot_dir", snapshotDir, "target_data_dir", opts.DataDir)

	// 1. Safety checks
	if _, err := opts.wrapper.Stat(snapshotDir); os.IsNotExist(err) {
		return fmt.Errorf("snapshot directory %s does not exist", snapshotDir)
	}
	manifest, manifestFileName, err := readManifestFromDir(snapshotDir, opts.wrapper)
	if err != nil {
		return fmt.Errorf("could not read snapshot manifest: %w", err)
	}
	// 2. Create a temporary directory for restore.
	tempRestoreDir, err := opts.wrapper.MkdirTemp(filepath.Dir(opts.DataDir), filepath.Base(opts.DataDir)+".restore-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary restore directory: %w", err)
	}
	restoreLogger.Info("Created temporary directory for restore.", "temp_dir", tempRestoreDir)
	defer func() {
		if _, statErr := opts.wrapper.Stat(tempRestoreDir); !os.IsNotExist(statErr) {
			restoreLogger.Info("Cleaning up temporary restore directory.", "temp_dir", tempRestoreDir)
			opts.wrapper.RemoveAll(tempRestoreDir)
		}
	}()

	restoreLogger.Info("Snapshot manifest loaded.", "sequence_number", manifest.SequenceNumber)

	// 3a. Pre-create all necessary subdirectories in the temporary location.
	requiredDirs := []string{
		filepath.Join(tempRestoreDir, "sst"),
		// Use the constant from the indexer package to avoid hardcoding the directory name.
		filepath.Join(tempRestoreDir, indexer.IndexSSTDirName),
		filepath.Join(tempRestoreDir, "wal"),
	}
	for _, dir := range requiredDirs {
		if err := opts.wrapper.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to pre-create required subdirectory %s: %w", dir, err)
		}
	}

	// Restore the tag index
	srcIndexDir := filepath.Join(snapshotDir, indexer.IndexDirName)
	destIndexDir := filepath.Join(tempRestoreDir, "index_sst")
	if _, err := opts.wrapper.Stat(srcIndexDir); !os.IsNotExist(err) {
		if err := opts.wrapper.CopyDirectoryContents(srcIndexDir, destIndexDir); err != nil {
			return fmt.Errorf("failed to copy tag index files from snapshot: %w", err)
		}
	}

	// List of files to copy from the manifest
	filesToCopy := []string{manifestFileName, manifest.DeletedSeriesFile, manifest.RangeTombstonesFile, manifest.StringMappingFile, manifest.SeriesMappingFile, CURRENT_FILE_NAME}
	for _, level := range manifest.Levels {
		for _, table := range level.Tables {
			filesToCopy = append(filesToCopy, table.FileName)
		}
	}

	// 3b. Copy files from the manifest.
	for _, fileName := range filesToCopy {
		if fileName == "" {
			continue
		}
		srcPath := filepath.Join(snapshotDir, fileName)
		destPath := filepath.Join(tempRestoreDir, fileName)
		if _, err := opts.wrapper.Stat(srcPath); os.IsNotExist(err) {
			restoreLogger.Warn("File listed in manifest not found in snapshot directory, skipping.", "file", fileName)
			continue
		}
		if err := opts.wrapper.CopyFile(srcPath, destPath); err != nil {
			return fmt.Errorf("failed to copy file %s to temporary restore directory: %w", srcPath, err)
		}
		restoreLogger.Debug("Restored file to temporary directory.", "file", fileName)
	}

	// Handle WAL directory
	if manifest.WALFile != "" {
		srcWALPath := filepath.Join(snapshotDir, manifest.WALFile)
		destWALPath := filepath.Join(tempRestoreDir, manifest.WALFile)
		if stat, err := opts.wrapper.Stat(srcWALPath); err == nil {
			if stat.IsDir() {
				if err := opts.wrapper.CopyDirectoryContents(srcWALPath, destWALPath); err != nil {
					return fmt.Errorf("failed to copy WAL directory from snapshot: %w", err)
				}
			} else {
				if err := opts.wrapper.CopyFile(srcWALPath, destWALPath); err != nil {
					return fmt.Errorf("failed to copy legacy WAL file from snapshot: %w", err)
				}
			}
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat source WAL path %s in snapshot: %w", srcWALPath, err)
		}
	}

	// 4. Final swap
	if _, err := opts.wrapper.Stat(opts.DataDir); !os.IsNotExist(err) {
		restoreLogger.Info("Removing original data directory before replacing.", "original_data_dir", opts.DataDir)
		if err := opts.wrapper.RemoveAll(opts.DataDir); err != nil {
			return fmt.Errorf("failed to remove original data directory %s: %w", opts.DataDir, err)
		}
	}
	if err := opts.wrapper.Rename(tempRestoreDir, opts.DataDir); err != nil {
		return fmt.Errorf("failed to rename temporary restore directory %s to %s: %w", tempRestoreDir, opts.DataDir, err)
	}

	restoreLogger.Info("Snapshot restoration complete. Data directory replaced.", "data_dir", opts.DataDir)
	return nil
}

func RestoreFromLatest(opts RestoreOptions, snapshotsBaseDir string) error {
	// Implementation for RestoreFromLatestSnapshot would be moved here.
	// This is a placeholder for brevity.
	return fmt.Errorf("RestoreFromLatest not implemented yet")
}

// findLatestSnapshot finds the most recent snapshot directory in a base directory.
// It assumes snapshot directories have sortable names (e.g., based on timestamps).
func findLatestSnapshot(snapshotsBaseDir string, wrapper internal.PrivateSnapshotHelper) (snapshotID, snapshotPath string, err error) {
	entries, err := wrapper.ReadDir(snapshotsBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", nil // No snapshots yet, not an error
		}
		return "", "", fmt.Errorf("failed to read snapshots directory %s: %w", snapshotsBaseDir, err)
	}

	var snapshotIDs []string
	for _, entry := range entries {
		if entry.IsDir() {
			snapshotIDs = append(snapshotIDs, entry.Name())
		}
	}

	if len(snapshotIDs) == 0 {
		return "", "", nil // No snapshot directories found
	}

	sort.Strings(snapshotIDs)
	latestID := snapshotIDs[len(snapshotIDs)-1]
	return latestID, filepath.Join(snapshotsBaseDir, latestID), nil
}

// readManifestFromDir is a helper to read the manifest from a specific snapshot directory.
func readManifestFromDir(dir string, wrapper internal.PrivateSnapshotHelper) (*core.SnapshotManifest, string, error) {
	currentFilePath := filepath.Join(dir, CURRENT_FILE_NAME)
	manifestFileNameBytes, err := wrapper.ReadFile(currentFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read CURRENT file from %s: %w", dir, err)
	}
	manifestFileName := strings.TrimSpace(string(manifestFileNameBytes))
	manifestFilePath := filepath.Join(dir, manifestFileName)

	manifestFile, err := wrapper.Open(manifestFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open manifest file %s: %w", manifestFilePath, err)
	}
	defer manifestFile.Close()

	manifest, err := wrapper.ReadManifestBinary(manifestFile)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read manifest from %s: %w", manifestFilePath, err)
	}
	return manifest, manifestFileName, nil
}
