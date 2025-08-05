package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"go.opentelemetry.io/otel/attribute"
)

const (
	CURRENT_FILE_NAME    = "CURRENT"
	MANIFEST_FILE_PREFIX = "MANIFEST"
)

// manager implements the ManagerInterface.
type manager struct {
	provider EngineProvider
}

// NewManager creates a new snapshot manager.
func NewManager(provider EngineProvider) ManagerInterface {
	return &manager{provider: provider}
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
	if _, statErr := os.Stat(snapshotDir); !os.IsNotExist(statErr) {
		if removeErr := os.RemoveAll(snapshotDir); removeErr != nil {
			return fmt.Errorf("failed to clean existing snapshot directory %s: %w", snapshotDir, removeErr)
		}
	}
	if mkdirErr := os.MkdirAll(snapshotDir, 0755); mkdirErr != nil {
		return fmt.Errorf("failed to create snapshot directory: %s", mkdirErr)
	}

	concreteStringStore := p.GetStringStore()
	concreteSeriesIDStore := p.GetSeriesIDStore()

	defer func() {
		if err != nil {
			p.GetLogger().Warn("Snapshot creation failed, cleaning up snapshot directory.", "snapshot_dir", snapshotDir, "error", err)
			os.RemoveAll(snapshotDir)
		}
	}()

	p.GetLogger().Info("Starting to create full snapshot.", "snapshot_dir", snapshotDir)

	// 2. Acquire lock to get a consistent view of memtables and other state.
	p.Lock()
	memtablesToFlush, _ := p.GetMemtablesForFlush()
	currentSeqNum := p.GetSequenceNumber()
	deletedSeriesToSave := p.GetDeletedSeries()
	rangeTombstonesToSave := p.GetRangeTombstones()
	p.Unlock()

	// 3. Synchronously flush all collected memtables to L0.
	if len(memtablesToFlush) > 0 {
		p.GetLogger().Info("Snapshot: Flushing all in-memory data.", "memtable_count", len(memtablesToFlush))
		for _, mem := range memtablesToFlush {
			if flushErr := p.FlushMemtableToL0(mem, ctx); flushErr != nil {
				return fmt.Errorf("failed to flush memtable (size: %d) during snapshot creation: %w", mem.Size(), flushErr)
			}
		}
	}

	// 4. Get the final, consistent list of SSTables AFTER the synchronous flush.
	levelStates, unlockFunc := p.GetLevelsManager().GetSSTablesForRead()
	defer unlockFunc()

	// 5. Create Manifest and copy/link all necessary files.
	manifest := core.SnapshotManifest{
		SequenceNumber:     currentSeqNum,
		Levels:             make([]core.SnapshotLevelManifest, 0, p.GetLevelsManager().MaxLevels()),
		SSTableCompression: p.GetSSTableCompressionType(),
	}

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
			if copyErr := linkOrCopyFile(table.FilePath(), destPath); copyErr != nil {
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

	// 6. Create a snapshot of the Tag Index Manager state.
	indexSnapshotDir := filepath.Join(snapshotDir, "index")
	if err := os.MkdirAll(indexSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create subdirectory for index snapshot: %w", err)
	}
	if err := p.GetTagIndexManager().CreateSnapshot(indexSnapshotDir); err != nil {
		return fmt.Errorf("failed to create tag index snapshot: %w", err)
	}

	// 7. Serialize and save auxiliary state files.
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

	// 8. Copy mapping and WAL files.
	if err := copyAuxiliaryFile(concreteStringStore.GetLogFilePath(), "string_mapping.log", snapshotDir, &manifest.StringMappingFile, p.GetLogger()); err != nil {
		return err
	}
	if err := copyAuxiliaryFile(concreteSeriesIDStore.GetLogFilePath(), "series_mapping.log", snapshotDir, &manifest.SeriesMappingFile, p.GetLogger()); err != nil {
		return err
	}
	srcWALDir := p.GetWALPath()
	if _, statErr := os.Stat(srcWALDir); !os.IsNotExist(statErr) {
		destWALDirName := "wal"
		destWALDir := filepath.Join(snapshotDir, destWALDirName)
		if err := os.MkdirAll(destWALDir, 0755); err != nil {
			return fmt.Errorf("failed to create wal directory in snapshot: %w", err)
		}
		if err := linkOrCopyDirectoryContents(srcWALDir, destWALDir); err != nil {
			return fmt.Errorf("failed to copy WAL directory to snapshot: %w", err)
		}
		manifest.WALFile = destWALDirName
		p.GetLogger().Info("Copied WAL directory to snapshot.", "source", srcWALDir, "destination", destWALDir)
	}

	// 9. Write the final manifest and CURRENT file.
	uniqueManifestFileName := fmt.Sprintf("%s_%d.bin", MANIFEST_FILE_PREFIX, p.GetClock().Now().UnixNano())
	manifestPath := filepath.Join(snapshotDir, uniqueManifestFileName)
	manifestFile, createErr := os.Create(manifestPath)
	if createErr != nil {
		return fmt.Errorf("failed to create snapshot manifest file %s: %w", manifestPath, createErr)
	}
	if writeErr := writeManifestBinary(manifestFile, &manifest); writeErr != nil {
		manifestFile.Close()
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
	p.GetHookManager().Trigger(ctx, hooks.NewPostCreateSnapshotEvent(postSnapshotPayload))

	p.GetLogger().Info("Snapshot created successfully.", "snapshot_dir", snapshotDir, "manifest_file", uniqueManifestFileName)
	return nil
}

func (m *manager) CreateIncremental(ctx context.Context, snapshotsBaseDir string) (err error) {
	// Implementation for CreateIncrementalSnapshot would be moved here.
	// This is a placeholder for brevity.
	return fmt.Errorf("CreateIncremental not implemented yet")
}

func (m *manager) ListSnapshots(snapshotsBaseDir string) ([]Info, error) {
	// Implementation for ListSnapshots would be moved here from the snapshot-util.
	// This is a placeholder for brevity.
	return nil, fmt.Errorf("ListSnapshots not implemented yet")
}

// RestoreFromFull restores the database state from a given snapshot directory.
func RestoreFromFull(opts RestoreOptions, snapshotDir string) error {
	restoreLogger := opts.Logger
	if restoreLogger == nil {
		restoreLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	restoreLogger = restoreLogger.With("component", "RestoreFromSnapshot")
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

	// 3a. Pre-create all necessary subdirectories in the temporary location.
	requiredDirs := []string{
		filepath.Join(tempRestoreDir, "sst"),
		filepath.Join(tempRestoreDir, "index_sst"),
		filepath.Join(tempRestoreDir, "wal"),
	}
	for _, dir := range requiredDirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to pre-create required subdirectory %s: %w", dir, err)
		}
	}

	// Restore the tag index
	srcIndexDir := filepath.Join(snapshotDir, "index")
	destIndexDir := filepath.Join(tempRestoreDir, "index_sst")
	if _, err := os.Stat(srcIndexDir); !os.IsNotExist(err) {
		if err := copyDirectoryContents(srcIndexDir, destIndexDir); err != nil {
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
		if _, err := os.Stat(srcPath); os.IsNotExist(err) {
			restoreLogger.Warn("File listed in manifest not found in snapshot directory, skipping.", "file", fileName)
			continue
		}
		if err := CopyFile(srcPath, destPath); err != nil {
			return fmt.Errorf("failed to copy file %s to temporary restore directory: %w", srcPath, err)
		}
		restoreLogger.Debug("Restored file to temporary directory.", "file", fileName)
	}

	// Handle WAL directory
	if manifest.WALFile != "" {
		srcWALPath := filepath.Join(snapshotDir, manifest.WALFile)
		destWALPath := filepath.Join(tempRestoreDir, manifest.WALFile)
		if stat, err := os.Stat(srcWALPath); err == nil {
			if stat.IsDir() {
				if err := copyDirectoryContents(srcWALPath, destWALPath); err != nil {
					return fmt.Errorf("failed to copy WAL directory from snapshot: %w", err)
				}
			} else {
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

func RestoreFromLatest(opts RestoreOptions, snapshotsBaseDir string) error {
	// Implementation for RestoreFromLatestSnapshot would be moved here.
	// This is a placeholder for brevity.
	return fmt.Errorf("RestoreFromLatest not implemented yet")
}

// --- Helper Functions ---

func saveJSON(v interface{}, path string) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func copyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		logger.Warn("Source file does not exist, skipping copy for snapshot.", "path", srcPath)
		return nil
	}
	destPath := filepath.Join(snapshotDir, destFileName)
	if err := linkOrCopyFile(srcPath, destPath); err != nil {
		return fmt.Errorf("failed to link or copy %s to snapshot: %w", destFileName, err)
	}
	*manifestField = destFileName
	logger.Info("Copied auxiliary file to snapshot.", "source", srcPath, "destination", destPath)
	return nil
}

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

func linkOrCopyFile(src, dst string) error {
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	return CopyFile(src, dst)
}

func linkOrCopyDirectoryContents(src, dst string) error {
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
			if err := linkOrCopyDirectoryContents(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := linkOrCopyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to link or copy file from %s to %s: %w", srcPath, dstPath, err)
			}
		}
	}
	return nil
}

// CopyFile copies a file from src to dst.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

