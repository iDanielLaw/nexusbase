package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

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

// restoreFromFullFunc is a variable to allow mocking RestoreFromFull in tests.
var restoreFromFullFunc = RestoreFromFull

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
			m.wrapper.RemoveAll(snapshotDir)
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

	// 4. Get the final, consistent list of SSTables AFTER the synchronous flush.
	levelStates, unlockFunc := p.GetLevelsManager().GetSSTablesForRead()
	p.Unlock() // Unlock after getting both sequence number and table states
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
			if err := m.wrapper.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
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

// incrementalCreator holds the state for a single incremental snapshot creation.
type incrementalCreator struct {
	m                *manager
	ctx              context.Context
	snapshotsBaseDir string
	parentID         string
	parentPath       string
	parentManifest   *core.SnapshotManifest
	newSnapshotID    string
	snapshotDir      string
	newManifest      *core.SnapshotManifest
}

func (m *manager) ListSnapshots(snapshotsBaseDir string) ([]Info, error) {
	entries, err := m.wrapper.ReadDir(snapshotsBaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []Info{}, nil // No directory means no snapshots, not an error.
		}
		return nil, fmt.Errorf("failed to read snapshots base directory %s: %w", snapshotsBaseDir, err)
	}

	var infos []Info
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		snapshotID := entry.Name()
		snapshotDir := filepath.Join(snapshotsBaseDir, snapshotID)

		manifest, _, err := readManifestFromDir(snapshotDir, m.wrapper)
		if err != nil {
			// If a directory doesn't contain a valid manifest, skip it but log a warning.
			m.provider.GetLogger().Warn("Skipping directory in snapshot listing: not a valid snapshot (could not read manifest)", "dir", snapshotDir, "error", err)
			continue
		}

		// Calculate total size of the snapshot directory (best-effort).
		var totalSize int64
		_ = filepath.WalkDir(snapshotDir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				if info, statErr := d.Info(); statErr == nil {
					totalSize += info.Size()
				}
			}
			return nil // Continue walking regardless of errors.
		})

		infos = append(infos, Info{
			ID:        snapshotID,
			Type:      manifest.Type,
			CreatedAt: manifest.CreatedAt,
			Size:      totalSize,
			ParentID:  manifest.ParentID,
		})
	}

	// Sort by CreatedAt time, which is also implicitly sorting by ID/name.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].CreatedAt.Before(infos[j].CreatedAt)
	})

	return infos, nil
}

// collectAllSSTablesInChain walks the snapshot parent chain starting from a given snapshot path
// and collects a set of all unique SSTable file names present in the entire chain.
func (m *manager) collectAllSSTablesInChain(snapshotBaseDir, startSnapshotPath string) (map[string]struct{}, error) {
	allSSTables := make(map[string]struct{})
	currentSnapshotPath := startSnapshotPath

	for currentSnapshotPath != "" {
		manifest, _, err := readManifestFromDir(currentSnapshotPath, m.wrapper)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest from %s during chain walk: %w", currentSnapshotPath, err)
		}

		for _, level := range manifest.Levels {
			for _, table := range level.Tables {
				// The key is the relative path, e.g., "sst/1.sst"
				allSSTables[table.FileName] = struct{}{}
			}
		}

		if manifest.Type == core.SnapshotTypeFull {
			break // Reached the root of the chain
		}

		if manifest.ParentID == "" {
			// This should ideally not happen for a valid incremental snapshot.
			m.provider.GetLogger().Warn("Incremental snapshot found without a ParentID during chain walk, stopping walk.", "snapshot_path", currentSnapshotPath)
			break
		}

		// Move to the parent
		parentPath := filepath.Join(snapshotBaseDir, manifest.ParentID)
		if _, statErr := m.wrapper.Stat(parentPath); os.IsNotExist(statErr) {
			return nil, fmt.Errorf("parent snapshot %s not found for %s", manifest.ParentID, filepath.Base(currentSnapshotPath))
		}
		currentSnapshotPath = parentPath
	}

	return allSSTables, nil
}

func (m *manager) CreateIncremental(ctx context.Context, snapshotsBaseDir string) (err error) {
	p := m.provider
	if err := p.CheckStarted(); err != nil {
		return err
	}
	ctx, span := p.GetTracer().Start(ctx, "SnapshotManager.CreateIncremental")
	defer span.End()
	span.SetAttributes(attribute.String("snapshot.base_dir", snapshotsBaseDir))

	creator := &incrementalCreator{
		m:                m,
		ctx:              ctx,
		snapshotsBaseDir: snapshotsBaseDir,
	}
	return creator.run()
}

// run orchestrates the entire incremental snapshot creation process.
func (c *incrementalCreator) run() (err error) {
	// Defer cleanup for the new snapshot directory if it gets created.
	defer func() {
		if err != nil && c.snapshotDir != "" {
			c.m.provider.GetLogger().Warn("Incremental snapshot creation failed, cleaning up.", "snapshot_dir", c.snapshotDir, "error", err)
			c.m.wrapper.RemoveAll(c.snapshotDir)
		}
	}()

	// The sequence of operations
	shouldSkip, err := c.findAndValidateParent()
	if err != nil {
		return err
	}
	if shouldSkip {
		return nil // No error, just nothing to do.
	}

	if err := c.prepareNewSnapshotDir(); err != nil {
		return err
	}

	if err := c.flushMemtables(); err != nil {
		return err
	}

	if err := c.buildAndCopyNewFiles(); err != nil {
		return err
	}

	if err := c.finalizeSnapshot(); err != nil {
		return err
	}

	return nil
}

// findAndValidateParent finds the latest snapshot, reads its manifest, and checks if an incremental snapshot is needed.
func (c *incrementalCreator) findAndValidateParent() (shouldSkip bool, err error) {
	p := c.m.provider
	// 1. Find the parent snapshot
	parentID, parentPath, err := findLatestSnapshot(c.snapshotsBaseDir, c.m.wrapper)
	if err != nil {
		return false, fmt.Errorf("failed to find latest snapshot: %w", err)
	}
	if parentID == "" {
		return false, fmt.Errorf("cannot create incremental snapshot: no parent snapshot found in %s. Please create a full snapshot first", c.snapshotsBaseDir)
	}
	c.parentID = parentID
	c.parentPath = parentPath
	p.GetLogger().Info("Found parent snapshot for incremental creation.", "parent_id", c.parentID)

	// 2. Read parent manifest
	parentManifest, _, err := readManifestFromDir(c.parentPath, c.m.wrapper)
	if err != nil {
		return false, fmt.Errorf("failed to read parent snapshot manifest from %s: %w", c.parentPath, err)
	}
	c.parentManifest = parentManifest

	// 3. Check if there are any new changes using the pre-flush sequence number.
	currentSeqNum := p.GetSequenceNumber()
	if currentSeqNum <= c.parentManifest.SequenceNumber {
		p.GetLogger().Info("Skipping incremental snapshot: no new data since parent.", "current_seq", currentSeqNum, "parent_seq", c.parentManifest.SequenceNumber)
		return true, nil
	}

	return false, nil
}

// prepareNewSnapshotDir creates the new directory for the incremental snapshot.
func (c *incrementalCreator) prepareNewSnapshotDir() error {
	p := c.m.provider
	c.newSnapshotID = fmt.Sprintf("%d_incr", p.GetClock().Now().UnixNano())
	c.snapshotDir = filepath.Join(c.snapshotsBaseDir, c.newSnapshotID)
	if err := c.m.wrapper.MkdirAll(c.snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create incremental snapshot directory: %w", err)
	}
	p.GetLogger().Info("Creating incremental snapshot.", "id", c.newSnapshotID, "parent_id", c.parentID)
	return nil
}

// flushMemtables flushes any outstanding memtables to L0.
func (c *incrementalCreator) flushMemtables() error {
	p := c.m.provider
	p.Lock()
	memtablesToFlush, _ := p.GetMemtablesForFlush()
	p.Unlock()
	if len(memtablesToFlush) > 0 {
		for _, mem := range memtablesToFlush {
			if flushErr := p.FlushMemtableToL0(mem, c.ctx); flushErr != nil {
				return fmt.Errorf("failed to flush memtable during incremental snapshot: %w", flushErr)
			}
		}
	}
	return nil
}

// buildAndCopyNewFiles identifies new SSTables, copies them, and builds the new manifest.
func (c *incrementalCreator) buildAndCopyNewFiles() error {
	p := c.m.provider
	// Build set of parent SSTable file paths for quick lookup by walking the entire chain.
	parentSSTables, err := c.m.collectAllSSTablesInChain(c.snapshotsBaseDir, c.parentPath)
	if err != nil {
		return fmt.Errorf("failed to collect SSTables from parent chain: %w", err)
	}

	// Get current SSTables
	levelStates, unlockFunc := p.GetLevelsManager().GetSSTablesForRead()
	defer unlockFunc()

	c.newManifest = &core.SnapshotManifest{
		Type:                core.SnapshotTypeIncremental,
		ParentID:            c.parentID,
		CreatedAt:           p.GetClock().Now(),
		SequenceNumber:      p.GetSequenceNumber(), // Get sequence number *after* flush for accuracy.
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
				destPath := filepath.Join(c.snapshotDir, relativeFileName)
				if err := c.m.wrapper.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
					return fmt.Errorf("failed to create sst subdirectory in incremental snapshot: %w", err)
				}
				if copyErr := c.m.wrapper.LinkOrCopyFile(table.FilePath(), destPath); copyErr != nil {
					return fmt.Errorf("failed to copy new SSTable %s for incremental snapshot: %w", table.FilePath(), copyErr)
				}
				levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
					ID: table.ID(), FileName: relativeFileName, MinKey: table.MinKey(), MaxKey: table.MaxKey(),
				})
			}
		}
		if len(levelManifest.Tables) > 0 {
			c.newManifest.Levels = append(c.newManifest.Levels, levelManifest)
		}
	}
	return nil
}

// finalizeSnapshot copies auxiliary files and writes the new manifest and CURRENT file.
func (c *incrementalCreator) finalizeSnapshot() error {
	// For simplicity and correctness, we copy the entire current state of WAL, index, and auxiliary files.
	if err := c.m.copyAuxiliaryAndWALFiles(c.snapshotDir, c.newManifest); err != nil {
		return err
	}

	// Write the final manifest and CURRENT file for the new snapshot.
	if _, err := c.m.writeManifestAndCurrentFunc(c.snapshotDir, c.newManifest); err != nil {
		return err
	}
	return nil
}

// auxiliaryCopier holds the state for copying auxiliary files and WAL for a snapshot.
type auxiliaryCopier struct {
	m           *manager
	snapshotDir string
	manifest    *core.SnapshotManifest
}

// copyAuxiliaryAndWALFiles is a helper for CreateFull and CreateIncremental.
func (m *manager) copyAuxiliaryAndWALFiles(snapshotDir string, manifest *core.SnapshotManifest) error {
	copier := &auxiliaryCopier{
		m:           m,
		snapshotDir: snapshotDir,
		manifest:    manifest,
	}
	return copier.run()
}

func (c *auxiliaryCopier) run() error {
	if err := c.saveJSONState(); err != nil {
		return err
	}
	if err := c.copyMappingLogs(); err != nil {
		return err
	}
	if err := c.createIndexSnapshot(); err != nil {
		return err
	}
	if err := c.copyWAL(); err != nil {
		return err
	}
	return nil
}

// saveJSONState saves state like deleted series and range tombstones to JSON files.
func (c *auxiliaryCopier) saveJSONState() error {
	p := c.m.provider
	p.Lock()
	deletedSeriesToSave := p.GetDeletedSeries()
	rangeTombstonesToSave := p.GetRangeTombstones()
	p.Unlock()

	if len(deletedSeriesToSave) > 0 {
		c.manifest.DeletedSeriesFile = "deleted_series.json"
		destPath := filepath.Join(c.snapshotDir, c.manifest.DeletedSeriesFile)
		if err := c.m.wrapper.SaveJSON(deletedSeriesToSave, destPath); err != nil {
			return fmt.Errorf("failed to save deleted_series for snapshot: %w", err)
		}
	}

	if len(rangeTombstonesToSave) > 0 {
		c.manifest.RangeTombstonesFile = "range_tombstones.json"
		destPath := filepath.Join(c.snapshotDir, c.manifest.RangeTombstonesFile)
		if err := c.m.wrapper.SaveJSON(rangeTombstonesToSave, destPath); err != nil {
			return fmt.Errorf("failed to save range_tombstones for snapshot: %w", err)
		}
	}
	return nil
}

// copyMappingLogs copies the string and series ID mapping log files.
func (c *auxiliaryCopier) copyMappingLogs() error {
	p := c.m.provider
	stringStore := p.GetPrivateStringStore()
	seriesIDStore := p.GetPrivateSeriesIDStore()

	if err := c.m.wrapper.CopyAuxiliaryFile(stringStore.GetLogFilePath(), "string_mapping.log", c.snapshotDir, &c.manifest.StringMappingFile, p.GetLogger()); err != nil {
		return err
	}
	if err := c.m.wrapper.CopyAuxiliaryFile(seriesIDStore.GetLogFilePath(), "series_mapping.log", c.snapshotDir, &c.manifest.SeriesMappingFile, p.GetLogger()); err != nil {
		return err
	}
	return nil
}

// createIndexSnapshot creates a snapshot of the tag index manager's state.
func (c *auxiliaryCopier) createIndexSnapshot() error {
	indexSnapshotDir := filepath.Join(c.snapshotDir, "index")
	if err := c.m.wrapper.MkdirAll(indexSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create subdirectory for index snapshot: %w", err)
	}
	if err := c.m.provider.GetTagIndexManager().CreateSnapshot(indexSnapshotDir); err != nil {
		return fmt.Errorf("failed to create tag index snapshot: %w", err)
	}
	return nil
}

// copyWAL copies the entire WAL directory to the snapshot.
func (c *auxiliaryCopier) copyWAL() error {
	p := c.m.provider
	srcWALDir := p.GetWAL().Path()

	if _, statErr := c.m.wrapper.Stat(srcWALDir); os.IsNotExist(statErr) {
		return nil // No WAL directory to copy, not an error.
	} else if statErr != nil {
		return fmt.Errorf("failed to stat source WAL directory %s: %w", srcWALDir, statErr)
	}

	destWALDirName := "wal"
	destWALDir := filepath.Join(c.snapshotDir, destWALDirName)
	if err := c.m.wrapper.MkdirAll(destWALDir, 0755); err != nil {
		return fmt.Errorf("failed to create wal directory in snapshot: %w", err)
	}
	if err := c.m.wrapper.LinkOrCopyDirectoryContents(srcWALDir, destWALDir); err != nil {
		return fmt.Errorf("failed to copy WAL directory to snapshot: %w", err)
	}
	c.manifest.WALFile = destWALDirName
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

// restorer holds the state for a single restore operation.
type restorer struct {
	opts           RestoreOptions
	snapshotDir    string
	tempRestoreDir string
	manifest       *core.SnapshotManifest
	manifestFile   string
	logger         *slog.Logger
	wrapper        internal.PrivateSnapshotHelper
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

	r := &restorer{
		opts:        opts,
		snapshotDir: snapshotDir,
		logger:      restoreLogger.With("component", "RestoreFromSnapshot"),
		wrapper:     opts.wrapper,
	}
	return r.run()
}

// run orchestrates the entire restore process.
func (r *restorer) run() (err error) {
	if err := r.validateAndPrepare(); err != nil {
		return err
	}

	if err := r.setupTempDir(); err != nil {
		return err
	}
	// Ensure the temporary directory is cleaned up if anything goes wrong after its creation.
	defer r.cleanupTempDir()

	if err := r.copyAllData(); err != nil {
		return err
	}

	if err := r.swapDataDirectories(); err != nil {
		return err
	}

	r.logger.Info("Snapshot restoration complete. Data directory replaced.", "data_dir", r.opts.DataDir)
	return nil
}

// validateAndPrepare performs initial safety checks and reads the snapshot manifest.
func (r *restorer) validateAndPrepare() error {
	r.logger.Info("Starting restore from snapshot.", "snapshot_dir", r.snapshotDir, "target_data_dir", r.opts.DataDir)

	if _, err := r.wrapper.Stat(r.snapshotDir); os.IsNotExist(err) {
		return fmt.Errorf("snapshot directory %s does not exist", r.snapshotDir)
	}

	manifest, manifestFileName, err := readManifestFromDir(r.snapshotDir, r.wrapper)
	if err != nil {
		return fmt.Errorf("could not read snapshot manifest: %w", err)
	}
	r.manifest = manifest
	r.manifestFile = manifestFileName
	r.logger.Info("Snapshot manifest loaded.", "sequence_number", r.manifest.SequenceNumber)
	return nil
}

// setupTempDir creates a temporary directory for the restore process.
func (r *restorer) setupTempDir() error {
	tempDir, err := r.wrapper.MkdirTemp(filepath.Dir(r.opts.DataDir), filepath.Base(r.opts.DataDir)+".restore-tmp-*")
	if err != nil {
		return fmt.Errorf("failed to create temporary restore directory: %w", err)
	}
	r.tempRestoreDir = tempDir
	r.logger.Info("Created temporary directory for restore.", "temp_dir", r.tempRestoreDir)
	return nil
}

// cleanupTempDir removes the temporary directory. It's safe to call even if the directory doesn't exist.
func (r *restorer) cleanupTempDir() {
	if r.tempRestoreDir == "" {
		return
	}
	if _, statErr := r.wrapper.Stat(r.tempRestoreDir); !os.IsNotExist(statErr) {
		r.logger.Info("Cleaning up temporary restore directory.", "temp_dir", r.tempRestoreDir)
		r.wrapper.RemoveAll(r.tempRestoreDir)
	}
}

// copyAllData copies all required files and directories from the snapshot to the temporary directory.
func (r *restorer) copyAllData() error {
	if err := r.copyIndex(); err != nil {
		return err
	}
	if err := r.copyManifestFiles(); err != nil {
		return err
	}
	if err := r.copyWAL(); err != nil {
		return err
	}
	return nil
}

// copyIndex copies the tag index directory from the snapshot.
func (r *restorer) copyIndex() error {
	srcIndexDir := filepath.Join(r.snapshotDir, indexer.IndexDirName)
	destIndexDir := filepath.Join(r.tempRestoreDir, indexer.IndexSSTDirName)

	if _, err := r.wrapper.Stat(srcIndexDir); os.IsNotExist(err) {
		r.logger.Debug("Index directory not found in snapshot, skipping.", "path", srcIndexDir)
		return nil // Not an error if it doesn't exist.
	} else if err != nil {
		return fmt.Errorf("failed to stat source index directory %s: %w", srcIndexDir, err)
	}

	if err := r.wrapper.MkdirAll(destIndexDir, 0755); err != nil {
		return fmt.Errorf("failed to create destination index directory %s: %w", destIndexDir, err)
	}
	if err := r.wrapper.CopyDirectoryContents(srcIndexDir, destIndexDir); err != nil {
		return fmt.Errorf("failed to copy tag index files from snapshot: %w", err)
	}
	r.logger.Debug("Restored tag index to temporary directory.")
	return nil
}

// copyManifestFiles gathers all file paths from the manifest and copies them.
func (r *restorer) copyManifestFiles() error {
	filesToCopy := []string{r.manifestFile, r.manifest.DeletedSeriesFile, r.manifest.RangeTombstonesFile, r.manifest.StringMappingFile, r.manifest.SeriesMappingFile, CURRENT_FILE_NAME}
	for _, level := range r.manifest.Levels {
		for _, table := range level.Tables {
			filesToCopy = append(filesToCopy, table.FileName)
		}
	}

	for _, fileName := range filesToCopy {
		if fileName == "" {
			continue
		}
		if err := r.copyFile(fileName); err != nil {
			return err
		}
	}
	return nil
}

// copyFile copies a single file from the snapshot source to the temp directory.
// It handles missing source files by logging a warning and continuing.
func (r *restorer) copyFile(fileName string) error {
	srcPath := filepath.Join(r.snapshotDir, fileName)
	destPath := filepath.Join(r.tempRestoreDir, fileName)

	if _, err := r.wrapper.Stat(srcPath); os.IsNotExist(err) {
		r.logger.Warn("File listed in manifest not found in snapshot directory, skipping.", "file", fileName)
		return nil // Not an error, just skip.
	} else if err != nil {
		return fmt.Errorf("failed to stat source file %s: %w", srcPath, err)
	}

	// The helper's CopyFile implementation already ensures the parent directory exists.
	if err := r.wrapper.CopyFile(srcPath, destPath); err != nil {
		return fmt.Errorf("failed to copy file %s to temporary restore directory: %w", srcPath, err)
	}
	r.logger.Debug("Restored file to temporary directory.", "file", fileName)
	return nil
}

// copyWAL copies the WAL directory or legacy WAL file from the snapshot.
func (r *restorer) copyWAL() error {
	if r.manifest.WALFile == "" {
		return nil
	}

	srcWALPath := filepath.Join(r.snapshotDir, r.manifest.WALFile)
	destWALPath := filepath.Join(r.tempRestoreDir, r.manifest.WALFile)

	stat, err := r.wrapper.Stat(srcWALPath)
	if os.IsNotExist(err) {
		r.logger.Warn("WAL directory/file listed in manifest not found in snapshot, skipping.", "path", srcWALPath)
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat source WAL path %s in snapshot: %w", srcWALPath, err)
	}

	if stat.IsDir() {
		if err := r.wrapper.MkdirAll(destWALPath, 0755); err != nil {
			return fmt.Errorf("failed to create WAL directory %s: %w", destWALPath, err)
		}
		if err := r.wrapper.CopyDirectoryContents(srcWALPath, destWALPath); err != nil {
			return fmt.Errorf("failed to copy WAL directory from snapshot: %w", err)
		}
		r.logger.Debug("Restored WAL directory to temporary directory.")
	} else {
		if err := r.wrapper.CopyFile(srcWALPath, destWALPath); err != nil {
			return fmt.Errorf("failed to copy legacy WAL file from snapshot: %w", err)
		}
		r.logger.Debug("Restored legacy WAL file to temporary directory.")
	}
	return nil
}

// swapDataDirectories removes the original data directory and renames the temporary directory to the final destination.
func (r *restorer) swapDataDirectories() error {
	_, err := r.wrapper.Stat(r.opts.DataDir)
	if err == nil {
		// If the directory exists, remove it.
		r.logger.Info("Removing original data directory before replacing.", "original_data_dir", r.opts.DataDir)
		if err := r.wrapper.RemoveAll(r.opts.DataDir); err != nil {
			return fmt.Errorf("failed to remove original data directory %s: %w", r.opts.DataDir, err)
		}
	} else if !os.IsNotExist(err) && !errors.Is(err, syscall.ENOENT) {
		// If there was an error and it's NOT a "not found" error (or its Windows equivalent),
		// then it's a real problem we should report.
		return fmt.Errorf("failed to stat original data directory %s: %w", r.opts.DataDir, err)
	}
	// If the error was os.IsNotExist or syscall.ENOENT, we do nothing and proceed to rename.

	if err := r.wrapper.Rename(r.tempRestoreDir, r.opts.DataDir); err != nil {
		return fmt.Errorf("failed to rename temporary restore directory %s to %s: %w", r.tempRestoreDir, r.opts.DataDir, err)
	}

	// After a successful rename, we prevent the deferred cleanup from removing the final data dir.
	r.tempRestoreDir = ""
	return nil
}

func RestoreFromLatest(opts RestoreOptions, snapshotsBaseDir string) error {
	restoreLogger := opts.Logger
	if restoreLogger == nil {
		restoreLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.wrapper == nil {
		opts.wrapper = newHelperSnapshot()
	}

	restoreLogger = restoreLogger.With("component", "RestoreFromLatest")
	restoreLogger.Info("Attempting to restore from the latest snapshot.", "base_dir", snapshotsBaseDir)

	latestID, latestPath, err := findLatestSnapshot(snapshotsBaseDir, opts.wrapper)
	if err != nil {
		return fmt.Errorf("failed to find the latest snapshot in %s: %w", snapshotsBaseDir, err)
	}
	if latestID == "" {
		return fmt.Errorf("no snapshots found in %s to restore from", snapshotsBaseDir)
	}

	restoreLogger.Info("Found latest snapshot to restore from.", "snapshot_id", latestID, "path", latestPath)

	// Call the (potentially mocked) restore function
	return restoreFromFullFunc(opts, latestPath)
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
