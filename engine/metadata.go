package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// persistManifest triggers the creation of a new manifest file and atomically updates the CURRENT file.
// This function is called periodically or on significant state changes.
// It expects e.mu to be locked by the caller.
func (e *storageEngine) persistManifest() (err error) {
	_, span := e.tracer.Start(context.Background(), "StorageEngine.persistManifest")
	defer span.End()

	currentFilePath := filepath.Join(e.opts.DataDir, CURRENT_FILE_NAME)
	oldManifestFileName := ""
	if oldCurrentFileContent, readErr := os.ReadFile(currentFilePath); readErr == nil {
		oldManifestFileName = strings.TrimSpace(string(oldCurrentFileContent))
	} else if !os.IsNotExist(readErr) {
		e.logger.Warn("Error reading existing CURRENT file for old manifest cleanup.", "error", readErr)
	}

	// Construct the manifest based on current levels state and sequence number
	manifest := core.SnapshotManifest{
		SequenceNumber:      e.sequenceNumber.Load(),
		Levels:              make([]core.SnapshotLevelManifest, 0, e.levelsManager.MaxLevels()),
		SSTableCompression:  e.opts.SSTableCompressor.Type().String(),
		CreatedAt:           e.clock.Now(),
		LastWALSegmentIndex: e.wal.ActiveSegmentIndex(),
	}

	levelStates, unlockFunc := e.levelsManager.GetSSTablesForRead()
	defer unlockFunc()
	for levelNum, levelState := range levelStates {
		tablesInLevel := levelState.GetTables()
		levelManifest := core.SnapshotLevelManifest{LevelNumber: levelNum, Tables: make([]core.SSTableMetadata, 0, len(tablesInLevel))}
		for _, table := range tablesInLevel {
			baseFileName := filepath.Base(table.FilePath())
			manifestFileName := filepath.Join("sst", baseFileName)
			levelManifest.Tables = append(levelManifest.Tables, core.SSTableMetadata{
				ID:       table.ID(),
				FileName: manifestFileName,
				MinKey:   table.MinKey(),
				MaxKey:   table.MaxKey(),
			})
		}
		if len(levelManifest.Tables) > 0 {
			manifest.Levels = append(manifest.Levels, levelManifest)
		}
	}

	// Create a new manifest file with a unique name
	uniqueManifestFileName := fmt.Sprintf("%s_%d.bin", MANIFEST_FILE_PREFIX, e.clock.Now().UnixNano())
	manifestFilePath := filepath.Join(e.opts.DataDir, uniqueManifestFileName)
	e.logger.Debug("Persisting manifest with current state.", "manifest_file", uniqueManifestFileName)

	manifestFile, createErr := os.Create(manifestFilePath)
	if createErr != nil {
		e.logger.Error("Failed to create manifest file for writing.", "path", manifestFilePath, "error", createErr)
		span.SetStatus(codes.Error, "write_manifest_failed")
		return fmt.Errorf("failed to write manifest file: %w", createErr)
	}
	defer func() {
		if err != nil {
			manifestFile.Close()
			os.Remove(manifestFilePath)
		}
	}()

	if err = writeManifestBinary(manifestFile, &manifest); err != nil {
		e.logger.Error("Failed to write binary manifest data.", "path", manifestFilePath, "error", err)
		span.SetStatus(codes.Error, "write_manifest_failed")
		return fmt.Errorf("failed to write binary manifest data: %w", err)
	}
	if err = manifestFile.Close(); err != nil {
		return fmt.Errorf("failed to close new manifest file: %w", err)
	}

	// Update nextSSTableID to file
	nextSSTableIDFilePath := filepath.Join(e.opts.DataDir, NEXTID_FILE_NAME)
	numByte := make([]byte, 8)
	binary.BigEndian.PutUint64(numByte, e.nextSSTableID.Load())
	if err = os.WriteFile(nextSSTableIDFilePath, numByte, 0644); err != nil {
		e.logger.Error("Failed to update NEXTID file.", "path", nextSSTableIDFilePath, "error", err)
		span.SetStatus(codes.Error, "update_next_id_file_failed")
		return fmt.Errorf("failed to update NEXTID file: %w", err)
	}

	// Atomically update the CURRENT file
	tempCurrentFilePath := currentFilePath + ".tmp"
	if err = os.WriteFile(tempCurrentFilePath, []byte(uniqueManifestFileName), 0644); err != nil {
		e.logger.Error("Failed to write temporary CURRENT file.", "path", tempCurrentFilePath, "error", err)
		span.SetStatus(codes.Error, "update_current_file_failed")
		return fmt.Errorf("failed to write temporary CURRENT file: %w", err)
	}
	if err = os.Rename(tempCurrentFilePath, currentFilePath); err != nil {
		e.logger.Error("Failed to atomically update CURRENT file.", "from", tempCurrentFilePath, "to", currentFilePath, "error", err)
		span.SetStatus(codes.Error, "update_current_file_failed")
		return fmt.Errorf("failed to atomically update CURRENT file: %w", err)
	}

	// Force a modification time update to assist tests on low-resolution filesystems.
	// This is a best-effort operation and should not fail the entire manifest persistence.
	if chtimesErr := os.Chtimes(currentFilePath, e.clock.Now(), e.clock.Now()); chtimesErr != nil {
		e.logger.Warn("Failed to update modification time for CURRENT file, this might affect tests on some filesystems", "path", currentFilePath, "error", chtimesErr)
	}

	// Clean up old manifest file (best effort)
	if oldManifestFileName != "" && oldManifestFileName != uniqueManifestFileName {
		oldManifestFilePath := filepath.Join(e.opts.DataDir, oldManifestFileName)
		if _, statErr := os.Stat(oldManifestFilePath); statErr == nil {
			if removeErr := os.Remove(oldManifestFilePath); removeErr != nil {
				e.logger.Warn("Failed to delete old manifest file.", "path", oldManifestFilePath, "error", removeErr)
			} else {
				e.logger.Info("Old manifest file deleted.", "path", oldManifestFilePath)
			}
		}
	}

	e.logger.Info("Manifest persisted successfully.", "manifest_file", uniqueManifestFileName)
	span.SetAttributes(attribute.String("manifest.file", uniqueManifestFileName))

	// --- Post-Manifest-Write Hook ---
	postManifestPayload := hooks.ManifestWritePayload{
		Path: manifestFilePath,
	}
	e.hookManager.Trigger(context.Background(), hooks.NewPostManifestWriteEvent(postManifestPayload))

	return nil
}
