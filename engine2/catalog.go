package engine2

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
)

// DatabaseMetadata is the compact per-database metadata persisted to disk.
type DatabaseMetadata struct {
	CreatedAt    int64
	Version      uint32
	LastSequence uint64
	Options      map[string]string
}

// SaveMetadataAtomic writes metadata to `path` atomically (write temp file then rename).
func SaveMetadataAtomic(path string, meta *DatabaseMetadata) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata dir: %w", err)
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write temp metadata file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		// best effort cleanup
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to atomically rename metadata file: %w", err)
	}
	return nil
}

// LoadMetadata loads metadata from path.
func LoadMetadata(path string) (*DatabaseMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	var meta DatabaseMetadata
	if err := dec.Decode(&meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// EnsureDBDirs creates the standard per-database directories under baseDir/<dbName>.
func EnsureDBDirs(baseDir, dbName string) error {
	dbRoot := filepath.Join(baseDir, dbName)
	walDir := filepath.Join(dbRoot, "wal")
	sstDir := filepath.Join(dbRoot, "sstables")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return fmt.Errorf("failed to create wal dir: %w", err)
	}
	if err := os.MkdirAll(sstDir, 0755); err != nil {
		return fmt.Errorf("failed to create sstables dir: %w", err)
	}
	return nil
}
