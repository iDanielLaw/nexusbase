package indexer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
)

// PersistIndexManifest writes the provided manifest to the given path using
// an atomic write: it writes to path + ".tmp" then renames into place.
func PersistIndexManifest(manifestPath string, manifest core.SnapshotManifest) error {
	tempPath := manifestPath + ".tmp"
	dir := filepath.Dir(manifestPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to ensure manifest dir exists: %w", err)
	}

	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temporary manifest file: %w", err)
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(manifest); err != nil {
		f.Close()
		_ = sys.Remove(tempPath)
		return fmt.Errorf("failed to encode manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = sys.Remove(tempPath)
		return fmt.Errorf("failed to close temporary manifest file: %w", err)
	}

	if err := sys.Rename(tempPath, manifestPath); err != nil {
		return fmt.Errorf("failed to rename temporary manifest: %w", err)
	}
	return nil
}

// LoadIndexManifest loads and decodes a manifest from the given path. If the
// file does not exist, it returns nil manifest and nil error to indicate a
// fresh start (caller may treat that as non-fatal).
func LoadIndexManifest(manifestPath string) (*core.SnapshotManifest, error) {
	f, err := os.Open(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open manifest file: %w", err)
	}
	defer f.Close()

	var manifest core.SnapshotManifest
	dec := json.NewDecoder(f)
	if err := dec.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}
	return &manifest, nil
}
