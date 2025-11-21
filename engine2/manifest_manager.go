package engine2

import (
	"os"
	"path/filepath"
	"sync"
)

// ManifestManager maintains an in-memory view of the SSTable manifest and
// provides helpers to persist and query entries.
type ManifestManager struct {
	mu           sync.RWMutex
	entries      []SSTableManifestEntry
	manifestPath string
}

// NewManifestManager constructs a manager for the given manifest path and loads
// any existing entries. It does not error on a missing manifest file.
func NewManifestManager(manifestPath string) (*ManifestManager, error) {
	m := &ManifestManager{manifestPath: manifestPath}
	// Ensure directory exists
	_ = ensureDir(filepath.Dir(manifestPath))
	if err := m.loadFromDisk(); err != nil {
		return nil, err
	}
	return m, nil
}

func ensureDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func (m *ManifestManager) loadFromDisk() error {
	manifest, err := LoadManifest(m.manifestPath)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = manifest.Entries
	return nil
}

// Reload refreshes the in-memory manifest from disk.
func (m *ManifestManager) Reload() error {
	return m.loadFromDisk()
}

// AddEntry persists the new entry and updates the in-memory state.
func (m *ManifestManager) AddEntry(e SSTableManifestEntry) error {
	// Persist by reading latest on-disk manifest, appending, saving, and
	// finally updating in-memory state. This reduces chances of overwriting
	// concurrent external updates and ensures in-memory state reflects
	// successfully persisted data.
	m.mu.Lock()
	defer m.mu.Unlock()

	// Load latest on-disk manifest
	diskManifest, err := LoadManifest(m.manifestPath)
	if err != nil {
		return err
	}
	newEntries := make([]SSTableManifestEntry, 0, len(diskManifest.Entries)+1)
	newEntries = append(newEntries, diskManifest.Entries...)
	newEntries = append(newEntries, e)

	// Persist the new manifest first
	if err := SaveManifest(m.manifestPath, SSTableManifest{Entries: newEntries}); err != nil {
		return err
	}

	// Update in-memory view to match persisted manifest
	m.entries = newEntries
	return nil
}

// ListEntries returns a copy of current manifest entries.
func (m *ManifestManager) ListEntries() []SSTableManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SSTableManifestEntry, len(m.entries))
	copy(out, m.entries)
	return out
}
