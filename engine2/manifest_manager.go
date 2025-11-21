package engine2

import (
	"fmt"
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
	// background queue for serializing manifest writes to disk. Entries are
	// sent to the queue and processed by a single worker goroutine which
	// performs persistence and updates the in-memory view.
	queue       chan queuedEntry
	shutdownCh  chan struct{}
	workerClose sync.WaitGroup
}

type queuedEntry struct {
	e    SSTableManifestEntry
	resp chan error
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
	// initialize background queue and worker
	m.queue = make(chan queuedEntry, 1024)
	m.shutdownCh = make(chan struct{})
	m.workerClose.Add(1)
	go m.manifestWriter()
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
	// Enqueue the entry and wait for the background writer to persist it.
	qe := queuedEntry{e: e, resp: make(chan error, 1)}
	select {
	case m.queue <- qe:
		// submitted
	case <-m.shutdownCh:
		return fmt.Errorf("manifest manager shutting down")
	}
	// wait for result
	return <-qe.resp
}

// Close stops the background writer and waits for outstanding work to finish.
func (m *ManifestManager) Close() {
	// Idempotent close
	select {
	case <-m.shutdownCh:
		return
	default:
	}
	close(m.shutdownCh)
	// drain queue to avoid blocking producer callers (should be none in tests)
	// wait for worker to finish
	m.workerClose.Wait()
}

// manifestWriter runs as a single background goroutine processing queued entries.
func (m *ManifestManager) manifestWriter() {
	defer m.workerClose.Done()
	for {
		select {
		case qe := <-m.queue:
			// Perform read-modify-write persist for this single entry.
			// Load latest manifest from disk to merge any external changes.
			diskManifest, err := LoadManifest(m.manifestPath)
			if err != nil {
				qe.resp <- err
				continue
			}
			newEntries := make([]SSTableManifestEntry, 0, len(diskManifest.Entries)+1)
			newEntries = append(newEntries, diskManifest.Entries...)
			newEntries = append(newEntries, qe.e)
			// Persist
			if err := SaveManifest(m.manifestPath, SSTableManifest{Entries: newEntries}); err != nil {
				qe.resp <- err
				continue
			}
			// Update in-memory view under lock
			m.mu.Lock()
			m.entries = newEntries
			m.mu.Unlock()
			qe.resp <- nil
		case <-m.shutdownCh:
			return
		}
	}
}

// ListEntries returns a copy of current manifest entries.
func (m *ManifestManager) ListEntries() []SSTableManifestEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]SSTableManifestEntry, len(m.entries))
	copy(out, m.entries)
	return out
}
