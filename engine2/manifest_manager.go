package engine2

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	const maxBatch = 128
	const batchWait = 2 * time.Millisecond

	for {
		select {
		case first := <-m.queue:
			// Start a batch with the first entry
			batch := []queuedEntry{first}
			// Collect additional entries up to maxBatch or until timeout
		collectLoop:
			for len(batch) < maxBatch {
				select {
				case more := <-m.queue:
					batch = append(batch, more)
				case <-time.After(batchWait):
					// small wait window to collect more writers
					break collectLoop
				case <-m.shutdownCh:
					// stop collecting further entries
					break collectLoop
				}
			}

			// Perform read-modify-write persist for the entire batch.
			diskManifest, err := LoadManifest(m.manifestPath)
			if err != nil {
				for _, be := range batch {
					be.resp <- err
				}
				continue
			}
			newEntries := make([]SSTableManifestEntry, 0, len(diskManifest.Entries)+len(batch))
			newEntries = append(newEntries, diskManifest.Entries...)
			for _, be := range batch {
				newEntries = append(newEntries, be.e)
			}
			// Persist once for the batch
			if err := SaveManifest(m.manifestPath, SSTableManifest{Entries: newEntries}); err != nil {
				for _, be := range batch {
					be.resp <- err
				}
				continue
			}
			// Update in-memory view under lock
			m.mu.Lock()
			m.entries = newEntries
			m.mu.Unlock()
			for _, be := range batch {
				be.resp <- nil
			}
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
