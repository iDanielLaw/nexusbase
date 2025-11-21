package engine2

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManifestManagerStartupLoadsExisting(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sstables", "manifest.bin")

	now := time.Now().UTC()
	entries := []SSTableManifestEntry{
		{ID: 201, FilePath: filepath.Join(tmpDir, "x.sst"), KeyCount: 7, CreatedAt: now},
		{ID: 202, FilePath: filepath.Join(tmpDir, "y.sst"), KeyCount: 8, CreatedAt: now.Add(time.Second)},
	}

	if err := SaveManifest(manifestPath, SSTableManifest{Entries: entries}); err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}

	mgr, err := NewManifestManager(manifestPath)
	if err != nil {
		t.Fatalf("NewManifestManager failed: %v", err)
	}
	defer mgr.Close()

	got := mgr.ListEntries()
	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}
	for i := range entries {
		if got[i].ID != entries[i].ID || got[i].FilePath != entries[i].FilePath || got[i].KeyCount != entries[i].KeyCount || got[i].CreatedAt.UnixNano() != entries[i].CreatedAt.UnixNano() {
			t.Fatalf("entry[%d] mismatch: exp=%+v got=%+v", i, entries[i], got[i])
		}
	}
}

func TestManifestManagerConcurrentAdd(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sstables", "manifest.bin")

	mgr, err := NewManifestManager(manifestPath)
	if err != nil {
		t.Fatalf("NewManifestManager failed: %v", err)
	}
	defer mgr.Close()

	var wg sync.WaitGroup
	var errOnce atomic.Uint32
	var idCounter uint64
	goroutines := 20
	perGoroutine := 25

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(gidx int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := atomic.AddUint64(&idCounter, 1)
				e := SSTableManifestEntry{
					ID:        id,
					FilePath:  fmt.Sprintf("%s/sst-%d-%d.sst", tmpDir, gidx, i),
					KeyCount:  uint64(id * 3),
					CreatedAt: time.Now().UTC(),
				}
				if err := mgr.AddEntry(e); err != nil {
					errOnce.Store(1)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	if errOnce.Load() != 0 {
		t.Fatalf("one or more AddEntry calls failed")
	}

	got := mgr.ListEntries()
	expected := goroutines * perGoroutine
	if len(got) != expected {
		t.Fatalf("expected %d entries after concurrent adds, got %d", expected, len(got))
	}

	// verify persisted manifest matches in-memory
	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if len(loaded.Entries) != expected {
		t.Fatalf("persisted manifest entry count mismatch: got %d want %d", len(loaded.Entries), expected)
	}
}
