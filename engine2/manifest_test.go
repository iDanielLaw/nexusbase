package engine2

import (
	"path/filepath"
	"testing"
	"time"
)

func TestManifestBinaryRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	manifestPath := filepath.Join(tmpDir, "sstables", "manifest.bin")

	// create two entries
	now := time.Now().UTC()
	entries := []SSTableManifestEntry{
		{ID: 101, FilePath: filepath.Join(tmpDir, "a.sst"), KeyCount: 10, CreatedAt: now},
		{ID: 102, FilePath: filepath.Join(tmpDir, "b.sst"), KeyCount: 20, CreatedAt: now.Add(time.Second)},
	}

	// save manifest
	if err := SaveManifest(manifestPath, SSTableManifest{Entries: entries}); err != nil {
		t.Fatalf("SaveManifest failed: %v", err)
	}

	// load and compare
	loaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("LoadManifest failed: %v", err)
	}
	if len(loaded.Entries) != len(entries) {
		t.Fatalf("entry count mismatch: got %d want %d", len(loaded.Entries), len(entries))
	}
	for i := range entries {
		exp := entries[i]
		got := loaded.Entries[i]
		if exp.ID != got.ID || exp.FilePath != got.FilePath || exp.KeyCount != got.KeyCount || exp.CreatedAt.UnixNano() != got.CreatedAt.UnixNano() {
			t.Fatalf("entry[%d] mismatch: exp=%+v got=%+v", i, exp, got)
		}
	}

	// Append one more entry using AppendManifestEntry and verify
	extra := SSTableManifestEntry{ID: 103, FilePath: filepath.Join(tmpDir, "c.sst"), KeyCount: 5, CreatedAt: now.Add(2 * time.Second)}
	if err := AppendManifestEntry(manifestPath, extra); err != nil {
		t.Fatalf("AppendManifestEntry failed: %v", err)
	}
	reloaded, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("LoadManifest after append failed: %v", err)
	}
	if len(reloaded.Entries) != 3 {
		t.Fatalf("after append entry count mismatch: got %d want %d", len(reloaded.Entries), 3)
	}
	last := reloaded.Entries[2]
	if last.ID != extra.ID || last.FilePath != extra.FilePath || last.KeyCount != extra.KeyCount || last.CreatedAt.UnixNano() != extra.CreatedAt.UnixNano() {
		t.Fatalf("appended entry mismatch: exp=%+v got=%+v", extra, last)
	}
}
