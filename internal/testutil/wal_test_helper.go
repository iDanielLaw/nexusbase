package testutil

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// WALStrictEnabled returns true when the environment requests strict WAL
// presence checks. It reads the `ENGINE2_WAL_STRICT` environment variable
// and parses it as a boolean. Default is false (permissive).
func WALStrictEnabled() bool {
	v := strings.TrimSpace(os.Getenv("ENGINE2_WAL_STRICT"))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

// RequireWALPresent asserts that the `wal/` directory exists under dataDir
// and contains at least one file. It fails the test immediately if the
// requirement is not met.
func RequireWALPresent(t *testing.T, dataDir string) {
	t.Helper()
	walDir := filepath.Join(dataDir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("expected wal directory after restore at %s: %v", walDir, err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected wal files in %s after restore, none found", walDir)
	}
}

// ListWALFiles returns the list of files (paths) under dataDir/wal.
// Returns an error if the wal directory does not exist or cannot be read.
func ListWALFiles(dataDir string) ([]string, error) {
	walDir := filepath.Join(dataDir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() {
			files = append(files, filepath.Join(walDir, e.Name()))
		}
	}
	return files, nil
}

// RequireSSTablesPresent asserts that either `sst/` or `sstables/` exists
// and contains at least one SSTable file.
func RequireSSTablesPresent(t *testing.T, dataDir string) {
	t.Helper()
	sstDir := filepath.Join(dataDir, "sst")
	sstEntries, err := os.ReadDir(sstDir)
	if err == nil && len(sstEntries) > 0 {
		return
	}
	sst2Dir := filepath.Join(dataDir, "sstables")
	entries2, err2 := os.ReadDir(sst2Dir)
	if err2 == nil && len(entries2) > 0 {
		return
	}
	// prefer original err if present
	if err != nil {
		t.Fatalf("expected sst or sstables directory after restore: %v / %v", err, err2)
	}
	t.Fatalf("expected at least one sstable in %s or %s", sstDir, sst2Dir)
}
