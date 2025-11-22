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
