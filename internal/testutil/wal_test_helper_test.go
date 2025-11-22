package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_WALStrictEnabled_Parse(t *testing.T) {
	orig := os.Getenv("ENGINE2_WAL_STRICT")
	defer os.Setenv("ENGINE2_WAL_STRICT", orig)

	os.Setenv("ENGINE2_WAL_STRICT", "true")
	if !WALStrictEnabled() {
		t.Fatalf("expected WALStrictEnabled to be true for 'true'")
	}
	os.Setenv("ENGINE2_WAL_STRICT", "1")
	if !WALStrictEnabled() {
		t.Fatalf("expected WALStrictEnabled to be true for '1'")
	}
	os.Setenv("ENGINE2_WAL_STRICT", "false")
	if WALStrictEnabled() {
		t.Fatalf("expected WALStrictEnabled to be false for 'false'")
	}
	os.Setenv("ENGINE2_WAL_STRICT", "0")
	if WALStrictEnabled() {
		t.Fatalf("expected WALStrictEnabled to be false for '0'")
	}
	os.Unsetenv("ENGINE2_WAL_STRICT")
	if WALStrictEnabled() {
		t.Fatalf("expected WALStrictEnabled to be false when unset")
	}
}

func Test_ListWALFiles_And_RequireWALPresent(t *testing.T) {
	tmp := t.TempDir()
	// missing wal dir should make ListWALFiles return error
	if _, err := ListWALFiles(tmp); err == nil {
		t.Fatalf("expected error when wal dir missing")
	}

	walDir := filepath.Join(tmp, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	// empty wal dir: ListWALFiles should return an empty slice
	files, err := ListWALFiles(tmp)
	if err != nil {
		t.Fatalf("ListWALFiles on empty wal dir failed: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("expected 0 wal files in empty wal dir, got %d", len(files))
	}

	// add a file and verify behavior
	f := filepath.Join(walDir, "engine.wal")
	if err := os.WriteFile(f, []byte("data"), 0644); err != nil {
		t.Fatalf("write wal file: %v", err)
	}

	files, err = ListWALFiles(tmp)
	if err != nil {
		t.Fatalf("ListWALFiles failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 wal file, got %d", len(files))
	}

	// RequireWALPresent should pass now
	t.Run("nonEmptyWalShouldPass", func(t *testing.T) {
		RequireWALPresent(t, tmp)
	})
}

func Test_RequireSSTablesPresent(t *testing.T) {
	tmp := t.TempDir()
	// neither sst nor sstables exists -> assert both are missing via os.ReadDir
	if _, err := os.ReadDir(filepath.Join(tmp, "sst")); err == nil {
		t.Fatalf("expected sst dir to be missing")
	}
	if _, err := os.ReadDir(filepath.Join(tmp, "sstables")); err == nil {
		t.Fatalf("expected sstables dir to be missing")
	}

	// create sst dir with file
	sst := filepath.Join(tmp, "sst")
	if err := os.MkdirAll(sst, 0755); err != nil {
		t.Fatalf("mkdir sst: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sst, "1.sst"), []byte("x"), 0644); err != nil {
		t.Fatalf("write sst: %v", err)
	}
	t.Run("sstShouldPass", func(t *testing.T) {
		RequireSSTablesPresent(t, tmp)
	})

	// remove sst, create sstables with file
	if err := os.RemoveAll(sst); err != nil {
		t.Fatalf("remove sst: %v", err)
	}
	sst2 := filepath.Join(tmp, "sstables")
	if err := os.MkdirAll(sst2, 0755); err != nil {
		t.Fatalf("mkdir sstables: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sst2, "2.sst"), []byte("y"), 0644); err != nil {
		t.Fatalf("write sst2: %v", err)
	}
	t.Run("sstablesShouldPass", func(t *testing.T) {
		RequireSSTablesPresent(t, tmp)
	})
}
