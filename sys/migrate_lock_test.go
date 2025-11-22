package sys

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMigrateLockFileToBinary_ConvertsLegacyText(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "manifest")
	lockPath := base + ".lock"

	pid := 4242
	ts := time.Now().UTC().UnixNano()
	content := strconv.Itoa(pid) + "\n" + strconv.FormatInt(ts, 10) + "\n"
	if err := os.WriteFile(lockPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write legacy lock: %v", err)
	}

	if err := MigrateLockFileToBinary(lockPath); err != nil {
		t.Fatalf("migration failed: %v", err)
	}

	b, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read migrated file: %v", err)
	}
	if len(b) < 12 {
		t.Fatalf("migrated file too small: %d", len(b))
	}
	pidFromFile := int(binary.LittleEndian.Uint32(b[0:4]))
	tsFromFile := int64(binary.LittleEndian.Uint64(b[4:12]))
	if pidFromFile != pid {
		t.Fatalf("pid mismatch: got %d want %d", pidFromFile, pid)
	}
	if tsFromFile != ts {
		t.Fatalf("timestamp mismatch: got %d want %d", tsFromFile, ts)
	}
}

func TestMigrateLockFileToBinary_NoopOnBinary(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "manifest2")
	lockPath := base + ".lock"

	pid := 555
	ts := time.Now().UTC().UnixNano()
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(pid))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts))
	if err := os.WriteFile(lockPath, buf, 0644); err != nil {
		t.Fatalf("write binary lock: %v", err)
	}

	if err := MigrateLockFileToBinary(lockPath); err != nil {
		t.Fatalf("migration failed: %v", err)
	}
	// verify unchanged
	b, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read migrated file: %v", err)
	}
	if len(b) < 12 {
		t.Fatalf("file too small: %d", len(b))
	}
	pidFromFile := int(binary.LittleEndian.Uint32(b[0:4]))
	tsFromFile := int64(binary.LittleEndian.Uint64(b[4:12]))
	if pidFromFile != pid || tsFromFile != ts {
		t.Fatalf("binary content changed")
	}
}

func TestMigrateLocksInDir_Recursive(t *testing.T) {
	root := t.TempDir()
	// create some lock files in nested dirs
	os.MkdirAll(filepath.Join(root, "a", "b"), 0755)
	// legacy text lock
	t1 := filepath.Join(root, "a", "old.lock")
	pid := 77
	ts := time.Now().UTC().UnixNano()
	content := strconv.Itoa(pid) + "\n" + strconv.FormatInt(ts, 10) + "\n"
	if err := os.WriteFile(t1, []byte(content), 0644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	// binary lock deeper
	t2 := filepath.Join(root, "a", "b", "bin.lock")
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(88))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts))
	if err := os.WriteFile(t2, buf, 0644); err != nil {
		t.Fatalf("write binary: %v", err)
	}

	migrated, err := MigrateLocksInDir(root)
	if err != nil {
		t.Fatalf("MigrateLocksInDir failed: %v", err)
	}
	if migrated != 1 {
		t.Fatalf("expected 1 migrated, got %d", migrated)
	}
	// verify t1 is binary now
	b, err := os.ReadFile(t1)
	if err != nil {
		t.Fatalf("read migrated: %v", err)
	}
	if len(b) < 12 || strings.Contains(string(b), "\n") {
		t.Fatalf("t1 not migrated to binary")
	}
}
