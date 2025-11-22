package sys

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Test that AcquireFileLock will break a stale lock file when staleTTL is small.
func TestAcquireFileLock_StaleBreak(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "manifest")
	lockPath := base + ".lock"

	// Create a stale lock file with an old timestamp (unixnano)
	pid := 99999
	oldTs := time.Now().Add(-2 * time.Minute).UTC().UnixNano()
	content := strconv.Itoa(pid) + "\n" + strconv.FormatInt(oldTs, 10) + "\n"
	if err := os.WriteFile(lockPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write stale lock: %v", err)
	}

	// Try to acquire with staleTTL of 1 second; should remove the stale lock and succeed.
	release, err := AcquireFileLock(base, 5, 10*time.Millisecond, 1*time.Second)
	if err != nil {
		t.Fatalf("expected to acquire lock after breaking stale lock, got: %v", err)
	}
	// ensure lockfile exists and then release
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file missing after acquire: %v", err)
	}
	if rerr := release(); rerr != nil {
		t.Fatalf("release failed: %v", rerr)
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lock file still exists after release")
	}
}

// Test that AcquireFileLock will not break a fresh lock file when TTL not exceeded.
func TestAcquireFileLock_FreshPreventsAcquisition(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "manifest2")
	lockPath := base + ".lock"

	// Create a fresh lock file with current timestamp
	pid := os.Getpid()
	nowTs := time.Now().UTC().UnixNano()
	content := strconv.Itoa(pid) + "\n" + strconv.FormatInt(nowTs, 10) + "\n"
	if err := os.WriteFile(lockPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write fresh lock: %v", err)
	}

	// Try to acquire with staleTTL of 1 minute; should not break and should fail to acquire
	_, err := AcquireFileLock(base, 3, 20*time.Millisecond, 1*time.Minute)
	if err == nil {
		t.Fatalf("expected acquire to fail due to fresh lock, but it succeeded")
	}
}
