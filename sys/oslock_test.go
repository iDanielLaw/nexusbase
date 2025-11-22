package sys

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAcquireOSFileLock_Exclusive(t *testing.T) {
	dir := t.TempDir()
	lockBase := filepath.Join(dir, "lck")
	lockPath := lockBase + ".lock"

	// First acquire with a reasonable timeout
	rel1, err := AcquireOSFileLock(lockPath, 500*time.Millisecond)
	if err != nil {
		// If the platform doesn't support OS-level locks, skip the test.
		if strings.Contains(err.Error(), "not supported") || strings.Contains(err.Error(), "not implemented") {
			t.Skip("OS file locking not supported on this platform")
		}
		t.Fatalf("failed to acquire initial OS lock: %v", err)
	}
	// Second acquisition should fail quickly (use small timeout)
	_, err2 := AcquireOSFileLock(lockPath, 50*time.Millisecond)
	if err2 == nil {
		// If it succeeded, release the second and fail the test
		t.Fatalf("expected second acquisition to fail due to exclusive lock")
	}

	// Release first
	if rerr := rel1(); rerr != nil {
		t.Fatalf("failed to release lock: %v", rerr)
	}

	// Now acquisition should succeed
	rel2, err3 := AcquireOSFileLock(lockPath, 200*time.Millisecond)
	if err3 != nil {
		t.Fatalf("expected to acquire lock after release, got: %v", err3)
	}
	_ = rel2()
}
