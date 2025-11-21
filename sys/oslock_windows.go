//go:build windows
// +build windows

package sys

import (
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// AcquireOSFileLock attempts to acquire an exclusive lock on the provided
// lockPath using Windows LockFileEx. It opens (or creates) the file and
// locks a single byte; the lock is held until release is called or the
// process exits. The call will retry until timeout.
func AcquireOSFileLock(lockPath string, timeout time.Duration) (func() error, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	h := windows.Handle(f.Fd())
	// Prepare overlapped structure
	var ov windows.Overlapped

	deadline := time.Now().Add(timeout)
	for {
		// LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY (non-blocking)
		err = windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ov)
		if err == nil {
			rel := func() error {
				_ = windows.UnlockFileEx(h, 0, 1, 0, &ov)
				_ = f.Close()
				_ = os.Remove(lockPath)
				return nil
			}
			return rel, nil
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, err
		}
		time.Sleep(25 * time.Millisecond)
	}
}
