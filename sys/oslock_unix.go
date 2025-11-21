//go:build !windows
// +build !windows

package sys

import (
	"os"
	"syscall"
	"time"
)

// AcquireOSFileLock attempts to acquire an advisory exclusive lock on the
// provided lockPath using POSIX flock. It opens (or creates) the file and
// acquires the lock on the file descriptor. If successful it returns a
// release function which will unlock, close the file and remove the file.
// The function will retry until the provided timeout elapses.
func AcquireOSFileLock(lockPath string, timeout time.Duration) (func() error, error) {
	// Open or create the lock file for read-write.
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	fd := int(f.Fd())
	deadline := time.Now().Add(timeout)
	for {
		err = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			// Acquired lock
			rel := func() error {
				// Unlock
				_ = syscall.Flock(fd, syscall.LOCK_UN)
				_ = f.Close()
				// attempt to remove the lock file; ignore errors
				_ = os.Remove(lockPath)
				return nil
			}
			return rel, nil
		}
		// If timeout expired, fail
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, err
		}
		// otherwise wait a bit and retry
		time.Sleep(25 * time.Millisecond)
	}
}
