//go:build !unix && !windows
// +build !unix,!windows

package sys

import (
	"errors"
	"time"
)

var ErrOSFileLockNotSupported = errors.New("OS file locking not supported on this platform")

// Preallocate is a no-op on unsupported platforms (e.g., darwin) and returns
// ErrPreallocNotSupported to indicate the operation is not available. This
// prevents build errors on platforms that don't implement platform-specific
// preallocation helpers.

func AcquireOSFileLock(lockPath string, timeout time.Duration) (func() error, error) {
	return nil, ErrOSFileLockNotSupported
}
