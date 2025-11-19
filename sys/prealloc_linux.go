//go:build linux

package sys

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// Preallocate attempts to allocate space for the given file without changing
// the visible file size using fallocate where available. On filesystems that
// don't support KEEP_SIZE, it will attempt a best-effort allocation.
func Preallocate(f FileInterface, size int64) error {
	if size <= 0 {
		return nil
	}
	fg, ok := f.(interface{ Fd() uintptr })
	if !ok {
		return ErrPreallocNotSupported
	}
	fd := int(fg.Fd())

	// Try fallocate with KEEP_SIZE (allocate blocks without changing file size).
	if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, size); err == nil {
		return nil
	} else {
		// If fallocate returns a known "not supported"/invalid error, map to sentinel.
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
			return ErrPreallocNotSupported
		}
		// Otherwise try a plain fallocate which may change file size.
	}

	if err := unix.Fallocate(fd, 0, 0, size); err == nil {
		return nil
	} else {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTTY) {
			return ErrPreallocNotSupported
		}
		return fmt.Errorf("preallocation failed for fd=%d: %w", fd, err)
	}
}
