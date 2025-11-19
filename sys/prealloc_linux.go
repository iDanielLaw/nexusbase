//go:build linux

package sys

import (
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
		return fmt.Errorf("file does not expose file descriptor for preallocation")
	}
	fd := int(fg.Fd())

	// Try fallocate with KEEP_SIZE (allocate blocks without changing file size).
	if err := unix.Fallocate(fd, unix.FALLOC_FL_KEEP_SIZE, 0, size); err == nil {
		return nil
	}

	// Fallback: try plain fallocate (may change file size).
	if err := unix.Fallocate(fd, 0, 0, size); err == nil {
		return nil
	}

	return fmt.Errorf("preallocation failed for fd=%d", fd)
}
