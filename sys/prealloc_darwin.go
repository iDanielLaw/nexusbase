//go:build darwin

package sys

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Preallocate attempts to allocate space for the given file on macOS using
// the F_PREALLOCATE fcntl interface (fstore_t). We try to allocate
// contiguous space first and fall back to non-contiguous allocation. On
// filesystems that do not support preallocation we return
// ErrPreallocNotSupported so callers can treat this as non-fatal.
func Preallocate(f FileHandle, size int64) error {
	if size <= 0 {
		return nil
	}

	fg, ok := f.(interface{ Fd() uintptr })
	if !ok {
		return ErrPreallocNotSupported
	}
	fd := int(fg.Fd())

	// Try to get device id for caching decisions
	var stat unix.Stat_t
	var dev uint64
	if err := unix.Fstat(fd, &stat); err == nil {
		dev = uint64(stat.Dev)
		if allow, ok := preallocCacheLoad(dev); ok {
			preallocCacheHit()
			if !allow {
				return ErrPreallocNotSupported
			}
			// cached allowed: attempt allocation below
		} else {
			preallocCacheMiss()
		}
	}

	// Build fstore_t for macOS preallocation
	var fst unix.Fstore_t
	// Try contiguous allocation first
	fst.Flags = unix.F_ALLOCATECONTIG
	fst.Posmode = unix.F_PEOFPOSMODE
	fst.Offset = 0
	fst.Length = size

	// Use SYS_FCNTL to invoke F_PREALLOCATE with pointer to fstore_t
	_, _, errno := unix.Syscall(unix.SYS_FCNTL, uintptr(fd), uintptr(unix.F_PREALLOCATE), uintptr(unsafe.Pointer(&fst)))
	if errno == 0 {
		if dev != 0 {
			preallocCacheStore(dev, true)
		}
		return nil
	}

	// Try non-contiguous allocation
	fst.Flags = unix.F_ALLOCATEALL
	_, _, errno2 := unix.Syscall(unix.SYS_FCNTL, uintptr(fd), uintptr(unix.F_PREALLOCATE), uintptr(unsafe.Pointer(&fst)))
	if errno2 == 0 {
		if dev != 0 {
			preallocCacheStore(dev, true)
		}
		return nil
	}

	// Map common not-supported errno values to the sentinel
	if errno == unix.ENOTSUP || errno == unix.EINVAL || errno == unix.ENOSYS {
		if dev != 0 {
			preallocCacheStore(dev, false)
		}
		return ErrPreallocNotSupported
	}
	if errno2 == unix.ENOTSUP || errno2 == unix.EINVAL || errno2 == unix.ENOSYS {
		if dev != 0 {
			preallocCacheStore(dev, false)
		}
		return ErrPreallocNotSupported
	}

	return errors.New(fmt.Sprintf("darwin preallocation failed: err1=%v err2=%v", errno, errno2))
}
