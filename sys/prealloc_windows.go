//go:build windows

package sys

import (
	"errors"
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Preallocate attempts to allocate disk space for the file without exposing
// the allocated region to readers. We try to use SetFileInformationByHandle
// with FileAllocationInfo which requests allocation of physical storage for a
// file. This is best-effort: if the call fails (unsupported filesystem or
// insufficient privileges), we return an error and callers should treat this
// as non-fatal.
func Preallocate(f FileHandle, size int64) error {
	if size <= 0 {
		return nil
	}

	// The underlying FileInterface should expose Fd()
	fg, ok := f.(interface{ Fd() uintptr })
	if !ok {
		return ErrPreallocNotSupported
	}

	// Use golang.org/x/sys/windows to call SetFileInformationByHandle
	// with FILE_ALLOCATION_INFO. This requests allocation of clusters for the
	// file without necessarily changing its logical size.
	h := windows.Handle(fg.Fd())

	// Try to get a volume/device identifier to cache per-device capability.
	var dev uint64
	var bhfi windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &bhfi); err == nil {
		dev = uint64(bhfi.VolumeSerialNumber)
		if allow, ok := preallocCacheLoad(dev); ok {
			preallocCacheHit()
			if !allow {
				return ErrPreallocNotSupported
			}
			// cached allowed: attempt allocation and return based on result
			type fileAllocInfo struct {
				AllocationSize int64
			}
			info := fileAllocInfo{AllocationSize: int64(size)}
			if err := windows.SetFileInformationByHandle(h, windows.FileAllocationInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err == nil {
				return nil
			} else {
				if errors.Is(err, windows.ERROR_INVALID_FUNCTION) || errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED) {
					return ErrPreallocNotSupported
				}
				return fmt.Errorf("windows preallocation failed: %w", err)
			}
		}
		// not cached: record a miss and continue to attempt allocation
		preallocCacheMiss()
	}

	// Construct FILE_ALLOCATION_INFO structure (LARGE_INTEGER AllocationSize)
	type fileAllocInfo struct {
		AllocationSize int64
	}
	info := fileAllocInfo{AllocationSize: int64(size)}

	err := windows.SetFileInformationByHandle(h, windows.FileAllocationInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	if err == nil {
		if dev != 0 {
			preallocCacheStore(dev, true)
		}
		return nil
	}

	// Map common "not supported" errors to the sentinel so callers can suppress
	// noisy warnings for expected filesystems or mounts that don't support this
	// operation.
	if errors.Is(err, windows.ERROR_INVALID_FUNCTION) || errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED) {
		if dev != 0 {
			preallocCacheStore(dev, false)
		}
		return ErrPreallocNotSupported
	}

	return fmt.Errorf("windows preallocation failed: %w", err)
}
