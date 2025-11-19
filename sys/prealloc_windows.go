//go:build windows

package sys

import (
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
func Preallocate(f FileInterface, size int64) error {
	if size <= 0 {
		return nil
	}

	// The underlying FileInterface should expose Fd()
	fg, ok := f.(interface{ Fd() uintptr })
	if !ok {
		return nil
	}

	// Use golang.org/x/sys/windows to call SetFileInformationByHandle
	// with FILE_ALLOCATION_INFO. This requests allocation of clusters for the
	// file without necessarily changing its logical size.
	h := windows.Handle(fg.Fd())

	// Construct FILE_ALLOCATION_INFO structure (LARGE_INTEGER AllocationSize)
	type fileAllocInfo struct {
		AllocationSize int64
	}
	info := fileAllocInfo{AllocationSize: int64(size)}

	err := windows.SetFileInformationByHandle(h, windows.FileAllocationInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
	if err == nil {
		return nil
	}

	// If allocation via SetFileInformationByHandle failed, return the error.
	return fmt.Errorf("windows preallocation failed: %w", err)
}
