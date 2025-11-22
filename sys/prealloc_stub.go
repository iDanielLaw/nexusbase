//go:build !linux && !windows && !darwin
// +build !linux,!windows,!darwin

package sys

// Preallocate is a no-op on unsupported platforms (e.g., darwin) and returns
// ErrPreallocNotSupported to indicate the operation is not available. This
// prevents build errors on platforms that don't implement platform-specific
// preallocation helpers.
func Preallocate(f FileHandle, size int64) error {
	return ErrPreallocNotSupported
}
