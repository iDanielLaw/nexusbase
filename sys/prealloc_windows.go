//go:build windows

package sys

// Preallocate is a no-op on Windows by default. Windows supports several
// allocation APIs, but they are platform/privilege sensitive; keep this
// as a safe no-op fallback.
func Preallocate(f FileInterface, size int64) error {
	return nil
}
