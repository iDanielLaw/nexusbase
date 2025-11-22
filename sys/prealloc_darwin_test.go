//go:build darwin

package sys

import (
	"os"
	"testing"
)

// TestPreallocateDarwin exercises the darwin Preallocate implementation.
// The test is macOS-only and verifies that Preallocate either succeeds or
// returns ErrPreallocNotSupported, and that it does not change the visible
// file size (we expect Preallocate to allocate blocks without changing the
// logical file size).
func TestPreallocateDarwin(t *testing.T) {
	f, err := os.CreateTemp("", "prealloc-test-*")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Size() != 0 {
		t.Fatalf("expected initial size 0, got %d", st.Size())
	}

	const want = 1 << 20 // 1 MiB
	err = Preallocate(f, int64(want))
	if err != nil && err != ErrPreallocNotSupported {
		t.Fatalf("Preallocate unexpected error: %v", err)
	}

	st2, err := f.Stat()
	if err != nil {
		t.Fatalf("stat2: %v", err)
	}
	if st2.Size() != 0 {
		t.Fatalf("expected size still 0 after Preallocate, got %d", st2.Size())
	}
}
