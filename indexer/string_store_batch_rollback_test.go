package indexer

import (
	"fmt"
	"io"
	"os"
	"testing"
)

// failingFile wraps *os.File but can be toggled to fail on Write or Sync.
type failingFile struct {
	f         *os.File
	failWrite bool
	failSync  bool
}

func (ff *failingFile) Read(p []byte) (int, error) { return ff.f.Read(p) }
func (ff *failingFile) Write(p []byte) (int, error) {
	if ff.failWrite {
		return 0, fmt.Errorf("injected write error")
	}
	return ff.f.Write(p)
}
func (ff *failingFile) Close() error                             { return ff.f.Close() }
func (ff *failingFile) ReadAt(p []byte, off int64) (int, error)  { return ff.f.ReadAt(p, off) }
func (ff *failingFile) WriteAt(p []byte, off int64) (int, error) { return ff.f.WriteAt(p, off) }
func (ff *failingFile) Seek(offset int64, whence int) (int64, error) {
	return ff.f.Seek(offset, whence)
}
func (ff *failingFile) ReadFrom(r io.Reader) (int64, error) { return ff.f.ReadFrom(r) }
func (ff *failingFile) WriteTo(w io.Writer) (int64, error)  { return ff.f.WriteTo(w) }
func (ff *failingFile) WriteString(s string) (int, error)   { return ff.f.WriteString(s) }
func (ff *failingFile) Stat() (os.FileInfo, error)          { return ff.f.Stat() }
func (ff *failingFile) Sync() error {
	if ff.failSync {
		return fmt.Errorf("injected sync error")
	}
	return ff.f.Sync()
}
func (ff *failingFile) Truncate(size int64) error { return ff.f.Truncate(size) }
func (ff *failingFile) Name() string              { return ff.f.Name() }

func TestAddStringsBatch_RollbackOnWriteFailure(t *testing.T) {
	dir := t.TempDir()
	// create a backing real file
	tmp, err := os.CreateTemp(dir, "string_mapping-*.log")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	// ensure file closed/removed at end
	defer os.Remove(tmp.Name())
	// wrap it with a failingFile initially failing on Write
	ff := &failingFile{f: tmp, failWrite: true}

	s := NewStringStore(nil, nil)
	// inject failing file handle
	s.logFile = ff

	inputs := []string{"x", "y"}
	ids, err := s.AddStringsBatch(inputs)
	if err == nil {
		t.Fatalf("expected AddStringsBatch to fail due to injected write error, got ids=%v", ids)
	}

	// Ensure no in-memory mappings exist after rollback
	for _, v := range inputs {
		if _, ok := s.GetID(v); ok {
			t.Fatalf("expected no id for %s after rollback", v)
		}
	}

	// nextID should be restored to 1 (no entries present)
	if s.nextID.Load() != 1 {
		t.Fatalf("expected nextID to be reset to 1 after rollback, got %d", s.nextID.Load())
	}

	// Now allow writes and try again
	ff.failWrite = false
	ids2, err2 := s.AddStringsBatch(inputs)
	if err2 != nil {
		t.Fatalf("AddStringsBatch failed on retry: %v", err2)
	}
	if len(ids2) != len(inputs) {
		t.Fatalf("expected %d ids on success, got %d", len(inputs), len(ids2))
	}
	// mappings should now exist
	for i, v := range inputs {
		id, ok := s.GetID(v)
		if !ok {
			t.Fatalf("expected id for %s after successful batch", v)
		}
		if id != ids2[i] {
			t.Fatalf("id mismatch for %s: expected %d got %d", v, ids2[i], id)
		}
	}

	// cleanup
	_ = s.Close()
}

func TestAddStringsBatch_RollbackOnSyncFailure(t *testing.T) {
	dir := t.TempDir()
	// create a backing real file
	tmp, err := os.CreateTemp(dir, "string_mapping-*.log")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	// ensure file closed/removed at end
	defer os.Remove(tmp.Name())
	// wrap it with a failingFile initially failing on Sync
	ff := &failingFile{f: tmp, failSync: true}

	s := NewStringStore(nil, nil)
	// inject failing file handle
	s.logFile = ff

	inputs := []string{"a", "b", "c"}
	ids, err := s.AddStringsBatch(inputs)
	if err == nil {
		t.Fatalf("expected AddStringsBatch to fail due to injected sync error, got ids=%v", ids)
	}

	// Ensure no in-memory mappings exist after rollback
	for _, v := range inputs {
		if _, ok := s.GetID(v); ok {
			t.Fatalf("expected no id for %s after rollback", v)
		}
	}

	// nextID should be restored to 1 (no entries present)
	if s.nextID.Load() != 1 {
		t.Fatalf("expected nextID to be reset to 1 after rollback, got %d", s.nextID.Load())
	}

	// Now allow Sync to succeed and try again
	ff.failSync = false
	ids2, err2 := s.AddStringsBatch(inputs)
	if err2 != nil {
		t.Fatalf("AddStringsBatch failed on retry after clearing sync error: %v", err2)
	}
	if len(ids2) != len(inputs) {
		t.Fatalf("expected %d ids on success, got %d", len(inputs), len(ids2))
	}
	// mappings should now exist
	for i, v := range inputs {
		id, ok := s.GetID(v)
		if !ok {
			t.Fatalf("expected id for %s after successful batch", v)
		}
		if id != ids2[i] {
			t.Fatalf("id mismatch for %s: expected %d got %d", v, ids2[i], id)
		}
	}

	// cleanup
	_ = s.Close()
}
