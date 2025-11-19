package sys

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// mockFile implements the File interface for testing. It delegates to the real
// OS file operations but records which methods were called.
type mockFile struct {
	dir              string
	CreateCalled     bool
	OpenCalled       bool
	OpenFileCalled   bool
	OpenRetryCalled  bool
	WriteFileCalled  bool
	SafeRemoveCalled bool
	GCCalled         bool
}

func (m *mockFile) Create(name string) (*os.File, error) {
	m.CreateCalled = true
	return os.Create(name)
}

func (m *mockFile) Open(name string) (*os.File, error) {
	m.OpenCalled = true
	return os.Open(name)
}

func (m *mockFile) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	m.OpenFileCalled = true
	return os.OpenFile(name, flag, perm)
}

func (m *mockFile) OpenWithRetry(path string, flag int, perm os.FileMode, maxRetries int, retryInterval time.Duration) (*os.File, error) {
	m.OpenRetryCalled = true
	return os.OpenFile(path, flag, perm)
}

func (m *mockFile) SafeRemove(name string) error {
	m.SafeRemoveCalled = true
	return os.Remove(name)
}

func (m *mockFile) SafeRemoveWithOption(name string, opts SafeRemoveOptions) error {
	m.SafeRemoveCalled = true
	return os.Remove(name)
}

func (m *mockFile) WriteFile(name string, data []byte, perm os.FileMode) error {
	m.WriteFileCalled = true
	return os.WriteFile(name, data, perm)
}

func (m *mockFile) GC() error {
	m.GCCalled = true
	return nil
}

func (m *mockFile) CreateTemp(dir, pattern string) (*os.File, error) {
	m.CreateCalled = true
	// Respect provided dir if given, otherwise use default
	if dir == "" {
		return os.CreateTemp(m.dir, pattern)
	}
	return os.CreateTemp(dir, pattern)
}

func (m *mockFile) NewFile(fd uintptr, name string) *os.File {
	return os.NewFile(fd, name)
}

func (m *mockFile) OpenInRoot(dir, name string) (*os.File, error) {
	m.OpenCalled = true
	return os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, 0644)
}

// TestSetDefaultFileAndDebugMode verifies that handlers use the configured
// default File implementation and that enabling debug mode returns a wrapper.
func TestSetDefaultFileAndDebugMode(t *testing.T) {
	tempDir := t.TempDir()

	// Backup original and restore at end
	origAny := defaultFile.Load()
	var orig File
	if origAny != nil {
		if v, ok := origAny.(File); ok {
			orig = v
		}
	}
	defer func() {
		if orig != nil {
			SetDefaultFile(orig)
		}
		SetDebugMode(false)
	}()

	// Install mock
	mf := &mockFile{dir: tempDir}
	SetDefaultFile(mf)

	// Use full paths so mocks write to the temp dir predictable location
	createPath := filepath.Join(tempDir, "create.txt")
	fi, err := Create(createPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !mf.CreateCalled && !mf.OpenFileCalled {
		t.Fatalf("expected mock Create/OpenFile to be called, none were")
	}
	// Write some data using the returned FileHandle
	data := []byte("testing123")
	_, err = fi.Write(data)
	if err != nil {
		fi.Close()
		t.Fatalf("Write on created file failed: %v", err)
	}
	fi.Close()

	// Verify the file was created and contains the data
	b, err := os.ReadFile(createPath)
	if err != nil {
		t.Fatalf("failed to read created file: %v", err)
	}
	if !bytes.Equal(b, data) {
		t.Fatalf("created file content mismatch: got %q want %q", string(b), string(data))
	}

	// Test WriteFile handler
	wfPath := filepath.Join(tempDir, "wf.txt")
	err = WriteFile(wfPath, []byte("hello"), 0644)
	if err != nil {
		t.Fatalf("WriteFile handler failed: %v", err)
	}
	if !mf.WriteFileCalled {
		t.Fatalf("expected mock WriteFileCalled to be true")
	}

	// Test GC handler
	if err := GC(); err != nil {
		t.Fatalf("GC handler returned error: %v", err)
	}
	if !mf.GCCalled {
		t.Fatalf("expected mock GCCalled to be true")
	}

	// Now enable debug mode and ensure Create returns a non-nil FileHandle
	SetDebugMode(true)
	dbgPath := filepath.Join(tempDir, "dbg.txt")
	df, err := Create(dbgPath)
	if err != nil {
		t.Fatalf("Create with debug mode failed: %v", err)
	}
	// Basic smoke: Name should contain the filename
	name := df.Name()
	if name == "" {
		df.Close()
		t.Fatalf("debug wrapper returned empty Name()")
	}
	df.Close()
}
