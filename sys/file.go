package sys

import (
	"io"
	"os"
	"sync/atomic"
	"time"
)

// fileWrapper is a stable concrete type used to store the File interface
// inside an atomic.Value. atomic.Value requires that all stored values
// have the same concrete type; wrapping the File interface in this small
// struct ensures we can swap different File implementations safely.
type fileWrapper struct {
	f File
}

// defaultFile stores the current platform `File` implementation wrapped in a
// concrete `fileWrapper`. We store `fileWrapper` (not the interface) so that
// `atomic.Value` always sees the same concrete type across stores.
var defaultFile atomic.Value // stores fileWrapper
var debugMode atomic.Bool

// FileOpener defines an interface for opening files with specific sharing modes.
// This is used to abstract platform-specific file opening behaviors,
// especially for handling file locking on Windows.
type File interface {
	Create(name string) (*os.File, error)
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	OpenWithRetry(path string, flag int, perm os.FileMode, maxRetries int, retryInterval time.Duration) (*os.File, error)
	SafeRemove(name string) error
	SafeRemoveWithOption(name string, opts SafeRemoveOptions) error

	WriteFile(name string, data []byte, perm os.FileMode) error

	GC() error
	// Convenience helpers
	CreateTemp(dir, pattern string) (*os.File, error)
	NewFile(fd uintptr, name string) *os.File
	OpenInRoot(dir, name string) (*os.File, error)
}

type FileHandle interface {
	io.ReadWriteCloser
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.ReaderFrom
	io.WriterTo
	io.StringWriter

	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Name() string
}

type SafeRemoveOptions interface {
	GetRetry() int
	GetIntervalRetry() time.Duration
}

type CreateHandler func(name string) (FileHandle, error)
type OpenHandler func(name string) (FileHandle, error)
type OpenFileHandler func(name string, flag int, perm os.FileMode) (FileHandle, error)
type WriteFileHandler func(name string, data []byte, perm os.FileMode) error
type GCFileHandler func() error
type RemoveHandler func(name string) error

func init() {
	debugMode.Store(false)
	file := NewFile()
	defaultFile.Store(fileWrapper{f: file})
}

func SetDefaultFile(file File) {
	// Store a pointer to the provided File value. Using a pointer
	// Store the provided File value atomically wrapped in fileWrapper.
	defaultFile.Store(fileWrapper{f: file})
}

func SetDebugMode(mode bool) {
	debugMode.Store(mode)
}

var Create CreateHandler = (func(name string) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f

	if debugMode.Load() {
		return DCreate(file, name)
	}
	return RCreate(file, name)
})

var Open OpenHandler = (func(name string) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f
	if debugMode.Load() {
		return DOpen(file, name)
	}
	return ROpen(file, name)
})

var OpenFile OpenFileHandler = (func(name string, flag int, perm os.FileMode) (FileHandle, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return nil, os.ErrInvalid
	}
	file := fw.f
	if debugMode.Load() {
		return DOpenFile(file, name, flag, perm)
	}
	return ROpenFile(file, name, flag, perm)
})

var GC GCFileHandler = (func() error {
	p := defaultFile.Load()
	if p == nil {
		return os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return os.ErrInvalid
	}
	return fw.f.GC()
})

var WriteFile WriteFileHandler = (func(name string, data []byte, perm os.FileMode) error {
	p := defaultFile.Load()
	if p == nil {
		return os.ErrInvalid
	}
	fw, ok := p.(fileWrapper)
	if !ok || fw.f == nil {
		return os.ErrInvalid
	}
	return fw.f.WriteFile(name, data, perm)
})

var Remove RemoveHandler = (func(name string) error {
	return os.Remove(name)
})
