package sys

import (
	"io"
	"os"
	"sync/atomic"
	"time"
)

var defaultFile atomic.Pointer[File]
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
}

type FileInterface interface {
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

type CreateHandler func(name string) (FileInterface, error)
type OpenHandler func(name string) (FileInterface, error)
type OpenFileHandler func(name string, flag int, perm os.FileMode) (FileInterface, error)
type WriteFileHandler func(name string, data []byte, perm os.FileMode) error
type GCFileHandler func() error
type RemoveHandler func(name string) error

func init() {
	debugMode.Store(false)
	file := NewFile()
	defaultFile.Store(&file)
}

func SetDefaultFile(file File) {
	// Store a pointer to the provided File value. Using a pointer
	// allows atomic load/store of the interface value across goroutines.
	defaultFile.Store(&file)
}

func SetDebugMode(mode bool) {
	debugMode.Store(mode)
}

var Create CreateHandler = (func(name string) (FileInterface, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	file := *p

	if debugMode.Load() {
		return DCreate(file, name)
	}
	return RCreate(file, name)
})

var Open OpenHandler = (func(name string) (FileInterface, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	file := *p
	if debugMode.Load() {
		return DOpen(file, name)
	}
	return ROpen(file, name)
})

var OpenFile OpenFileHandler = (func(name string, flag int, perm os.FileMode) (FileInterface, error) {
	p := defaultFile.Load()
	if p == nil {
		return nil, os.ErrInvalid
	}
	file := *p
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
	file := *p
	return file.GC()
})

var WriteFile WriteFileHandler = (func(name string, data []byte, perm os.FileMode) error {
	p := defaultFile.Load()
	if p == nil {
		return os.ErrInvalid
	}
	file := *p
	return file.WriteFile(name, data, perm)
})

var Remove RemoveHandler = (func(name string) error {
	return os.Remove(name)
})
