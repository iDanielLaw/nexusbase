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
type GCFileHandler func() error

func init() {
	debugMode.Store(false)
	file := NewFile()
	defaultFile.Store(&file)
}

func SetDefaultFile(file File) {
	defaultFile.Store(&file)
}

func SetDebugMode(mode bool) {
	// debugMode.Store(mode)
}

var Create CreateHandler = (func(name string) (FileInterface, error) {
	file := (*defaultFile.Load())
	if file == nil {
		return nil, os.ErrInvalid
	}

	if debugMode.Load() {
		return DCreate(file, name)
	}
	return RCreate(file, name)
})

var Open OpenHandler = (func(name string) (FileInterface, error) {
	file := (*defaultFile.Load())
	if file == nil {
		return nil, os.ErrInvalid
	}
	if debugMode.Load() {
		return DOpen(file, name)
	}
	return ROpen(file, name)
})

var OpenFile OpenFileHandler = (func(name string, flag int, perm os.FileMode) (FileInterface, error) {
	file := (*defaultFile.Load())
	if file == nil {
		return nil, os.ErrInvalid
	}
	if debugMode.Load() {
		return DOpenFile(file, name, flag, perm)
	}
	return ROpenFile(file, name, flag, perm)
})

var GC GCFileHandler = (func() error {
	file := (*defaultFile.Load())
	if file == nil {
		return os.ErrInvalid
	}
	return file.GC()
})
