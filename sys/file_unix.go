// file_unix.go
//go:build unix

package sys

import (
	"os"
	"path/filepath"
	"time"
)

// unixFileOpener implements FileOpener for Unix-like systems, using os.OpenFile directly.
type unixFile struct{}

type unixSafeRemoveOptions struct {
	Retry         int
	IntervalRetry time.Duration
}

func (wso *unixSafeRemoveOptions) GetRetry() int {
	return wso.Retry
}

func (wso *unixSafeRemoveOptions) GetIntervalRetry() time.Duration {
	return wso.IntervalRetry
}

// NewFileOpener returns a platform-specific FileOpener.
func NewFile() File {
	return &unixFile{}
}

func (ufo *unixFile) Create(name string) (*os.File, error) {
	return os.Create(name)
}

// OpenFile simply calls os.OpenFile, as Unix-like systems handle file deletion
// while open differently (allowing it).
func (ufo *unixFile) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Open simply calls os.Open
func (ufo *unixFile) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (ufo *unixFile) OpenWithRetry(path string, flag int, perm os.FileMode, maxRetries int, retryInterval time.Duration) (*os.File, error) {
	return ufo.OpenFile(path, flag, perm)
}

func (ufo *unixFile) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

func (ufo *unixFile) NewFile(fd uintptr, name string) *os.File {
	return os.NewFile(fd, name)
}

func (ufo *unixFile) OpenInRoot(dir, name string) (*os.File, error) {
	// Open a file inside the provided directory. This is a thin helper
	// that uses filepath.Join; callers that require symlink or security
	// checks should perform them separately.
	return os.OpenFile(filepath.Join(dir, name), os.O_RDONLY, 0)
}

// SafeRemove, retries remove
func (ufo *unixFile) SafeRemove(name string) error {
	return ufo.SafeRemoveWithOption(name, &unixSafeRemoveOptions{
		Retry:         5,
		IntervalRetry: 100 * time.Millisecond,
	})
}

func (ufo *unixFile) SafeRemoveWithOption(name string, opts SafeRemoveOptions) error {
	var err error
	retry := opts.GetRetry()
	if retry < 1 || retry > 5 {
		retry = 5
	}

	for i := 0; i < retry; i++ {
		err = os.Remove(name)
		if err == nil || os.IsNotExist(err) {
			return nil // Success if file is removed or already doesn't exist
		}
		time.Sleep(opts.GetIntervalRetry() * time.Duration(1<<i))
	}
	return err
}

func (ufo *unixFile) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

// Nothing
func (ufo *unixFile) GC() error {
	return nil
}
