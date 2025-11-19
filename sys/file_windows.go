// file_windows.go
//go:build windows

package sys

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

// windowsFileOpener implements FileOpener for Windows, using CreateFile with FILE_SHARE_DELETE.
type windowsFile struct{}

type windowsSafeRemoveOptions struct {
	Retry         int
	IntervalRetry time.Duration
}

func (wso *windowsSafeRemoveOptions) GetRetry() int {
	return wso.Retry
}

func (wso *windowsSafeRemoveOptions) GetIntervalRetry() time.Duration {
	return wso.IntervalRetry
}

// NewFileOpener returns a platform-specific FileOpener.
func NewFile() File {
	return &windowsFile{}
}

func (wfo *windowsFile) Create(name string) (*os.File, error) {
	return wfo.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

// OpenFile opens a file on Windows with specified flags and permissions.
// It specifically uses FILE_SHARE_DELETE to allow the file to be deleted or renamed
// while it is still open, which is crucial for LSM-tree compaction on Windows.
func (wfo *windowsFile) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	var access uint32
	var creationDisposition uint32
	var shareMode uint32 = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE // Allow delete/rename

	// Map os.OpenFile flags to Windows CreateFile access and creation disposition
	if flag&os.O_RDWR != 0 {
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	} else if flag&os.O_WRONLY != 0 {
		access = windows.GENERIC_WRITE
	} else { // This handles os.O_RDONLY (which is 0)
		access = windows.GENERIC_READ
	}

	if flag&os.O_CREATE != 0 {
		if flag&os.O_EXCL != 0 {
			creationDisposition = windows.CREATE_NEW
		} else {
			creationDisposition = windows.OPEN_ALWAYS
		}
	} else {
		creationDisposition = windows.OPEN_EXISTING
	}

	if flag&os.O_TRUNC != 0 {
		if creationDisposition == windows.OPEN_EXISTING {
			creationDisposition = windows.TRUNCATE_EXISTING
		} else {
			// If O_TRUNC is set with O_CREATE, it implies CREATE_ALWAYS
			creationDisposition = windows.CREATE_ALWAYS
		}
	}

	// Convert file path to UTF16 pointer
	pathp, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}

	// Call Windows CreateFile API
	handle, err := windows.CreateFile(
		pathp,
		access,
		shareMode,
		nil, // Default security attributes
		creationDisposition,
		windows.FILE_ATTRIBUTE_NORMAL,
		0, // No template file
	)
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			if errno == windows.ERROR_FILE_NOT_FOUND {
				return nil, os.ErrNotExist
			}
			if errno == windows.ERROR_ACCESS_DENIED {
				return nil, fmt.Errorf("windows CreateFile failed for %s: Access is denied: %w", name, err)
			}
		}
		return nil, fmt.Errorf("windows CreateFile failed for %s: %w", name, err)
	}

	// Convert the Windows handle to an *os.File
	// Convert the Windows handle to an *os.File
	file := os.NewFile(uintptr(handle), name)

	// หากมีการเปิดแบบ O_APPEND, ต้อง Seek ไปท้ายไฟล์หลังจากเปิด
	if flag&os.O_APPEND != 0 {
		_, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			// หาก Seek ล้มเหลว, ต้องปิดไฟล์และคืนค่า error
			file.Close()
			return nil, fmt.Errorf("ไม่สามารถ Seek ไปท้ายไฟล์เพื่อ append ได้: %w", err)
		}
	}

	return file, nil
}

func (wfo *windowsFile) Open(name string) (*os.File, error) {
	return wfo.OpenFile(name, os.O_RDONLY, 0)
}

// SafeRemove, retries remove
func (wfo *windowsFile) SafeRemove(name string) error {
	return wfo.SafeRemoveWithOption(name, &windowsSafeRemoveOptions{
		Retry:         5,
		IntervalRetry: 100 * time.Millisecond,
	})
}

func (wfo *windowsFile) SafeRemoveWithOption(name string, opts SafeRemoveOptions) error {
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

func (wfo *windowsFile) WriteFile(name string, data []byte, perm os.FileMode) error {
	f, err := wfo.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}

func (wfo *windowsFile) OpenWithRetry(path string, flag int, perm os.FileMode, maxRetries int, retryInterval time.Duration) (*os.File, error) {
	var file *os.File
	var err error

	i := 0
	for ; i < maxRetries; i++ {
		file, err = wfo.OpenFile(path, flag, perm)
		if err == nil {
			return file, nil
		} else if strings.Contains(err.Error(), "Access is denied") {
			time.Sleep(retryInterval * time.Duration(1<<i))
			continue
		}

		return nil, err
	}
	return nil, err
}

func (wfo *windowsFile) CreateTemp(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

func (wfo *windowsFile) NewFile(fd uintptr, name string) *os.File {
	return os.NewFile(fd, name)
}

func (wfo *windowsFile) OpenInRoot(dir, name string) (*os.File, error) {
	// Simple helper to open a file inside `dir` on Windows.
	return wfo.OpenFile(filepath.Join(dir, name), os.O_RDONLY, 0)
}

// Delay GC
func (wfo *windowsFile) GC() error {
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	return nil
}
