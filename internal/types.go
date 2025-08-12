package internal

import (
	"io"
	"log/slog"
	"os"

	"github.com/INLOpen/nexusbase/core"
)

// PrivateSnapshotHelper defines an interface for file system operations,
// allowing them to be mocked in tests.
type PrivateSnapshotHelper interface {
	RemoveAll(path string) error
	ReadFile(name string) ([]byte, error)
	MkdirTemp(dir, pattern string) (string, error)
	Rename(oldpath, newpath string) error
	Stat(name string) (os.FileInfo, error)
	Open(name string) (*os.File, error)
	Create(name string) (*os.File, error) // Added
	MkdirAll(path string, perm os.FileMode) error
	WriteFile(name string, data []byte, perm os.FileMode) error
	CopyDirectoryContents(src, dst string) error
	LinkOrCopyFile(src, dst string) error
	LinkOrCopyDirectoryContents(src, dst string) error
	CopyFile(src, dst string) error
	ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error)
	ReadDir(name string) ([]os.DirEntry, error) // Added
	CopyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error
	SaveJSON(v interface{}, path string) error
}
