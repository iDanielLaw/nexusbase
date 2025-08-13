package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/sys"
)

type helperSnapshot struct{}

var _ internal.PrivateSnapshotHelper = (*helperSnapshot)(nil)

func newHelperSnapshot() *helperSnapshot {
	return &helperSnapshot{}
}

func (h *helperSnapshot) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

func (h *helperSnapshot) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (h *helperSnapshot) MkdirTemp(dir, pattern string) (string, error) {
	return os.MkdirTemp(dir, pattern)
}

func (h *helperSnapshot) Rename(oldpath, newpath string) error {
	// Ensure the destination directory exists before renaming. This is a common
	// cross-platform issue, especially on Windows where rename can fail if the
	// parent directory of the new path does not exist.
	if err := h.MkdirAll(filepath.Dir(newpath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory for newpath %s: %w", newpath, err)
	}
	return os.Rename(oldpath, newpath)
}

func (h *helperSnapshot) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (h *helperSnapshot) Open(name string) (sys.FileInterface, error) {
	return sys.Open(name)
}

func (h *helperSnapshot) Create(name string) (sys.FileInterface, error) {
	// Ensure the parent directory exists before creating the file. This is crucial
	// for robustness, as os.Create will fail on some OSes (like Windows) if the
	// parent directory does not exist.
	if err := h.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory for %s: %w", name, err)
	}
	return sys.Create(name)
}

func (h *helperSnapshot) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (h *helperSnapshot) WriteFile(name string, data []byte, perm os.FileMode) error {
	// Ensure the parent directory exists before writing the file for robustness.
	if err := h.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory for %s: %w", name, err)
	}
	return sys.WriteFile(name, data, perm)
}

func (h *helperSnapshot) CopyDirectoryContents(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		// Skip the root directory itself.
		if relPath == "." {
			return nil
		}

		dstPath := filepath.Join(dst, relPath)

		if d.IsDir() {
			return h.MkdirAll(dstPath, 0755)
		}
		// CopyFile will create the parent directory if it doesn't exist.
		return h.CopyFile(path, dstPath)
	})
}

func (h *helperSnapshot) LinkOrCopyFile(src, dst string) error {
	// Ensure the destination directory exists before attempting to link.
	// This is crucial for robustness, especially on Windows where os.Create
	// (used by the CopyFile fallback) will fail if the parent dir is missing.
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory for link %s: %w", dst, err)
	}
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	// If linking fails (e.g., across different filesystems), fall back to a standard file copy.
	return h.CopyFile(src, dst)
}

func (h *helperSnapshot) LinkOrCopyDirectoryContents(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path for %s: %w", path, err)
		}

		// Skip the root directory itself.
		if relPath == "." {
			return nil
		}

		dstPath := filepath.Join(dst, relPath)

		if d.IsDir() {
			return h.MkdirAll(dstPath, 0755)
		}
		// LinkOrCopyFile will create the parent directory if it doesn't exist.
		return h.LinkOrCopyFile(path, dstPath)
	})
}

// CopyFile copies a file from src to dst.
func (h *helperSnapshot) CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer in.Close()

	// This is slightly redundant if called from LinkOrCopyFile, but makes
	// CopyFile safe to call directly. The performance impact is negligible.
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory for %s: %w", dst, err)
	}

	out, err := sys.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s: %w", src, dst, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("failed to close destination file %s: %w", dst, err)
	}
	return nil
}

func (h *helperSnapshot) ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
	return ReadManifestBinary(r)
}

func (h *helperSnapshot) CopyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
	if srcPath == "" {
		logger.Debug("Source path for auxiliary file is empty, skipping copy.", "file", destFileName)
		return nil
	}
	_, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) || errors.Is(err, syscall.ENOENT) { // เพิ่ม errors.Is(err, syscall.ENOENT)
			logger.Warn("Source file does not exist, skipping copy for snapshot.", "path", srcPath)
			return nil
		}
		return fmt.Errorf("failed to stat source file %s for auxiliary copy: %w", srcPath, err)
	}
	destPath := filepath.Join(snapshotDir, destFileName)
	if err := h.LinkOrCopyFile(srcPath, destPath); err != nil {
		return fmt.Errorf("failed to link or copy %s to snapshot: %w", destFileName, err)
	}
	*manifestField = destFileName
	logger.Info("Copied auxiliary file to snapshot.", "source", srcPath, "destination", destPath)
	return nil
}

func (h *helperSnapshot) SaveJSON(v interface{}, path string) error {
	// Ensure the destination directory exists before writing the file.
	// This is crucial for robustness, especially on Windows where os.WriteFile
	// will fail if the parent directory does not exist.
	if err := h.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory for json file %s: %w", path, err)
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return h.WriteFile(path, data, 0644)
}
