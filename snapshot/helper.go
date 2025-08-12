package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal"
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
	return os.Rename(oldpath, newpath)
}

func (h *helperSnapshot) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (h *helperSnapshot) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (h *helperSnapshot) Create(name string) (*os.File, error) {
	return os.Create(name)
}

func (h *helperSnapshot) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (h *helperSnapshot) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

func (h *helperSnapshot) CopyDirectoryContents(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read source directory %s: %w", src, err)
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := os.MkdirAll(dstPath, 0755); err != nil {
				return fmt.Errorf("failed to create destination subdirectory %s: %w", dstPath, err)
			}
			if err := h.CopyDirectoryContents(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := h.CopyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to copy file from %s to %s: %w", srcPath, dstPath, err)
			}
		}
	}
	return nil
}

func (h *helperSnapshot) LinkOrCopyFile(src, dst string) error {
	err := os.Link(src, dst)
	if err == nil {
		return nil
	}
	return h.CopyFile(src, dst)
}

func (h *helperSnapshot) LinkOrCopyDirectoryContents(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read source directory %s: %w", src, err)
	}
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := os.MkdirAll(dstPath, 0755); err != nil {
				return fmt.Errorf("failed to create destination subdirectory %s: %w", dstPath, err)
			}
			if err := h.LinkOrCopyDirectoryContents(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := h.LinkOrCopyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to link or copy file from %s to %s: %w", srcPath, dstPath, err)
			}
		}
	}
	return nil
}

// CopyFile copies a file from src to dst.
func (h *helperSnapshot) CopyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func (h *helperSnapshot) ReadManifestBinary(r io.Reader) (*core.SnapshotManifest, error) {
	return ReadManifestBinary(r)
}

func (h *helperSnapshot) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (h *helperSnapshot) CopyAuxiliaryFile(srcPath, destFileName, snapshotDir string, manifestField *string, logger *slog.Logger) error {
	if srcPath == "" {
		logger.Debug("Source path for auxiliary file is empty, skipping copy.", "file", destFileName)
		return nil
	}
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		logger.Warn("Source file does not exist, skipping copy for snapshot.", "path", srcPath)
		return nil
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
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return h.WriteFile(path, data, 0644)
}
