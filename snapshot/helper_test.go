package snapshot

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupHelperTest creates a temporary directory for tests.
func setupHelperTest(t *testing.T) (h *helperSnapshot, tempDir string) {
	t.Helper()
	tempDir = t.TempDir()
	h = newHelperSnapshot()
	return h, tempDir
}

func TestHelperSnapshot_FileOps(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	filePath := filepath.Join(tempDir, "test.txt")
	renamedPath := filepath.Join(tempDir, "renamed.txt")
	content := []byte("hello world")

	// WriteFile
	err := h.WriteFile(filePath, content, 0644)
	require.NoError(t, err)

	// Stat
	info, err := h.Stat(filePath)
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), info.Size())

	// ReadFile
	readContent, err := h.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, content, readContent)

	// Open
	f, err := h.Open(filePath)
	require.NoError(t, err)
	f.Close()

	// Rename
	err = h.Rename(filePath, renamedPath)
	require.NoError(t, err)
	_, err = h.Stat(filePath)
	assert.True(t, os.IsNotExist(err), "Original file should not exist after rename")
	_, err = h.Stat(renamedPath)
	assert.NoError(t, err, "Renamed file should exist")

	// RemoveAll (on a file)
	err = h.RemoveAll(renamedPath)
	require.NoError(t, err)
	_, err = h.Stat(renamedPath)
	assert.True(t, os.IsNotExist(err), "File should not exist after RemoveAll")
}

func TestHelperSnapshot_DirOps(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	nestedDir := filepath.Join(tempDir, "a", "b", "c")

	// MkdirAll
	err := h.MkdirAll(nestedDir, 0755)
	require.NoError(t, err)
	info, err := h.Stat(nestedDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// MkdirTemp
	tempSubDir, err := h.MkdirTemp(tempDir, "sub-")
	require.NoError(t, err)
	info, err = h.Stat(tempSubDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// RemoveAll (on a directory)
	err = h.RemoveAll(nestedDir)
	require.NoError(t, err)
	_, err = h.Stat(nestedDir)
	assert.True(t, os.IsNotExist(err))
}

func TestHelperSnapshot_CopyFile(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	srcPath := filepath.Join(tempDir, "source.txt")
	dstPath := filepath.Join(tempDir, "dest.txt")
	content := []byte("copy me")

	require.NoError(t, h.WriteFile(srcPath, content, 0644))

	// Happy path
	err := h.CopyFile(srcPath, dstPath)
	require.NoError(t, err)

	copiedContent, err := h.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, content, copiedContent)

	// Error path: source does not exist
	err = h.CopyFile(filepath.Join(tempDir, "nonexistent.txt"), dstPath)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestHelperSnapshot_LinkOrCopyFile(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	srcPath := filepath.Join(tempDir, "source.txt")
	dstPath := filepath.Join(tempDir, "dest.txt")
	content := []byte("link or copy me")

	require.NoError(t, h.WriteFile(srcPath, content, 0644))

	err := h.LinkOrCopyFile(srcPath, dstPath)
	require.NoError(t, err, "LinkOrCopyFile should succeed")

	// Verify content
	copiedContent, err := h.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, content, copiedContent)

	// Verify file info to see if it's a link (on systems that support it)
	srcInfo, _ := os.Lstat(srcPath)
	dstInfo, _ := os.Lstat(dstPath)
	if os.SameFile(srcInfo, dstInfo) {
		t.Log("Successfully created a hard link.")
	} else {
		t.Log("File was copied instead of linked (expected on some filesystems).")
	}
}

func TestHelperSnapshot_CopyDirectoryContents(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	srcDir := filepath.Join(tempDir, "src")
	dstDir := filepath.Join(tempDir, "dst")
	require.NoError(t, h.MkdirAll(srcDir, 0755))
	require.NoError(t, h.MkdirAll(dstDir, 0755))
	require.NoError(t, h.MkdirAll(filepath.Join(srcDir, "subdir"), 0755))

	require.NoError(t, h.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("file1"), 0644))
	require.NoError(t, h.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("file2"), 0644))

	err := h.CopyDirectoryContents(srcDir, dstDir)
	require.NoError(t, err)

	assert.FileExists(t, filepath.Join(dstDir, "file1.txt"))
	assert.FileExists(t, filepath.Join(dstDir, "subdir", "file2.txt"))

	content, err := h.ReadFile(filepath.Join(dstDir, "subdir", "file2.txt"))
	require.NoError(t, err)
	assert.Equal(t, "file2", string(content))
}

func TestHelperSnapshot_SaveJSON(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	filePath := filepath.Join(tempDir, "test.json")
	data := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{Name: "test", Age: 99}

	err := h.SaveJSON(data, filePath)
	require.NoError(t, err)

	readBytes, err := h.ReadFile(filePath)
	require.NoError(t, err)

	var decodedData struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	err = json.Unmarshal(readBytes, &decodedData)
	require.NoError(t, err)
	assert.Equal(t, data, decodedData)
}

func TestHelperSnapshot_CopyAuxiliaryFile(t *testing.T) {
	h, tempDir := setupHelperTest(t)
	srcPath := filepath.Join(tempDir, "aux.log")
	snapshotDir := filepath.Join(tempDir, "snapshot")
	// Explicitly create the parent directory for the source file.
	// This is a defensive measure to ensure the test is robust, especially on
	// Windows, and directly addresses the "file not found" error if the
	// srcPath were to be in a subdirectory that doesn't exist.
	require.NoError(t, h.MkdirAll(filepath.Dir(srcPath), 0755))
	require.NoError(t, h.MkdirAll(snapshotDir, 0755))
	require.NoError(t, h.WriteFile(srcPath, []byte("aux data"), 0644))

	var manifestField string
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Happy path
	err := h.CopyAuxiliaryFile(srcPath, "aux_copy.log", snapshotDir, &manifestField, logger)
	require.NoError(t, err)
	assert.Equal(t, "aux_copy.log", manifestField)
	assert.FileExists(t, filepath.Join(snapshotDir, "aux_copy.log"))

	// Source does not exist
	manifestField = ""
	err = h.CopyAuxiliaryFile("/non/existent/path", "nonexistent.log", snapshotDir, &manifestField, logger)
	require.NoError(t, err, "Should not return an error if source file does not exist")
	assert.Empty(t, manifestField, "Manifest field should not be set if source file does not exist")
}

func TestHelperSnapshot_ReadManifestBinary(t *testing.T) {
	h, _ := setupHelperTest(t)
	manifest := &core.SnapshotManifest{
		SequenceNumber: 42,
		Levels: []core.SnapshotLevelManifest{
			{LevelNumber: 0, Tables: []core.SSTableMetadata{{ID: 1, FileName: "1.sst"}}},
		},
	}

	var buf bytes.Buffer
	err := WriteManifestBinary(&buf, manifest)
	require.NoError(t, err)

	decodedManifest, err := h.ReadManifestBinary(&buf)
	require.NoError(t, err)
	require.NotNil(t, decodedManifest)
	assert.Equal(t, manifest.SequenceNumber, decodedManifest.SequenceNumber)
	require.Len(t, decodedManifest.Levels, 1)
	require.Len(t, decodedManifest.Levels[0].Tables, 1)
	assert.Equal(t, manifest.Levels[0].Tables[0].ID, decodedManifest.Levels[0].Tables[0].ID)
}
