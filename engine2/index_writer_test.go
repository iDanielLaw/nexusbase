package engine2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteBlockIndexNamedCreatesFile(t *testing.T) {
	dir := t.TempDir()
	blockDir := filepath.Join(dir, "blocks", "1")
	if err := os.MkdirAll(blockDir, 0o755); err != nil {
		t.Fatalf("mkdir blockDir: %v", err)
	}

	// Call helper with no series (minimal index)
	if err := WriteBlockIndexNamed(blockDir, nil); err != nil {
		t.Fatalf("WriteBlockIndexNamed error: %v", err)
	}

	idxPath := filepath.Join(blockDir, "index.idx")
	fi, err := os.Stat(idxPath)
	if err != nil {
		t.Fatalf("expected index file at %s: %v", idxPath, err)
	}
	if fi.Size() == 0 {
		t.Fatalf("index file %s has zero size", idxPath)
	}
}
