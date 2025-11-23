package engine2

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/sys"
)

func TestWriteChunksFile_RenameFallback(t *testing.T) {
	blockDir := t.TempDir()
	// prepare samples map with a single small series
	samples := make(map[string]*bytes.Buffer)
	buf := &bytes.Buffer{}
	// write one record: ts + uvarint len + payload
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(12345))
	buf.Write(ts)
	p := []byte("payload")
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, uint64(len(p)))
	buf.Write(tmp[:n])
	buf.Write(p)
	samples["metric|tags"] = buf

	// override sys rename impl to force failure and trigger copy fallback
	old := sys.GetRenameImpl()
	sys.SetRenameImpl(func(old, new string) error { return os.ErrPermission })
	defer sys.SetRenameImpl(old)

	refs, err := writeChunksFile(blockDir, samples, 1024)
	if err != nil {
		t.Fatalf("writeChunksFile failed: %v", err)
	}
	if refs == nil || len(refs) == 0 {
		t.Fatalf("expected refs for series, got nil/empty")
	}
	// chunks.dat should exist in blockDir
	if _, err := os.Stat(filepath.Join(blockDir, "chunks.dat")); err != nil {
		t.Fatalf("expected chunks.dat to exist, err=%v", err)
	}
}
