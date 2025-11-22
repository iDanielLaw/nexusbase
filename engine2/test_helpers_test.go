package engine2

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/INLOpen/nexusbase/index"
)

// findIndexFile searches the blocks directory under dataRoot and returns the
// first found `index.idx` path. Returns an error if none found.
func findIndexFile(dataRoot string) (string, error) {
	blocksDir := filepath.Join(dataRoot, "blocks")
	fis, err := os.ReadDir(blocksDir)
	if err != nil {
		return "", err
	}
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		p := filepath.Join(blocksDir, fi.Name(), "index.idx")
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", errors.New("no index.idx found")
}

// readIndexSymbols opens indexPath and returns the symbol table.
func readIndexSymbols(indexPath string) ([]string, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	toc, err := index.ReadTOC(indexPath)
	if err != nil {
		return nil, err
	}
	return index.ReadSymbolTable(f, int64(toc.Symbols))
}

// readIndexSeries returns the parsed series entries from the series section.
func readIndexSeries(indexPath string) ([]index.SeriesEntry, error) {
	f, err := os.Open(indexPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	toc, err := index.ReadTOC(indexPath)
	if err != nil {
		return nil, err
	}
	return index.ReadSeriesSection(f, int64(toc.Series))
}

// readChunkPayloadAt reads a length-prefixed chunk payload from chunks.dat at
// the given DataRef offset and returns the payload bytes.
func readChunkPayloadAt(blockDir string, dataRef uint64) ([]byte, error) {
	chunksPath := filepath.Join(blockDir, "chunks.dat")
	f, err := os.Open(chunksPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(int64(dataRef), io.SeekStart); err != nil {
		return nil, err
	}
	var l32 uint32
	if err := binaryReadLE(f, &l32); err != nil {
		return nil, err
	}
	buf := make([]byte, l32)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// binaryReadLE is a tiny wrapper to avoid importing encoding/binary in tests
// repeatedly; it reads a little-endian uint32 into dst.
func binaryReadLE(r io.Reader, dst *uint32) error {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return err
	}
	*dst = uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
	return nil
}
