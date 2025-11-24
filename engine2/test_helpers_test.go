package engine2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/core"
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

// payloadTimestampRange scans a chunk payload (our internal record format)
// and returns the observed min and max timestamps.
func payloadTimestampRange(payload []byte) (int64, int64, error) {
	if len(payload) == 0 {
		return 0, 0, errors.New("empty payload")
	}
	r := bytes.NewReader(payload)
	var pMin, pMax int64
	first := true
	for r.Len() > 0 {
		var tsb [8]byte
		if _, err := io.ReadFull(r, tsb[:]); err != nil {
			return 0, 0, err
		}
		ts := int64(binary.BigEndian.Uint64(tsb[:]))
		l, err := binary.ReadUvarint(r)
		if err != nil {
			return 0, 0, err
		}
		if _, err := r.Seek(int64(l), io.SeekCurrent); err != nil {
			return 0, 0, err
		}
		if first {
			pMin = ts
			pMax = ts
			first = false
		} else {
			if ts < pMin {
				pMin = ts
			}
			if ts > pMax {
				pMax = ts
			}
		}
	}
	if first {
		return 0, 0, errors.New("no samples in payload")
	}
	return pMin, pMax, nil
}

// findSeriesByLabels searches the provided series entries using the symbol
// table and returns the first SeriesEntry matching all expected labels (map).
func findSeriesByLabels(series []index.SeriesEntry, syms []string, expected map[string]string) *index.SeriesEntry {
	for _, se := range series {
		labels := make(map[string]string)
		for _, p := range se.LabelPairs {
			if int(p[0]) >= len(syms) || int(p[1]) >= len(syms) {
				continue
			}
			labels[syms[int(p[0])]] = syms[int(p[1])]
		}
		ok := true
		for k, v := range expected {
			if labels[k] != v {
				ok = false
				break
			}
		}
		if ok {
			tmp := se
			return &tmp
		}
	}
	return nil
}

// HelperFieldValueValidateFloat64 retrieves a float64 field and fails the test if not found or wrong type.
func HelperFieldValueValidateFloat64(t *testing.T, fields core.FieldValues, selected string) float64 {
	t.Helper()
	if fields == nil {
		t.Fatalf("expected FieldValues, got nil")
	}
	pv, ok := fields[selected]
	if !ok {
		t.Fatalf("field '%s' not found in FieldValues", selected)
	}
	v, ok := pv.ValueFloat64()
	if !ok {
		t.Fatalf("field '%s' is not float64", selected)
	}
	return v
}

// HelperFieldValueValidateInt64 retrieves an int64 field and fails the test if not found or wrong type.
func HelperFieldValueValidateInt64(t *testing.T, fields core.FieldValues, selected string) int64 {
	t.Helper()
	if fields == nil {
		t.Fatalf("expected FieldValues, got nil")
	}
	pv, ok := fields[selected]
	if !ok {
		t.Fatalf("field '%s' not found in FieldValues", selected)
	}
	v, ok := pv.ValueInt64()
	if !ok {
		t.Fatalf("field '%s' is not int64", selected)
	}
	return v
}

// HelperFieldValueValidateString retrieves a string field and fails the test if not found or wrong type.
func HelperFieldValueValidateString(t *testing.T, fields core.FieldValues, selected string) string {
	t.Helper()
	if fields == nil {
		t.Fatalf("expected FieldValues, got nil")
	}
	pv, ok := fields[selected]
	if !ok {
		t.Fatalf("field '%s' not found in FieldValues", selected)
	}
	v, ok := pv.ValueString()
	if !ok {
		t.Fatalf("field '%s' is not string", selected)
	}
	return v
}
