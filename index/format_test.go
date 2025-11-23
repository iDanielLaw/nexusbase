package index

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestWriteReadTOC(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.idx")

	symbols := []string{"__name__", "host", "region", "cpu"}
	series := []SeriesEntry{
		{
			LabelPairs: [][2]uint32{{1, 3}}, // host=cpu
			Chunks: []ChunkMeta{
				{Mint: 1000, Maxt: 1999, DataRef: 10},
				{Mint: 2000, Maxt: 2999, DataRef: 20},
			},
		},
		{
			LabelPairs: [][2]uint32{{0, 2}}, // __name__=region
			Chunks: []ChunkMeta{
				{Mint: 500, Maxt: 750, DataRef: 5},
			},
		},
	}

	if err := CreateIndexWithSections(p, symbols, series); err != nil {
		t.Fatalf("CreateIndexWithSections: %v", err)
	}

	// open and validate header
	f, err := os.Open(p)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := ValidateHeader(f); err != nil {
		t.Fatalf("ValidateHeader: %v", err)
	}

	// read TOC and then symbol table and series by offsets
	toc, err := ReadTOC(p)
	if err != nil {
		t.Fatalf("ReadTOC: %v", err)
	}
	if toc.Symbols == 0 || toc.Series == 0 {
		t.Fatalf("unexpected toc offsets: %+v", toc)
	}

	gotSymbols, err := ReadSymbolTable(f, int64(toc.Symbols))
	if err != nil {
		t.Fatalf("ReadSymbolTable: %v", err)
	}
	// The writer sorts symbols lexicographically. Compare against sorted input.
	wantSyms := make([]string, len(symbols))
	copy(wantSyms, symbols)
	sort.Strings(wantSyms)
	if len(gotSymbols) != len(wantSyms) {
		t.Fatalf("symbols count mismatch: got=%d want=%d", len(gotSymbols), len(wantSyms))
	}
	for i := range wantSyms {
		if gotSymbols[i] != wantSyms[i] {
			t.Fatalf("symbol[%d] mismatch: got=%q want=%q", i, gotSymbols[i], wantSyms[i])
		}
	}

	gotSeries, err := ReadSeriesSection(f, int64(toc.Series))
	if err != nil {
		t.Fatalf("ReadSeriesSection: %v", err)
	}
	if len(gotSeries) != len(series) {
		t.Fatalf("series count mismatch: got=%d want=%d", len(gotSeries), len(series))
	}
	// compare label pairs and chunk metadata
	for i := range series {
		if len(gotSeries[i].LabelPairs) != len(series[i].LabelPairs) {
			t.Fatalf("series[%d] label count mismatch", i)
		}
		for j := range series[i].LabelPairs {
			if gotSeries[i].LabelPairs[j] != series[i].LabelPairs[j] {
				t.Fatalf("series[%d].pair[%d] mismatch: got=%v want=%v", i, j, gotSeries[i].LabelPairs[j], series[i].LabelPairs[j])
			}
		}
		if len(gotSeries[i].Chunks) != len(series[i].Chunks) {
			t.Fatalf("series[%d] chunks count mismatch: got=%d want=%d", i, len(gotSeries[i].Chunks), len(series[i].Chunks))
		}
		for j := range series[i].Chunks {
			g := gotSeries[i].Chunks[j]
			w := series[i].Chunks[j]
			if g.Mint != w.Mint || g.Maxt != w.Maxt || g.DataRef != w.DataRef {
				t.Fatalf("series[%d].chunk[%d] mismatch: got=%+v want=%+v", i, j, g, w)
			}
		}
	}
	f.Close()
}
