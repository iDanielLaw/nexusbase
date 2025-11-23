package engine2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/INLOpen/nexusbase/index"
)

// TestIndexRoundTrip writes an index using WriteBlockIndexNamed and then
// reads it back using the index package readers to ensure symbol and
// series information round-trips correctly.
func TestIndexRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "index.idx")

	named := []NamedSeries{
		{Labels: map[string]string{"host": "server1", "region": "us-east"}},
		{Labels: map[string]string{"host": "server2", "region": "us-west"}},
	}

	if err := WriteBlockIndexNamed(tmp, named); err != nil {
		t.Fatalf("WriteBlockIndexNamed: %v", err)
	}

	// ensure file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("index file missing: %v", err)
	}

	toc, err := index.ReadTOC(path)
	if err != nil {
		t.Fatalf("ReadTOC: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer f.Close()

	syms, err := index.ReadSymbolTable(f, int64(toc.Symbols))
	if err != nil {
		t.Fatalf("ReadSymbolTable: %v", err)
	}

	series, err := index.ReadSeriesSection(f, int64(toc.Series))
	if err != nil {
		t.Fatalf("ReadSeriesSection: %v", err)
	}

	if len(series) != len(named) {
		t.Fatalf("series count mismatch: got=%d want=%d", len(series), len(named))
	}

	// Compare each series by reconstructing label maps from label pairs.
	for i, s := range series {
		got := make(map[string]string, len(s.LabelPairs))
		for _, p := range s.LabelPairs {
			name := ""
			val := ""
			if int(p[0]) < len(syms) {
				name = syms[p[0]]
			}
			if int(p[1]) < len(syms) {
				val = syms[p[1]]
			}
			got[name] = val
		}

		want := named[i].Labels

		if len(got) != len(want) {
			t.Fatalf("labels count mismatch for series %d: got=%v want=%v", i, got, want)
		}
		for k, v := range want {
			if gv, ok := got[k]; !ok || gv != v {
				t.Fatalf("label mismatch series %d key=%s: got=%q want=%q", i, k, gv, v)
			}
		}
	}
}
