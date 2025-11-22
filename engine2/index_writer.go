package engine2

import (
	"path/filepath"
	"sort"

	"github.com/INLOpen/nexusbase/index"
)

// WriteBlockIndex writes a block-level index file named `index.idx` inside
// the provided block directory. It delegates to `index.CreateIndexWithSections`.
// This helper centralizes the location and filename used for block index files
// so callers can simply provide the block directory and the symbol/series data.
func WriteBlockIndex(blockDir string, symbols []string, series []index.SeriesEntry) error {
	path := filepath.Join(blockDir, "index.idx")
	return index.CreateIndexWithSections(path, symbols, series)
}

// NamedSeries represents a series using string label names and values.
type NamedSeries struct {
	Labels map[string]string
	Chunks []index.ChunkMeta
}

// WriteBlockIndexNamed is a convenience wrapper that accepts series label sets
// as strings. It builds a deterministic symbol table (names + values), maps
// labels to symbol indices, and delegates to CreateIndexWithSections.
func WriteBlockIndexNamed(blockDir string, named []NamedSeries) error {
	// Collect unique symbols (both names and values).
	symSet := make(map[string]struct{})
	for _, s := range named {
		for k, v := range s.Labels {
			symSet[k] = struct{}{}
			symSet[v] = struct{}{}
		}
	}

	symbols := make([]string, 0, len(symSet))
	for s := range symSet {
		symbols = append(symbols, s)
	}
	// Sort deterministically — CreateIndexWithSections also sorts, but we
	// must compute indices here to populate series entries.
	sort.Strings(symbols)

	// Build mapping string -> index (uint32)
	symIndex := make(map[string]uint32, len(symbols))
	for i, s := range symbols {
		symIndex[s] = uint32(i)
	}

	// Convert named series into index.SeriesEntry with label pairs as indices.
	out := make([]index.SeriesEntry, 0, len(named))
	for _, s := range named {
		pairs := make([][2]uint32, 0, len(s.Labels))
		for k, v := range s.Labels {
			ni := symIndex[k]
			vi := symIndex[v]
			pairs = append(pairs, [2]uint32{ni, vi})
		}
		// Sort label pairs by symbol name then value for deterministic ordering
		sort.Slice(pairs, func(i, j int) bool {
			if symbols[pairs[i][0]] == symbols[pairs[j][0]] {
				return symbols[pairs[i][1]] < symbols[pairs[j][1]]
			}
			return symbols[pairs[i][0]] < symbols[pairs[j][0]]
		})

		out = append(out, index.SeriesEntry{LabelPairs: pairs, Chunks: s.Chunks})
	}

	path := filepath.Join(blockDir, "index.idx")
	return index.CreateIndexWithSections(path, symbols, out)
}
