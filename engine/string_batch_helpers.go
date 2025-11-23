package engine

import (
	"github.com/INLOpen/nexusbase/indexer"
)

// ensureIDs attempts to populate `idMap` with IDs for any strings in `strs`
// that are not already present. It prefers the concrete StringStore's
// `AddStringsBatch` for efficiency and falls back to per-string
// `GetOrCreateID` when batching isn't available or fails.
func (e *storageEngine) ensureIDs(idMap map[string]uint64, strs []string) {
	if e.stringStore == nil || len(strs) == 0 {
		return
	}
	// build list of missing strings
	missing := make([]string, 0)
	seen := make(map[string]struct{}, len(strs))
	for _, s := range strs {
		if _, ok := idMap[s]; ok {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		missing = append(missing, s)
	}
	if len(missing) == 0 {
		return
	}
	// try batch API on concrete implementation
	if ss, ok := e.stringStore.(*indexer.StringStore); ok {
		if ids, err := ss.AddStringsBatch(missing); err == nil {
			for i, s := range missing {
				idMap[s] = ids[i]
			}
			return
		}
	}
	// fallback to per-string creation
	for _, s := range missing {
		if id, err := e.stringStore.GetOrCreateID(s); err == nil {
			idMap[s] = id
		}
	}
}

// getOrCreateIDFromMap returns an ID for `s` using `idMap` if present,
// otherwise it falls back to the persistent StringStore. It returns (0,nil)
// when the engine has no StringStore configured.
func (e *storageEngine) getOrCreateIDFromMap(idMap map[string]uint64, s string) (uint64, error) {
	if idMap != nil {
		if v, ok := idMap[s]; ok {
			return v, nil
		}
	}
	if e.stringStore == nil {
		return 0, nil
	}
	return e.stringStore.GetOrCreateID(s)
}
