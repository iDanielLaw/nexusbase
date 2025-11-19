package sys

// PreallocCacheStats returns the current preallocation cache hit and miss
// counters. These counters are updated inside the package and this accessor
// provides a safe way to read them for diagnostics or metrics.
func PreallocCacheStats() (hits uint64, misses uint64) {
	hits = preallocCacheHits.Load()
	misses = preallocCacheMisses.Load()
	return
}
