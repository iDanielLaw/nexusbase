package sys

import "sync/atomic"

// PreallocCacheStats returns the current preallocation cache hit and miss
// counters. These counters are updated inside the package and this accessor
// provides a safe way to read them for diagnostics or metrics.
func PreallocCacheStats() (hits uint64, misses uint64) {
	hits = atomic.LoadUint64(&preallocCacheHits)
	misses = atomic.LoadUint64(&preallocCacheMisses)
	return
}
