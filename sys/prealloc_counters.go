package sys

import (
	"sync"
	"sync/atomic"
)

// preallocCache caches preallocation capability per-device (dev ID).
// Key: uint64 device ID -> bool (true = allowed)
// Access to the cache should go through the small API below so callers
// don't depend on the underlying storage type.
var preallocCache sync.Map

// preallocCacheHits / preallocCacheMisses are counters updated by platform
// implementations to record cache behavior. Use atomic.Uint64 for clearer
// semantics and safer concurrent updates.
var preallocCacheHits atomic.Uint64
var preallocCacheMisses atomic.Uint64

// preallocCacheLoad returns (allowed, found).
func preallocCacheLoad(dev uint64) (allowed bool, found bool) {
	if v, ok := preallocCache.Load(dev); ok {
		if b, ok2 := v.(bool); ok2 {
			return b, true
		}
	}
	return false, false
}

// preallocCacheStore stores the allow/deny value for the given device id.
func preallocCacheStore(dev uint64, allowed bool) {
	preallocCache.Store(dev, allowed)
}

// preallocCacheHit increments the hit counter.
func preallocCacheHit() {
	preallocCacheHits.Add(1)
}

// preallocCacheMiss increments the miss counter.
func preallocCacheMiss() {
	preallocCacheMisses.Add(1)
}
