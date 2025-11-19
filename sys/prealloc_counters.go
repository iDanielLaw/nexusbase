package sys

import (
	"sync"
	"sync/atomic"
)

// Package-level preallocation cache and counters
// ---------------------------------------------
// The code in this package tries to determine whether a given device /
// filesystem supports efficient preallocation (e.g., fallocate on Linux or
// FILE_ALLOCATION_INFO on Windows). To avoid repeated expensive checks
// (fstatfs, feature probes), we cache a boolean per-device id.
//
// The helpers below encapsulate access to the cache and counters. Use the
// helpers instead of referencing `preallocCache` or counter variables
// directly so we can change the underlying implementation without touching
// platform code.
//
// Example usage (platform implementation):
//
//    if allow, found := preallocCacheLoad(dev); found {
//        preallocCacheHit()
//        if !allow {
//            return ErrPreallocNotSupported
//        }
//        // attempt prealloc
//    }
//
//    // on miss:
//    preallocCacheMiss()
//    // compute allowed and then store
//    preallocCacheStore(dev, allowed)

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
