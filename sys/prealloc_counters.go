package sys

import "sync"

// preallocCache caches preallocation capability per-device (dev ID).
// Key: uint64 device ID -> bool (true = allowed)
var preallocCache sync.Map

// preallocCacheHits / preallocCacheMisses are counters updated by platform
// implementations to record cache behavior. They are declared here so the
// accessor `PreallocCacheStats` can build on all platforms.
var preallocCacheHits uint64
var preallocCacheMisses uint64
