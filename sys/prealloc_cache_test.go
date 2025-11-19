package sys

import "testing"

// resetPreallocCacheForTest clears the package-level cache and resets counters.
func resetPreallocCacheForTest() {
	preallocCache.Range(func(k, v any) bool {
		preallocCache.Delete(k)
		return true
	})
	preallocCacheHits.Store(0)
	preallocCacheMisses.Store(0)
}

func TestPreallocCacheCountersAndStoreLoad(t *testing.T) {
	resetPreallocCacheForTest()

	hits, misses := PreallocCacheStats()
	if hits != 0 || misses != 0 {
		t.Fatalf("expected zeroed stats, got hits=%d misses=%d", hits, misses)
	}

	// Exercise miss increment
	preallocCacheMiss()
	hits, misses = PreallocCacheStats()
	if hits != 0 || misses != 1 {
		t.Fatalf("after one miss, expected hits=0 misses=1, got hits=%d misses=%d", hits, misses)
	}

	// Exercise hit increments
	preallocCacheHit()
	preallocCacheHit()
	hits, misses = PreallocCacheStats()
	if hits != 2 || misses != 1 {
		t.Fatalf("after two hits, expected hits=2 misses=1, got hits=%d misses=%d", hits, misses)
	}

	// Test load/store behavior
	const devID = uint64(0xABCD)
	if allow, found := preallocCacheLoad(devID); found {
		t.Fatalf("expected not found for dev %d, but found allow=%v", devID, allow)
	}

	preallocCacheStore(devID, false)
	if allow, found := preallocCacheLoad(devID); !found || allow {
		t.Fatalf("expected found=false stored as false for dev %d, got found=%v allow=%v", devID, found, allow)
	}

	preallocCacheStore(devID, true)
	if allow, found := preallocCacheLoad(devID); !found || !allow {
		t.Fatalf("expected found=true stored as true for dev %d, got found=%v allow=%v", devID, found, allow)
	}

	// Ensure counters are unchanged by store operations
	hits, misses = PreallocCacheStats()
	if hits != 2 || misses != 1 {
		t.Fatalf("unexpected stats after stores: hits=%d misses=%d", hits, misses)
	}

	// Another hit to verify increments continue to work
	preallocCacheHit()
	hits, _ = PreallocCacheStats()
	if hits != 3 {
		t.Fatalf("expected hits=3 after an additional hit, got %d", hits)
	}
}
