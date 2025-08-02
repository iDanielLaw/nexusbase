package cache

import (
	"expvar"
	"reflect"
	"testing"
)

func TestNewBlockCache(t *testing.T) {
	// Test with valid capacity
	cache := NewLRUCache(10, nil, nil, nil) // No onEvicted func for this test
	if cache == nil {
		t.Fatal("NewBlockCache returned nil")
	}
	if cache.capacity != 10 {
		t.Errorf("Expected capacity 10, got %d", cache.capacity)
	}
	if cache.lruList.Len() != 0 {
		t.Errorf("Expected empty LRU list, got length %d", cache.lruList.Len())
	}
	if len(cache.cacheItems) != 0 {
		t.Errorf("Expected empty cache items map, got size %d", len(cache.cacheItems))
	}

	// Test with invalid capacity (<= 0)
	cacheInvalid := NewLRUCache(0, nil, nil, nil) // No onEvicted func for this test
	if cacheInvalid == nil {
		t.Fatal("NewBlockCache returned nil for invalid capacity")
	}
	if cacheInvalid.capacity != 0 { // Should be 0 for disabled cache
		t.Errorf("Expected capacity 0 for invalid input (disabled cache), got %d", cacheInvalid.capacity)
	}
}

func TestBlockCache_PutAndGet(t *testing.T) {
	cache := NewLRUCache(3, nil, nil, nil) // Capacity 3, no onEvicted

	key1, val1 := "key1", []byte("value1")
	key2, val2 := "key2", []byte("value2")
	key3, val3 := "key3", []byte("value3")
	key4, val4 := "key4", []byte("value4") // For eviction

	// Put items up to capacity
	cache.Put(key1, val1)
	cache.Put(key2, val2)
	cache.Put(key3, val3)

	if cache.Len() != 3 {
		t.Errorf("Expected cache size 3 after 3 puts, got %d", cache.Len())
	}

	// Get existing items
	v, found := cache.Get(key3)                         // v is interface{}
	if !found || !reflect.DeepEqual(v.([]byte), val3) { // Type assert v to []byte
		t.Errorf("Get(%s) failed. Found: %v, Value: %s", key3, found, string(v.([]byte))) // Type assert v to []byte
	}
	v, found = cache.Get(key1)                          // v is interface{}
	if !found || !reflect.DeepEqual(v.([]byte), val1) { // Type assert v to []byte
		t.Errorf("Get(%s) failed. Found: %v, Value: %s", key1, found, string(v.([]byte))) // Type assert v to []byte
	}

	// Get non-existent item
	_, found = cache.Get("nonexistent")
	if found {
		t.Error("Get(nonexistent) unexpectedly found item")
	}

	// Put item that exceeds capacity (should evict LRU)
	// key2 should be LRU now (key3, key1 accessed more recently)
	cache.Put(key4, val4)
	if cache.Len() != 3 {
		t.Errorf("Expected cache size 3 after put exceeding capacity, got %d", cache.Len())
	}

	// key2 should be evicted
	_, found = cache.Get(key2)
	if found {
		t.Errorf("Get(%s) unexpectedly found item after eviction", key2)
	}

	// key4 should be in cache
	v, found = cache.Get(key4)                          // v is interface{}
	if !found || !reflect.DeepEqual(v.([]byte), val4) { // Type assert v to []byte
		t.Errorf("Get(%s) failed after put exceeding capacity. Found: %v, Value: %s", key4, found, string(v.([]byte))) // Type assert v to []byte
	}
}

func TestBlockCache_Put_Update(t *testing.T) {
	cache := NewLRUCache(2, nil, nil, nil) // No onEvicted func for this test

	key := "updateKey"
	val1 := []byte("value1")
	val2 := []byte("value2")

	cache.Put(key, val1)
	if cache.Len() != 1 {
		t.Errorf("Expected cache size 1 after first put, got %d", cache.Len())
	}

	// Put with the same key (should update)
	cache.Put(key, val2)
	if cache.Len() != 1 { // Size should not change
		t.Errorf("Expected cache size 1 after update put, got %d", cache.Len())
	}

	v, found := cache.Get(key)
	if !found || !reflect.DeepEqual(v.([]byte), val2) { // Type assert v to []byte
		t.Errorf("Get(%s) failed after update. Found: %v, Value: %s", key, found, string(v.([]byte))) // Type assert v to []byte
	}
}

func TestBlockCache_Len(t *testing.T) {
	cache := NewLRUCache(5, nil, nil, nil) // No onEvicted
	if cache.Len() != 0 {
		t.Errorf("Expected initial size 0, got %d", cache.Len())
	}
	cache.Put("k1", []byte("v1"))
	if cache.Len() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Len())
	}
	cache.Put("k2", []byte("v2"))
	if cache.Len() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Len())
	}
	cache.Get("k1") // Access should not change length
	if cache.Len() != 2 {
		t.Errorf("Expected size 2 after Get, got %d", cache.Len())
	}
	cache.Put("k1", []byte("v1_updated")) // Update should not change length
	if cache.Len() != 2 {
		t.Errorf("Expected size 2 after update, got %d", cache.Len())
	}
	cache.Put("k3", []byte("v3"))
	cache.Put("k4", []byte("v4"))
	cache.Put("k5", []byte("v5"))
	if cache.Len() != 5 {
		t.Errorf("Expected size 5, got %d", cache.Len())
	}
	cache.Put("k6", []byte("v6")) // Exceed capacity
	if cache.Len() != 5 {
		t.Errorf("Expected size 5 after exceeding capacity, got %d", cache.Len())
	}
}

func TestBlockCache_Clear(t *testing.T) {
	cache := NewLRUCache(5, nil, nil, nil) // No onEvicted func for this test
	cache.Put("k1", []byte("v1"))
	cache.Put("k2", []byte("v2"))
	if cache.Len() != 2 {
		t.Fatalf("Expected size 2 before clear, got %d", cache.Len())
	}

	cache.Clear()
	if cache.Len() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", cache.Len())
	}
	_, found := cache.Get("k1")
	if found {
		t.Error("Get(k1) unexpectedly found item after clear")
	}
}

func TestBlockCache_GetHitRate(t *testing.T) {
	hits := new(expvar.Int)
	hits.Set(0) // Ensure initial value is 0
	misses := new(expvar.Int)
	misses.Set(0)                          // Ensure initial value is 0
	cache := NewLRUCache(2, nil, nil, nil) // No onEvicted func for this test
	cache.SetMetrics(hits, misses)
	// Initial state
	if rate := cache.GetHitRate(); rate != 0.0 {
		t.Errorf("Expected initial hit rate 0.0, got %f", rate)
	}

	// 1 miss
	// NewLRUCache does not take hits/misses directly.
	// We need to assign them after creation.

	cache.Get("k1")
	if rate := cache.GetHitRate(); rate != 0.0 { // 0/1
		t.Errorf("Expected hit rate 0.0 after 1 miss, got %f", rate)
	}
	cache.Put("k1", []byte("v1")) // Put doesn't affect hit/miss count
	cache.Get("k1")
	if rate := cache.GetHitRate(); rate != 0.5 { // 1/2
		t.Errorf("Expected hit rate 0.5 after 1 hit and 1 miss, got %f", rate)
	}

	// 1 miss, 2 hits
	cache.Get("k1")                                  // Hit
	if rate := cache.GetHitRate(); rate != 2.0/3.0 { // 2 hits / 3 total attempts
		t.Errorf("Expected hit rate 2/3, got %f", rate)
	}
	// 2 misses, 2 hits (eviction might happen)
	cache.Put("k2", []byte("v2")) // Put
	cache.Get("k2")               // Hit
	cache.Put("k3", []byte("v3")) // Put (evicts k1)
	cache.Get("k1")               // Miss

	// Total: 2 hits (k1, k2) + 2 hits (k1, k1) + 1 hit (k2) + 1 miss (k3) + 1 miss (k1) = 4 hits, 2 misses
	// Total requests: 6
	// Get(k2): miss (1h, 2m) Rate: 1/3=0.333
	// Get(k3): miss (1h, 3m) Rate: 1/4=0.25
	// Get(k1): miss (1h, 4m) Rate: 1/5=0.2

	// Let's redo the sequence carefully tracking hits/misses
	hits.Set(0)
	misses.Set(0)
	cache.Clear() // Start fresh

	cache.Get("k1") // Miss (0h, 1m)
	cache.Put("k1", []byte("v1"))
	cache.Get("k1") // Hit  (1h, 1m)
	cache.Put("k2", []byte("v2"))
	cache.Get("k2")               // Hit  (2h, 1m)
	cache.Put("k3", []byte("v3")) // Evicts k1
	cache.Get("k1")               // Miss (2h, 2m)
	cache.Get("k3")               // Hit  (3h, 2m)

	if hits.Value() != 3 || misses.Value() != 2 {
		t.Errorf("Final hits/misses mismatch: got hits=%d, misses=%d; want hits=3, misses=2", hits.Value(), misses.Value())
	}
	expectedRate := 3.0 / (3.0 + 2.0) // 3 / 5 = 0.6
	if rate := cache.GetHitRate(); rate != expectedRate {
		t.Errorf("Expected hit rate %f, got %f", expectedRate, rate)
	}
}

func TestBlockCache_Disabled(t *testing.T) {
	cache := NewLRUCache(0, nil, nil, nil) // Disabled cache, no onEvicted func for this test
	cache.SetMetrics(nil, nil)             // Ensure metrics are nil for this test

	key, val := "k1", []byte("v1")

	cache.Put(key, val)
	if cache.Len() != 0 { // Should be 0 for disabled cache
		t.Errorf("Expected cache size 0 for disabled cache, got %d", cache.Len())
	}

	_, found := cache.Get(key)
	if found {
		t.Error("Get unexpectedly found item in disabled cache")
	}

	// Metrics should not be affected if nil is passed
	hits := new(expvar.Int)
	misses := new(expvar.Int)
	cacheWithMetrics := NewLRUCache(0, nil, nil, nil) // Disabled cache, no onEvicted func for this test
	cacheWithMetrics.SetMetrics(hits, misses)
	cacheWithMetrics.Put("k2", []byte("v2"))
	cacheWithMetrics.Get("k2")

	if hits.Value() != 0 || misses.Value() != 0 {
		t.Errorf("Metrics unexpectedly updated for disabled cache: hits=%d, misses=%d", hits.Value(), misses.Value())
	}
}
