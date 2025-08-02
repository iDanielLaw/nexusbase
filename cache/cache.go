package cache

import (
	"container/list" // Import container/list for LRU
	"expvar"         // Import expvar
	"sync"
)

// cacheEntry holds the key and value for a cache item.
type cacheEntry struct {
	key   string
	value interface{} // Changed to interface{} for generic LRU
}

// LRUCache implements a generic fixed-size LRU cache.
type LRUCache struct {
	mu         sync.Mutex
	capacity   int
	lruList    *list.List
	cacheItems map[string]*list.Element
	onEvicted  func(key string, value interface{}) // Optional callback on eviction
	onHit      func(key string)                    // Optional: called on a cache hit.
	onMiss     func(key string)                    // Optional: called on a cache miss.

	// Metrics (FR5.4)
	hits   *expvar.Int
	misses *expvar.Int
}

// NewLRUCache creates a new generic LRUCache.
func NewLRUCache(capacity int, onEvicted func(key string, value interface{}), onHit, onMiss func(key string)) *LRUCache {
	return &LRUCache{
		capacity:   capacity,
		lruList:    list.New(),
		cacheItems: make(map[string]*list.Element),
		onEvicted:  onEvicted,
		onHit:      onHit,
		onMiss:     onMiss,
	}
}

func (c *LRUCache) SetMetrics(hits, misses *expvar.Int) {
	c.hits = hits
	c.misses = misses
}

// Get retrieves a value from the cache.
func (c *LRUCache) Get(key string) (value interface{}, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity <= 0 {
		if c.misses != nil {
			// Even if disabled, a Get attempt is a conceptual miss against the "caching mechanism"
			// However, for a truly disabled cache, we might not want to increment misses.
			// Let's decide to NOT increment misses if cache is disabled.
		}
		return nil, false
	}

	if elem, ok := c.cacheItems[key]; ok {
		if c.hits != nil {
			c.hits.Add(1)
		}
		if c.onHit != nil {
			c.onHit(key)
		}
		c.lruList.MoveToFront(elem)
		return elem.Value.(*cacheEntry).value, true
	}

	if c.misses != nil {
		c.misses.Add(1)
	}
	if c.onMiss != nil {
		c.onMiss(key)
	}
	return nil, false
}

// Put adds a value to the cache.
func (c *LRUCache) Put(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.capacity <= 0 {
		return
	}

	if elem, ok := c.cacheItems[key]; ok {
		// Update existing entry
		c.lruList.MoveToFront(elem)
		elem.Value.(*cacheEntry).value = value
		return
	}

	// Add new entry
	if c.lruList.Len() >= c.capacity {
		// Evict least recently used item
		c.evict()
	}

	newEntry := &cacheEntry{key: key, value: value}
	element := c.lruList.PushFront(newEntry)
	c.cacheItems[key] = element
}

// Len returns the current number of items in the cache.
func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lruList.Len()
}

// evict removes the least recently used item from the cache.
// Must be called with c.mu locked.
func (c *LRUCache) evict() {
	if elem := c.lruList.Back(); elem != nil {
		removedEntry := c.lruList.Remove(elem).(*cacheEntry)
		delete(c.cacheItems, removedEntry.key)
		if c.onEvicted != nil {
			c.onEvicted(removedEntry.key, removedEntry.value)
		}
	}
}

// Clear removes all entries from the cache.
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If an onEvicted callback is set, call it for all items being cleared.
	// This is crucial for returning pooled resources (like buffers) to their pools.
	if c.onEvicted != nil {
		for _, elem := range c.cacheItems {
			c.onEvicted(elem.Value.(*cacheEntry).key, elem.Value.(*cacheEntry).value)
		}
	}
	c.lruList = list.New()
	c.cacheItems = make(map[string]*list.Element)
	// Also reset metrics when clearing the cache
	if c.hits != nil {
		c.hits.Set(0)
	}
	if c.misses != nil {
		c.misses.Set(0)
	}
}

// GetHitRate calculates the cache hit rate.
// This is useful for expvar.Func.
func (c *LRUCache) GetHitRate() float64 {
	var hits, misses float64
	if c.hits != nil {
		// expvar.Int.Value() returns int64 directly. No type assertion needed.
		hits = float64(c.hits.Value())
	}
	if c.misses != nil {
		misses = float64(c.misses.Value())
	}

	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return hits / total
}
