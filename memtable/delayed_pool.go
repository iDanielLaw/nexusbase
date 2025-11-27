package memtable

import (
	"log/slog"
	"sync"
	"time"
)

// delayedEntryPool implements a quarantine-based delayed reuse pool.
// Objects returned to the pool are placed into a quarantine buffer for
// a short delay, after which they become available for reuse. This avoids
// immediate reuse of objects that may still be referenced by other
// goroutines while keeping eventual reuse to reduce GC pressure.

type quarantinedItem struct {
	e  *MemtableEntry
	ts int64 // unix nano timestamp when quarantined
}

type delayedEntryPool struct {
	inner      *entryPool
	mu         sync.Mutex
	quarantine []quarantinedItem
	delay      time.Duration
}

// newDelayedEntryPool creates a delayed pool with a background drainer.
func newDelayedEntryPool(capacity int) *delayedEntryPool {
	p := &delayedEntryPool{
		inner:      newEntryPool(capacity),
		quarantine: make([]quarantinedItem, 0, 256),
		delay:      250 * time.Millisecond,
	}

	// start background drainer
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now().UnixNano()
			p.mu.Lock()
			i := 0
			for ; i < len(p.quarantine); i++ {
				if now-p.quarantine[i].ts < p.delay.Nanoseconds() {
					break
				}
				// move to inner pool
				item := p.quarantine[i].e
				// reset fields before putting into inner pool
				if item != nil {
					item.Key = nil
					item.Value = nil
					item.EntryType = 0
					item.PointID = 0
				}
				p.inner.Put(item)
			}
			if i > 0 {
				// drop drained items from slice
				p.quarantine = append(p.quarantine[:0], p.quarantine[i:]...)
			}
			p.mu.Unlock()
		}
	}()

	slog.Default().Debug("DelayedEntryPool: created", "delay_ms", p.delay.Milliseconds())
	return p
}

// Get obtains an entry from the inner pool or allocates a new one.
func (p *delayedEntryPool) Get() *MemtableEntry {
	return p.inner.Get()
}

// Put quarantines the entry instead of returning it immediately to the inner pool.
func (p *delayedEntryPool) Put(e *MemtableEntry) {
	if e == nil {
		return
	}
	qi := quarantinedItem{e: e, ts: time.Now().UnixNano()}
	p.mu.Lock()
	p.quarantine = append(p.quarantine, qi)
	p.mu.Unlock()
}

// GetMetrics forwards metrics from the inner pool (approximate).
func (p *delayedEntryPool) GetMetrics() (hits, misses uint64, size int) {
	return p.inner.GetMetrics()
}
