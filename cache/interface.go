package cache

import "expvar"

// Interface defines the public API for a generic cache.
type Interface interface {
	Put(key string, value interface{})
	Get(key string) (value interface{}, ok bool)
	Clear()
	GetHitRate() float64
	SetMetrics(hits, misses *expvar.Int)
	Len() int
}
