package engine2

import (
	"sync"

	"github.com/INLOpen/nexusbase/api/tsdb"
	"github.com/INLOpen/nexusbase/engine"
)

// PubSub is a simple in-memory publisher-subscriber implementation that
// mirrors the legacy `engine.PubSub` behavior but lives in `engine2` so the
// adapter can return it directly. It deliberately uses the legacy types
// (`engine.Subscription` and `engine.SubscriptionFilter`) so it satisfies
// `engine.PubSubInterface` exactly.
type PubSub struct {
	mu          sync.RWMutex
	subscribers map[uint64]*engine.Subscription
	nextID      uint64
}

// NewPubSub constructs a new PubSub instance.
func NewPubSub() *PubSub {
	return &PubSub{subscribers: make(map[uint64]*engine.Subscription)}
}

// Subscribe creates a new subscription with the provided filter.
func (ps *PubSub) Subscribe(filter engine.SubscriptionFilter) *engine.Subscription {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.nextID++
	sub := &engine.Subscription{
		ID:      ps.nextID,
		Updates: make(chan *tsdb.DataPointUpdate, 100),
		Filter:  filter,
	}
	// closure to allow subscribers to unsubscribe themselves
	sub.Close = func() {
		ps.Unsubscribe(sub.ID)
	}
	ps.subscribers[sub.ID] = sub
	return sub
}

// Unsubscribe removes and closes a subscription.
func (ps *PubSub) Unsubscribe(id uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if sub, ok := ps.subscribers[id]; ok {
		close(sub.Updates)
		delete(ps.subscribers, id)
	}
}

// Publish sends an update to all matching subscribers. Non-blocking send
// to avoid a slow subscriber from blocking the publisher.
func (ps *PubSub) Publish(update *tsdb.DataPointUpdate) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, sub := range ps.subscribers {
		if sub.Filter.Matches(update) {
			select {
			case sub.Updates <- update:
			default:
				// drop if subscriber channel is full
			}
		}
	}
}

// The import of "strings" is intentionally present in case matching logic
// needs to be extended locally in the future; the current implementation
// delegates matching to `engine.SubscriptionFilter.Matches`.
