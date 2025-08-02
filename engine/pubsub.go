package engine

import (
	"strings"
	"sync"

	"github.com/INLOpen/nexusbase/api/tsdb"
)

// Subscription represents a client's subscription to data updates.
type Subscription struct {
	ID      uint64
	Updates chan *tsdb.DataPointUpdate // Channel to send updates to the subscriber.
	Filter  SubscriptionFilter
	Close   func() // Function to close the subscription.
}

// SubscriptionFilter defines the criteria for a subscription.
type SubscriptionFilter struct {
	Metric string
	Tags   map[string]string
}

// Matches checks if a given data point update matches the filter.
func (f *SubscriptionFilter) Matches(update *tsdb.DataPointUpdate) bool {
	// Check metric filter with wildcard support
	if f.Metric != "" {
		if strings.HasSuffix(f.Metric, "*") {
			prefix := strings.TrimSuffix(f.Metric, "*")
			if !strings.HasPrefix(update.Metric, prefix) {
				return false
			}
		} else if f.Metric != update.Metric {
			return false
		}
	}

	// Check tag filters with wildcard support on values
	for k, v := range f.Tags {
		tagVal, ok := update.Tags[k]
		if !ok {
			return false
		}
		if strings.HasSuffix(v, "*") {
			prefix := strings.TrimSuffix(v, "*")
			if !strings.HasPrefix(tagVal, prefix) {
				return false
			}
		} else if tagVal != v {
			return false
		}
	}
	return true
}

// PubSub handles real-time data subscriptions.
type PubSub struct {
	PubSubInterface
	mu          sync.RWMutex
	subscribers map[uint64]*Subscription
	nextID      uint64
}

// NewPubSub creates a new PubSub system.
func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[uint64]*Subscription),
	}
}

// Subscribe creates a new subscription and returns it.
func (ps *PubSub) Subscribe(filter SubscriptionFilter) *Subscription {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.nextID++
	sub := &Subscription{
		ID:      ps.nextID,
		Updates: make(chan *tsdb.DataPointUpdate, 100), // Buffered channel
		Filter:  filter,
	}
	sub.Close = func() {
		ps.Unsubscribe(sub.ID)
	}

	ps.subscribers[sub.ID] = sub
	return sub
}

// Unsubscribe removes a subscription.
func (ps *PubSub) Unsubscribe(id uint64) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if sub, ok := ps.subscribers[id]; ok {
		close(sub.Updates)
		delete(ps.subscribers, id)
	}
}

// Publish sends a data point update to all matching subscribers.
func (ps *PubSub) Publish(update *tsdb.DataPointUpdate) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, sub := range ps.subscribers {
		if sub.Filter.Matches(update) {
			// Non-blocking send to avoid a slow subscriber from blocking the write path.
			select {
			case sub.Updates <- update:
			default:
				// Subscriber's channel is full. We can log this or drop the update.
			}
		}
	}
}
