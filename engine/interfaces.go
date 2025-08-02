package engine

import (
	"github.com/INLOpen/nexusbase/api/tsdb"
)

// PubSubInterface defines the public API for the real-time publisher-subscriber system.
type PubSubInterface interface {
	Subscribe(filter SubscriptionFilter) *Subscription
	Unsubscribe(id uint64)
	Publish(update *tsdb.DataPointUpdate)
}
