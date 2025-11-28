package engine2

import (
	"reflect"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/api/tsdb"
)

func TestPubSub_SubscribeUnsubscribe(t *testing.T) {
	ps := NewPubSub()

	// Subscribe
	sub := ps.Subscribe(SubscriptionFilter{Metric: "test.metric"})
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	if sub.ID == 0 {
		t.Error("Subscription ID should not be 0")
	}

	ps.mu.RLock()
	if len(ps.subscribers) != 1 {
		t.Errorf("Expected 1 subscriber, got %d", len(ps.subscribers))
	}
	if _, ok := ps.subscribers[sub.ID]; !ok {
		t.Error("Subscriber not found in map by ID")
	}
	ps.mu.RUnlock()

	// Unsubscribe using the Close function
	sub.Close()

	// Allow some time for the unsubscribe to process
	time.Sleep(10 * time.Millisecond)

	ps.mu.RLock()
	if len(ps.subscribers) != 0 {
		t.Errorf("Expected 0 subscribers after unsubscribe, got %d", len(ps.subscribers))
	}
	ps.mu.RUnlock()

	// Check if channel is closed
	select {
	case _, ok := <-sub.Updates:
		if ok {
			t.Error("Subscription channel should be closed after unsubscribe")
		}
	default:
		// OK - non-blocking
	}
}

func TestPubSub_PublishAndFilter(t *testing.T) {
	ps := NewPubSub()

	// Subscriber 1: specific metric and tags
	sub1 := ps.Subscribe(SubscriptionFilter{
		Metric: "cpu.usage",
		Tags:   map[string]string{"host": "serverA"},
	})
	defer sub1.Close()

	// Subscriber 2: metric only
	sub2 := ps.Subscribe(SubscriptionFilter{
		Metric: "cpu.usage",
	})
	defer sub2.Close()

	// Subscriber 3: tags only
	sub3 := ps.Subscribe(SubscriptionFilter{
		Tags: map[string]string{"region": "us-east"},
	})
	defer sub3.Close()

	// Subscriber 4: no filter (receives all)
	sub4 := ps.Subscribe(SubscriptionFilter{})
	defer sub4.Close()

	// Subscriber 5: wrong metric
	sub5 := ps.Subscribe(SubscriptionFilter{
		Metric: "memory.free",
	})
	defer sub5.Close()

	// Update that should match sub1, sub2, and sub4
	update1 := &tsdb.DataPointUpdate{
		Metric: "cpu.usage",
		Tags:   map[string]string{"host": "serverA", "region": "us-west"},
		Value:  50.0,
	}
	ps.Publish(update1)

	// Update that should match sub3 and sub4
	update2 := &tsdb.DataPointUpdate{
		Metric: "network.io",
		Tags:   map[string]string{"host": "serverB", "region": "us-east"},
		Value:  100.0,
	}
	ps.Publish(update2)

	// Check sub1
	select {
	case received := <-sub1.Updates:
		if received != update1 {
			t.Errorf("sub1 received wrong update: got %+v, want %+v", received, update1)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("sub1 did not receive update1 in time")
	}
	if len(sub1.Updates) > 0 {
		t.Error("sub1 received more updates than expected")
	}

	// Check sub2
	select {
	case received := <-sub2.Updates:
		if received != update1 {
			t.Errorf("sub2 received wrong update: got %+v, want %+v", received, update1)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("sub2 did not receive update1 in time")
	}
	if len(sub2.Updates) > 0 {
		t.Error("sub2 received more updates than expected")
	}

	// Check sub3
	select {
	case received := <-sub3.Updates:
		if received != update2 {
			t.Errorf("sub3 received wrong update: got %+v, want %+v", received, update2)
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("sub3 did not receive update2 in time")
	}
	if len(sub3.Updates) > 0 {
		t.Error("sub3 received more updates than expected")
	}

	// Check sub4 (should receive both)
	received4_1 := <-sub4.Updates
	received4_2 := <-sub4.Updates
	if !((received4_1 == update1 && received4_2 == update2) || (received4_1 == update2 && received4_2 == update1)) {
		t.Errorf("sub4 did not receive both updates correctly. Got: %+v, %+v", received4_1, received4_2)
	}

	// Check sub5 (should receive none)
	select {
	case received := <-sub5.Updates:
		t.Errorf("sub5 received an unexpected update: %+v", received)
	case <-time.After(50 * time.Millisecond):
		// Expected outcome
	}
}

func TestPubSub_PublishAndFilter_Wildcard(t *testing.T) {
	ps := NewPubSub()

	// Subscriber 1: wildcard metric 'cpu.*'
	sub1 := ps.Subscribe(SubscriptionFilter{Metric: "cpu.*"})
	defer sub1.Close()

	// Subscriber 2: wildcard tag value 'host=server-*'
	sub2 := ps.Subscribe(SubscriptionFilter{Tags: map[string]string{"host": "server-*"}})
	defer sub2.Close()

	// Subscriber 3: specific metric and wildcard tag 'mem.usage', 'region=us-*'
	sub3 := ps.Subscribe(SubscriptionFilter{
		Metric: "mem.usage",
		Tags:   map[string]string{"region": "us-*"},
	})
	defer sub3.Close()

	// Subscriber 4: non-matching wildcard 'disk.*'
	sub4 := ps.Subscribe(SubscriptionFilter{Metric: "disk.*"})
	defer sub4.Close()

	// Updates to publish
	updateCPU1 := &tsdb.DataPointUpdate{Metric: "cpu.usage", Tags: map[string]string{"host": "server-a"}}
	updateCPU2 := &tsdb.DataPointUpdate{Metric: "cpu.temp", Tags: map[string]string{"host": "server-b"}}
	updateMem1 := &tsdb.DataPointUpdate{Metric: "mem.usage", Tags: map[string]string{"host": "server-a", "region": "us-east"}}
	updateMem2 := &tsdb.DataPointUpdate{Metric: "mem.free", Tags: map[string]string{"host": "server-c", "region": "us-west"}}
	updateNet1 := &tsdb.DataPointUpdate{Metric: "net.io", Tags: map[string]string{"host": "server-a"}}

	ps.Publish(updateCPU1)
	ps.Publish(updateCPU2)
	ps.Publish(updateMem1)
	ps.Publish(updateMem2)
	ps.Publish(updateNet1)

	// Helper to collect results from a channel
	collectResults := func(sub *Subscription, count int) []*tsdb.DataPointUpdate {
		var results []*tsdb.DataPointUpdate
		for i := 0; i < count; i++ {
			select {
			case u := <-sub.Updates:
				results = append(results, u)
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Timed out waiting for update %d for subscriber %d", i+1, sub.ID)
				return results
			}
		}
		// Check for extra messages
		select {
		case u := <-sub.Updates:
			t.Errorf("Subscriber %d received unexpected extra update: %+v", sub.ID, u)
		default:
			// OK
		}
		return results
	}

	// Verify sub1 (cpu.*)
	actualSub1 := collectResults(sub1, 2)
	expectedSub1 := []*tsdb.DataPointUpdate{updateCPU1, updateCPU2}
	if !reflect.DeepEqual(actualSub1, expectedSub1) {
		t.Errorf("sub1 results mismatch:\nGot:    %+v\nWanted: %+v", actualSub1, expectedSub1)
	}

	// Verify sub2 (host=server-*)
	actualSub2 := collectResults(sub2, 5)
	expectedSub2 := []*tsdb.DataPointUpdate{updateCPU1, updateCPU2, updateMem1, updateMem2, updateNet1}
	if !reflect.DeepEqual(actualSub2, expectedSub2) {
		t.Errorf("sub2 results mismatch:\nGot:    %+v\nWanted: %+v", actualSub2, expectedSub2)
	}

	// Verify sub3 (mem.usage, region=us-*)
	actualSub3 := collectResults(sub3, 1)
	expectedSub3 := []*tsdb.DataPointUpdate{updateMem1}
	if !reflect.DeepEqual(actualSub3, expectedSub3) {
		t.Errorf("sub3 results mismatch:\nGot:    %+v\nWanted: %+v", actualSub3, expectedSub3)
	}

	// Verify sub4 (disk.*)
	collectResults(sub4, 0) // Expect 0 results
}
