package core

import (
	"context"
	"sync"
)

// ReplicationTracker manages waiting for follower acknowledgements in synchronous replication.
type ReplicationTracker struct {
	mu                  sync.Mutex
	cond                *sync.Cond
	latestAppliedSeqNum uint64
}

// NewReplicationTracker creates a new tracker.
func NewReplicationTracker() *ReplicationTracker {
	t := &ReplicationTracker{}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// ReportAppliedSequence is called by the leader when a follower reports its progress.
// It updates the latest known applied sequence number and wakes up any waiting goroutines.
func (t *ReplicationTracker) ReportAppliedSequence(seqNum uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if seqNum > t.latestAppliedSeqNum {
		t.latestAppliedSeqNum = seqNum
		// Broadcast to all waiting goroutines that the sequence number has changed.
		t.cond.Broadcast()
	}
}

// WaitForSequence blocks until the given sequence number has been acknowledged by a follower,
// or until the context is cancelled (e.g., due to a timeout).
func (t *ReplicationTracker) WaitForSequence(ctx context.Context, waitSeqNum uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// This pattern allows waiting on a condition variable with context cancellation.
	// A separate goroutine waits for the broadcast signal.
	waitDone := make(chan struct{})
	go func() {
		for t.latestAppliedSeqNum < waitSeqNum {
			// Check for context cancellation before waiting.
			if ctx.Err() != nil {
				break
			}
			t.cond.Wait()
		}
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// The sequence number has been applied.
		return nil
	case <-ctx.Done():
		// The context was cancelled (e.g., timeout).
		// We need to wake up our waiting goroutine so it can exit.
		t.cond.Broadcast()
		// Wait for the goroutine to finish its check and exit.
		<-waitDone
		return ctx.Err()
	}
}

// GetLatestApplied returns the latest sequence number known to be applied by a follower.
func (t *ReplicationTracker) GetLatestApplied() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.latestAppliedSeqNum
}