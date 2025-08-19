package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationTracker_ReportAndGet(t *testing.T) {
	tracker := NewReplicationTracker()

	assert.Equal(t, uint64(0), tracker.GetLatestApplied(), "Initial applied sequence should be 0")

	tracker.ReportAppliedSequence(100)
	assert.Equal(t, uint64(100), tracker.GetLatestApplied(), "Applied sequence should be updated to 100")

	// Reporting an older sequence number should not change the value
	tracker.ReportAppliedSequence(50)
	assert.Equal(t, uint64(100), tracker.GetLatestApplied(), "Reporting an older sequence should not decrease the value")

	tracker.ReportAppliedSequence(100)
	assert.Equal(t, uint64(100), tracker.GetLatestApplied(), "Reporting the same sequence should not change the value")

	tracker.ReportAppliedSequence(101)
	assert.Equal(t, uint64(101), tracker.GetLatestApplied(), "Applied sequence should be updated to 101")
}

func TestReplicationTracker_WaitForSequence_Success(t *testing.T) {
	tracker := NewReplicationTracker()
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		// This goroutine will block until sequence 5 is reported
		err := tracker.WaitForSequence(context.Background(), 5)
		errChan <- err
	}()

	// Give the goroutine a moment to start and block
	time.Sleep(10 * time.Millisecond)

	// Report progress, but not enough to unblock
	tracker.ReportAppliedSequence(3)
	tracker.ReportAppliedSequence(4)

	// Unblock the waiting goroutine
	tracker.ReportAppliedSequence(5)

	wg.Wait() // Wait for the goroutine to finish

	// Check the result
	err := <-errChan
	require.NoError(t, err, "WaitForSequence should return no error on success")
}

func TestReplicationTracker_WaitForSequence_AlreadyApplied(t *testing.T) {
	tracker := NewReplicationTracker()
	tracker.ReportAppliedSequence(10)

	// This should return immediately without blocking
	err := tracker.WaitForSequence(context.Background(), 5)
	require.NoError(t, err, "WaitForSequence should return immediately if sequence is already applied")

	err = tracker.WaitForSequence(context.Background(), 10)
	require.NoError(t, err, "WaitForSequence should return immediately if sequence is exactly the same")
}

func TestReplicationTracker_WaitForSequence_Timeout(t *testing.T) {
	tracker := NewReplicationTracker()
	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// Create a context with a short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// This goroutine will block, but should be cancelled by the context timeout
		err := tracker.WaitForSequence(ctx, 10)
		errChan <- err
	}()

	wg.Wait() // Wait for the goroutine to finish

	// Check the result
	err := <-errChan
	require.Error(t, err, "WaitForSequence should return an error on timeout")
	assert.ErrorIs(t, err, context.DeadlineExceeded, "The error should be context.DeadlineExceeded")
}