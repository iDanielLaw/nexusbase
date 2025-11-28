package engine2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeletesByTimeRange_WithHooks(t *testing.T) {
	ctx := context.Background()

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		// Setup
		s := newStorageEngineShim(t.TempDir())
		require.NoError(t, s.Start())
		defer s.Close()

		metric := "hook.range_delete.cancel"
		tags := map[string]string{"test": "pre_range_delete_hook"}
		ts1, ts2, ts3 := int64(100), int64(200), int64(300)

		// Put data so the series exists
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 1.0})))
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0})))
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0})))

		// Register a hook that will cancel the deletion
		expectedErr := errors.New("range deletion cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		s.GetHookManager().Register(hooks.EventPreDeleteRange, listener)

		// Action: Attempt to delete a range
		err := s.DeletesByTimeRange(ctx, metric, tags, ts2, ts2)

		// Assertions
		require.Error(t, err, "DeletesByTimeRange should have returned an error")
		require.ErrorIs(t, err, expectedErr, "Error should be the one from the hook")

		// Verify data was NOT deleted
		_, err = s.Get(ctx, metric, tags, ts2)
		require.NoError(t, err, "Get should find data for a cancelled range deletion")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		// Setup
		s := newStorageEngineShim(t.TempDir())
		require.NoError(t, s.Start())
		defer s.Close()

		metric := "hook.range_delete.post"
		tags := map[string]string{"test": "post_range_delete_hook"}
		ts1, ts2, ts3 := int64(100), int64(200), int64(300)

		// Put data so the series exists
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts1, map[string]interface{}{"value": 1.0})))
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts2, map[string]interface{}{"value": 2.0})))
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts3, map[string]interface{}{"value": 3.0})))

		// Register a listener to capture the post-delete event
		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		s.GetHookManager().Register(hooks.EventPostDeleteRange, listener)

		// Action: Delete a range
		deleteStartTime, deleteEndTime := ts2-50, ts2+50
		err := s.DeletesByTimeRange(ctx, metric, tags, deleteStartTime, deleteEndTime)
		require.NoError(t, err, "DeletesByTimeRange returned an unexpected error")

		// Assertions
		// 1. Data should be deleted immediately from a query perspective
		_, err = s.Get(ctx, metric, tags, ts2)
		require.ErrorIs(t, err, sstable.ErrNotFound, "Get should fail for deleted point in range")

		// 2. Wait for the async hook to be called
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostDeleteRangePayload)
			require.True(t, ok, "Post-hook received payload of wrong type: %T", event.Payload())

			assert.Equal(t, metric, payload.Metric, "Post-hook payload metric mismatch")
			assert.Equal(t, tags, payload.Tags, "Post-hook payload tags mismatch")
			assert.Equal(t, string(core.EncodeSeriesKeyWithString(metric, tags)), payload.SeriesKey, "Post-hook payload SeriesKey mismatch")
			assert.Equal(t, deleteStartTime, payload.StartTime, "Post-hook payload StartTime mismatch")
			assert.Equal(t, deleteEndTime, payload.EndTime, "Post-hook payload EndTime mismatch")

		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-range-delete-hook to be called")
		}
	})
}

func TestDeleteSeries_WithHooks(t *testing.T) {
	ctx := context.Background()

	t.Run("PreHook_CancelOperation", func(t *testing.T) {
		// Setup
		s := newStorageEngineShim(t.TempDir())
		require.NoError(t, s.Start())
		defer s.Close()

		metric := "hook.delete.cancel"
		tags := map[string]string{"test": "pre_delete_hook"}
		ts := time.Now().UnixNano()

		// Put data so the series exists
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0})))

		// Register a hook that will cancel the deletion
		expectedErr := errors.New("deletion cancelled by pre-hook")
		listener := &mockListener{returnErr: expectedErr}
		s.GetHookManager().Register(hooks.EventPreDeleteSeries, listener)

		// Action: Attempt to delete the series
		err := s.DeleteSeries(ctx, metric, tags)

		// Assertions
		require.Error(t, err, "DeleteSeries should have returned an error")
		require.ErrorIs(t, err, expectedErr, "Error should be the one from the hook")

		// Verify data was NOT deleted
		_, err = s.Get(ctx, metric, tags, ts)
		require.NoError(t, err, "Get should find data for a cancelled deletion")
	})

	t.Run("PostHook_AsyncNotification", func(t *testing.T) {
		// Setup
		s := newStorageEngineShim(t.TempDir())
		require.NoError(t, s.Start())
		defer s.Close()

		metric := "hook.delete.post"
		tags := map[string]string{"test": "post_delete_hook"}
		ts := time.Now().UnixNano()

		// Put data so the series exists
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tags, ts, map[string]interface{}{"value": 1.0})))

		// Register a listener to capture the post-delete event
		signalChan := make(chan hooks.HookEvent, 1)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		s.GetHookManager().Register(hooks.EventPostDeleteSeries, listener)

		// Action: Delete the series
		err := s.DeleteSeries(ctx, metric, tags)
		require.NoError(t, err, "DeleteSeries returned an unexpected error")

		// Assertions
		// 1. Data should be deleted immediately from a query perspective
		_, err = s.Get(ctx, metric, tags, ts)
		require.ErrorIs(t, err, sstable.ErrNotFound, "Get should fail for deleted series")

		// 2. Wait for the async hook to be called
		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.PostDeleteSeriesPayload)
			require.True(t, ok, "Post-hook received payload of wrong type: %T", event.Payload())
			assert.Equal(t, metric, payload.Metric, "Post-hook payload metric mismatch")
			assert.Equal(t, tags, payload.Tags, "Post-hook payload tags mismatch")
			assert.Equal(t, string(core.EncodeSeriesKeyWithString(metric, tags)), payload.SeriesKey, "Post-hook payload SeriesKey mismatch")

		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for post-delete-hook to be called")
		}
	})

	t.Run("PreHook_ModifyPayload", func(t *testing.T) {
		// Setup: Create two distinct series. The hook will redirect the deletion from one to the other.
		s := newStorageEngineShim(t.TempDir())
		require.NoError(t, s.Start())
		defer s.Close()

		metric := "hook.delete.modify"
		tagsOriginal := map[string]string{"host": "A"}
		tagsTarget := map[string]string{"host": "B"}
		tsOriginal := time.Now().UnixNano()
		tsTarget := tsOriginal + 1

		// Put data for both series
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tagsOriginal, tsOriginal, map[string]interface{}{"value": 1.0})))
		require.NoError(t, s.Put(ctx, HelperDataPoint(t, metric, tagsTarget, tsTarget, map[string]interface{}{"value": 2.0})))

		// Register a hook that modifies the tags to be deleted
		listener := &mockListener{onEvent: func(e hooks.HookEvent) {
			if p, ok := e.Payload().(hooks.PreDeleteSeriesPayload); ok {
				*p.Tags = tagsTarget
			}
		}}
		s.GetHookManager().Register(hooks.EventPreDeleteSeries, listener)

		// Action: Attempt to delete the original series
		err := s.DeleteSeries(ctx, metric, tagsOriginal)
		require.NoError(t, err, "DeleteSeries with modifying hook should not fail")

		// Assertions
		// 1. The original series should NOT be deleted.
		_, err = s.Get(ctx, metric, tagsOriginal, tsOriginal)
		assert.NoError(t, err, "Original series should not be deleted after hook modified the target")

		// 2. The target series (modified by the hook) SHOULD be deleted.
		_, err = s.Get(ctx, metric, tagsTarget, tsTarget)
		assert.ErrorIs(t, err, sstable.ErrNotFound, "Target series should be deleted by the hook")
	})
}
