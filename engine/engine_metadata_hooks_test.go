package engine

import (
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataHooks(t *testing.T) {
	opts := GetBaseOptsForTest(t, "test")
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)

	t.Run("OnStringCreateHook", func(t *testing.T) {
		signalChan := make(chan hooks.HookEvent, 5)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		concreteEngine.hookManager.Register(hooks.EventOnStringCreate, listener)

		// Action 1: Put a point with new strings
		metric := "new.metric"
		tags := map[string]string{"new_tag_key": "new_tag_value"}
		require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, metric, tags, 1, map[string]interface{}{"value": 1.0})))

		// Verification 1: Expect 3 events (metric, tag key, tag value)
		expectedStrings := map[string]bool{
			metric:          true,
			"new_tag_key":   true,
			"new_tag_value": true,
		}
		for i := 0; i < 3; i++ {
			select {
			case event := <-signalChan:
				payload, ok := event.Payload().(hooks.StringCreatePayload)
				require.True(t, ok)
				assert.Greater(t, payload.ID, uint64(0))
				delete(expectedStrings, payload.Str)
			case <-time.After(200 * time.Millisecond):
				t.Fatalf("Timed out waiting for OnStringCreate event %d", i+1)
			}
		}
		assert.Empty(t, expectedStrings, "Not all expected strings triggered a create event")

		// Action 2: Put a point with the same strings
		require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, metric, tags, 2, map[string]interface{}{"value": 2.0})))

		// Verification 2: Expect no new events
		select {
		case event := <-signalChan:
			t.Fatalf("Received unexpected OnStringCreate event for existing strings: %+v", event.Payload())
		case <-time.After(50 * time.Millisecond):
			// Expected behavior
		}
	})

	t.Run("OnSeriesCreateHook", func(t *testing.T) {
		signalChan := make(chan hooks.HookEvent, 2)
		listener := &mockListener{isAsync: true, callSignal: signalChan}
		concreteEngine.hookManager.Register(hooks.EventOnSeriesCreate, listener)

		// Action 1: Put a point for a new series
		metric := "new.series.metric"
		tags := map[string]string{"host": "server-x"}
		require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, metric, tags, 10, map[string]interface{}{"value": 10.0})))

		// Verification 1: Expect 1 event
		metricID, _ := concreteEngine.stringStore.GetID(metric)
		tagKeyID, _ := concreteEngine.stringStore.GetID("host")
		tagValID, _ := concreteEngine.stringStore.GetID("server-x")
		expectedSeriesKey := core.EncodeSeriesKey(metricID, []core.EncodedSeriesTagPair{{KeyID: tagKeyID, ValueID: tagValID}})

		select {
		case event := <-signalChan:
			payload, ok := event.Payload().(hooks.SeriesCreatePayload)
			require.True(t, ok)
			assert.Greater(t, payload.SeriesID, uint64(0))
			assert.Equal(t, string(expectedSeriesKey), payload.SeriesKey)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timed out waiting for OnSeriesCreate event")
		}

		// Action 2: Put another point for the same series
		require.NoError(t, engine.Put(context.Background(), HelperDataPoint(t, metric, tags, 11, map[string]interface{}{"value": 11.0})))

		// Verification 2: Expect no new events
		select {
		case event := <-signalChan:
			t.Fatalf("Received unexpected OnSeriesCreate event for existing series: %+v", event.Payload())
		case <-time.After(50 * time.Millisecond):
			// Expected behavior
		}
	})
}