package listeners

import (
	"context"
	"encoding/json"
	"expvar"
	"testing"

	"github.com/INLOpen/nexusbase/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAmplificationListener_OnEvent(t *testing.T) {
	// Reset expvar for a clean test run. This is necessary because expvars are global.
	// In a real application, this reset is not needed.
	initWAFMetrics() // Ensure metrics are initialized
	totalBytesRead.Set(0)
	totalBytesWritten.Set(0)
	compactionEvents.Set(0)

	listener := NewWriteAmplificationListener(nil)
	require.NotNil(t, listener)

	// Create a mock PostCompaction event
	payload := hooks.PostCompactionPayload{
		SourceLevel: 0,
		TargetLevel: 1,
		OldTables: []hooks.CompactedTableInfo{
			{ID: 1, Size: 1000},
			{ID: 2, Size: 1500}, // Total read: 2500
		},
		NewTables: []hooks.CompactedTableInfo{
			{ID: 3, Size: 2000}, // Total written: 2000
		},
	}
	event := hooks.NewPostCompactionEvent(payload)

	// Trigger the event
	err := listener.OnEvent(context.Background(), event)
	require.NoError(t, err)

	// Check if metrics were updated
	assert.Equal(t, int64(2500), totalBytesRead.Value(), "totalBytesRead should be updated")
	assert.Equal(t, int64(2000), totalBytesWritten.Value(), "totalBytesWritten should be updated")
	assert.Equal(t, int64(1), compactionEvents.Value(), "compactionEvents should be updated")

	// Check the WAF calculation from the expvar.Func
	wafVar := expvar.Get("engine_compaction_waf")
	require.NotNil(t, wafVar)

	// The value from expvar.Func is a JSON-encoded float
	var wafValue float64
	err = json.Unmarshal([]byte(wafVar.String()), &wafValue)
	require.NoError(t, err)
	assert.InDelta(t, float64(2000)/float64(2500), wafValue, 1e-9, "WAF should be calculated correctly")

	// Trigger another event to test accumulation
	payload2 := hooks.PostCompactionPayload{
		SourceLevel: 1,
		TargetLevel: 2,
		OldTables: []hooks.CompactedTableInfo{
			{ID: 4, Size: 500}, // Total read: 2500 + 500 = 3000
		},
		NewTables: []hooks.CompactedTableInfo{
			{ID: 5, Size: 400}, // Total written: 2000 + 400 = 2400
		},
	}
	event2 := hooks.NewPostCompactionEvent(payload2)
	err = listener.OnEvent(context.Background(), event2)
	require.NoError(t, err)

	assert.Equal(t, int64(3000), totalBytesRead.Value(), "totalBytesRead should be accumulated")
	assert.Equal(t, int64(2400), totalBytesWritten.Value(), "totalBytesWritten should be accumulated")
	assert.Equal(t, int64(2), compactionEvents.Value(), "compactionEvents should be accumulated")

	err = json.Unmarshal([]byte(wafVar.String()), &wafValue)
	require.NoError(t, err)
	assert.InDelta(t, float64(2400)/float64(3000), wafValue, 1e-9, "Accumulated WAF should be calculated correctly")
}

func TestWriteAmplificationListener_OnEvent_WrongPayload(t *testing.T) {
	// Reset expvar
	totalBytesRead.Set(0)
	totalBytesWritten.Set(0)
	compactionEvents.Set(0)

	listener := NewWriteAmplificationListener(nil)

	// Create an event of a different type
	event := hooks.NewPrePutDataPointEvent(hooks.PrePutDataPointPayload{})

	// Trigger the event and ensure no error and no metric changes
	require.NoError(t, listener.OnEvent(context.Background(), event))
	assert.Equal(t, int64(0), totalBytesRead.Value())
	assert.Equal(t, int64(0), totalBytesWritten.Value())
	assert.Equal(t, int64(0), compactionEvents.Value())
}
