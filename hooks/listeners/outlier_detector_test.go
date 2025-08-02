package listeners

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutlierDetectionListener_OnEvent(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	// Define rules for the listener
	rules := []OutlierRule{
		{
			MetricName: "cpu.temp",
			FieldName:  "degrees_celsius",
			Thresholds: Thresholds{Min: 0, Max: 90},
		},
		{
			MetricName: "http.requests",
			FieldName:  "latency_ms",
			Thresholds: Thresholds{Min: 1, Max: 1000},
		},
	}

	listener := NewOutlierDetectionListener(logger, rules)
	require.NotNil(t, listener)

	t.Run("DetectsFloatOutlier", func(t *testing.T) {
		logBuf.Reset()

		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{
			"degrees_celsius": 95.5, // Outlier ( > 90 )
		})
		require.NoError(t, err)

		points := []core.DataPoint{
			{Metric: "cpu.temp", Tags: map[string]string{"host": "server-1"}, Fields: fields},
		}
		event := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})

		err = listener.OnEvent(context.Background(), event)
		require.NoError(t, err)

		logOutput := logBuf.String()
		assert.Contains(t, logOutput, "Outlier detected", "Log should contain the alert message")
		assert.Contains(t, logOutput, `"metric":"cpu.temp"`, "Log should contain the metric name")
		assert.Contains(t, logOutput, `"field":"degrees_celsius"`, "Log should contain the field name")
		assert.Contains(t, logOutput, `"value":95.5`, "Log should contain the outlier value")
		assert.Contains(t, logOutput, `"max_threshold":90`, "Log should contain the max threshold")
	})

	t.Run("DetectsIntOutlier", func(t *testing.T) {
		logBuf.Reset()

		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{
			"latency_ms": int64(2000), // Outlier ( > 1000 )
		})
		require.NoError(t, err)

		points := []core.DataPoint{
			{Metric: "http.requests", Tags: map[string]string{"endpoint": "/api/v1/users"}, Fields: fields},
		}
		event := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})

		err = listener.OnEvent(context.Background(), event)
		require.NoError(t, err)

		logOutput := logBuf.String()
		assert.Contains(t, logOutput, "Outlier detected")
		assert.Contains(t, logOutput, `"metric":"http.requests"`)
		assert.Contains(t, logOutput, `"value":2000`)
	})

	t.Run("IgnoresInlierValue", func(t *testing.T) {
		logBuf.Reset()
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"degrees_celsius": 50.0})
		require.NoError(t, err)
		points := []core.DataPoint{{Metric: "cpu.temp", Fields: fields}}
		event := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})
		err = listener.OnEvent(context.Background(), event)
		require.NoError(t, err)
		assert.Empty(t, logBuf.String(), "Listener should not log for inlier values")
	})

	t.Run("IgnoresUnconfiguredMetric", func(t *testing.T) {
		logBuf.Reset()
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"bytes": 9999999999})
		require.NoError(t, err)
		points := []core.DataPoint{{Metric: "memory.usage", Fields: fields}}
		event := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})
		err = listener.OnEvent(context.Background(), event)
		require.NoError(t, err)
		assert.Empty(t, logBuf.String(), "Listener should not log for unconfigured metrics")
	})

	t.Run("IgnoresNonNumericField", func(t *testing.T) {
		logBuf.Reset()
		fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"degrees_celsius": "very hot"})
		require.NoError(t, err)
		points := []core.DataPoint{{Metric: "cpu.temp", Fields: fields}}
		event := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})
		err = listener.OnEvent(context.Background(), event)
		require.NoError(t, err)
		assert.Empty(t, logBuf.String(), "Listener should not log for non-numeric fields")
	})
}
