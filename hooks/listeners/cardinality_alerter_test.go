package listeners

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/INLOpen/nexusbase/hooks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCardinalityAlerterListener_OnEvent(t *testing.T) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))

	listener := NewCardinalityAlerterListener(logger)
	require.NotNil(t, listener)

	t.Run("Handles OnSeriesCreate event", func(t *testing.T) {
		logBuf.Reset() // Clear buffer for this sub-test

		payload := hooks.SeriesCreatePayload{
			SeriesKey: "metric.name\x00host=server1",
			SeriesID:  123,
		}
		event := hooks.NewOnSeriesCreateEvent(payload)

		err := listener.OnEvent(context.Background(), event)
		require.NoError(t, err)

		logOutput := logBuf.String()
		assert.Contains(t, logOutput, "New time series created", "Log should contain the alert message")
		assert.Contains(t, logOutput, `"series_id":123`, "Log should contain the series ID")
	})

	t.Run("Ignores other event types", func(t *testing.T) {
		logBuf.Reset()
		event := hooks.NewPostCompactionEvent(hooks.PostCompactionPayload{})
		require.NoError(t, listener.OnEvent(context.Background(), event))
		assert.Empty(t, logBuf.String(), "Listener should not log for non-OnSeriesCreate events")
	})
}
