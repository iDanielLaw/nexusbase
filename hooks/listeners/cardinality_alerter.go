package listeners

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/INLOpen/nexusbase/hooks"
)

// CardinalityAlerterListener logs a warning when a new time series is created.
// This can be used to monitor for high cardinality issues.
type CardinalityAlerterListener struct {
	logger *slog.Logger
}

// NewCardinalityAlerterListener creates a new listener for monitoring series creation.
func NewCardinalityAlerterListener(logger *slog.Logger) *CardinalityAlerterListener {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &CardinalityAlerterListener{
		logger: logger.With("component", "CardinalityAlerterListener"),
	}
}

// OnEvent handles the OnSeriesCreate event.
func (l *CardinalityAlerterListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	if event.Type() != hooks.EventOnSeriesCreate {
		return nil // Ignore other events
	}

	payload, ok := event.Payload().(hooks.SeriesCreatePayload)
	if !ok {
		l.logger.Error("Received OnSeriesCreate event with incorrect payload type", "payload_type", fmt.Sprintf("%T", event.Payload()))
		return nil
	}

	l.logger.Warn("New time series created (cardinality increase)",
		"series_id", payload.SeriesID,
		"series_key_hex", fmt.Sprintf("%x", payload.SeriesKey),
	)

	return nil
}

// Priority defines the execution order.
func (l *CardinalityAlerterListener) Priority() int { return 100 }

// IsAsync indicates this listener can run in the background.
func (l *CardinalityAlerterListener) IsAsync() bool { return true }
