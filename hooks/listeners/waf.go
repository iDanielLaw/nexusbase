package listeners

import (
	"context"
	"expvar"
	"io"
	"log/slog"
	"sync"

	"github.com/INLOpen/nexusbase/hooks"
)

// WriteAmplificationListener calculates and exposes metrics about data amplification during compaction.
var (
	// Use sync.Once to ensure these expvars are only ever created once,
	// making NewWriteAmplificationListener idempotent.
	wafMetricsOnce    sync.Once
	totalBytesRead    *expvar.Int
	totalBytesWritten *expvar.Int
	compactionEvents  *expvar.Int
)

func initWAFMetrics() {
	wafMetricsOnce.Do(func() {
		totalBytesRead = expvar.NewInt("engine_compaction_bytes_read_total")
		totalBytesWritten = expvar.NewInt("engine_compaction_bytes_written_total")
		compactionEvents = expvar.NewInt("engine_compaction_events_total")
		// Expose the calculated WAF as a float.
		// This function will be called by the metrics endpoint each time it's scraped.
		expvar.Publish("engine_compaction_waf", expvar.Func(func() interface{} {
			read := totalBytesRead.Value()
			if read == 0 {
				return 0.0 // Avoid division by zero.
			}
			return float64(totalBytesWritten.Value()) / float64(read)
		}))
	})
}

type WriteAmplificationListener struct {
	logger *slog.Logger

	// Metrics to track
	totalBytesRead    *expvar.Int
	totalBytesWritten *expvar.Int
	compactionEvents  *expvar.Int
}

// NewWriteAmplificationListener creates a new listener.
func NewWriteAmplificationListener(logger *slog.Logger) *WriteAmplificationListener {
	if logger == nil {
		// Default to a discard logger to prevent nil panics if no logger is provided.
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	initWAFMetrics() // This will only run the registration logic once.
	return &WriteAmplificationListener{
		logger:            logger.With("component", "WriteAmplificationListener"),
		totalBytesRead:    totalBytesRead,
		totalBytesWritten: totalBytesWritten,
		compactionEvents:  compactionEvents,
	}
}

// OnEvent is called when a PostCompaction event is triggered.
func (l *WriteAmplificationListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	payload, ok := event.Payload().(hooks.PostCompactionPayload)
	if !ok {
		// This listener only cares about PostCompaction events.
		return nil
	}

	var bytesRead int64
	for _, tableInfo := range payload.OldTables {
		bytesRead += tableInfo.Size
	}
	var bytesWritten int64
	for _, tableInfo := range payload.NewTables {
		bytesWritten += tableInfo.Size
	}

	l.totalBytesRead.Add(bytesRead)
	l.totalBytesWritten.Add(bytesWritten)
	l.compactionEvents.Add(1)

	l.logger.Info("Compaction event processed",
		"source_level", payload.SourceLevel,
		"target_level", payload.TargetLevel,
		"bytes_read", bytesRead,
		"bytes_written", bytesWritten,
	)

	// This is an async post-hook, so we don't return an error.
	return nil
}

// Priority defines the execution order. Lower numbers run first.
func (l *WriteAmplificationListener) Priority() int {
	return 100 // A lower priority is fine for metrics.
}

// IsAsync indicates this listener can run in the background.
func (l *WriteAmplificationListener) IsAsync() bool {
	return true
}

/**
// cmd/server/main.go
// Example: Engine2-first startup and hook registration.
// This snippet demonstrates constructing an Engine2 instance and adapting
// it to the repository StorageEngineInterface so we can obtain the
// HookManager and register listeners (preferred over starting the legacy
// `engine` directly).
//
// Note: this example is intentionally explicit so callers can see how to
// pass a HookManager into the Engine2 adapter when creating a server.
//
// ... imports ...
// import (
//     "github.com/INLOpen/nexusbase/internal"
//     "github.com/INLOpen/nexusbase/engine2"
//     "github.com/INLOpen/nexusbase/listeners"
// )
//
// func main() {
//     // ... (Load config, create logger) ...
//
//     // Create a raw Engine2 instance rooted at the configured data dir.
//     eng2, err := engine2.NewEngine2(opts.DataDir)
//     if err != nil {
//         logger.Error("Failed to create Engine2 instance", "error", err)
//         os.Exit(1)
//     }
//
//     // Wrap Engine2 with the adapter and provide a HookManager so hooks
//     // and listeners can be registered by callers.
//     hookManager := hooks.NewHookManager(logger.With("component", "HookManager"))
//     engAdapter := engine2.NewEngine2AdapterWithHooks(eng2, hookManager)
//     if startErr := engAdapter.Start(); startErr != nil {
//         logger.Error("Failed to start Engine2 adapter", "error", startErr)
//         _ = engAdapter.Close()
//         os.Exit(1)
//     }
//
//     // --- Register Hooks ---
//     waListener := listeners.NewWriteAmplificationListener(logger)
//     hookManager.Register(hooks.EventPostCompaction, waListener)
//     logger.Info("Registered WriteAmplificationListener for PostCompaction events.")
//     // --- End Register Hooks ---
//
//     // Create and initialize the application server using the adapter as
//     // the primary storage engine instance.
//     appServer, err := server.NewAppServer(engAdapter, cfg, logger)
//     // ... (rest of the main function) ...
// }
//
*/
