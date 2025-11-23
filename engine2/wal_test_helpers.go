package engine2

import (
	"testing"

	"github.com/INLOpen/nexusbase/indexer"
	"github.com/stretchr/testify/require"
)

// crashEngine simulates an abrupt server crash by closing important
// on-disk resources (WAL, string store) without performing a graceful
// engine shutdown. Tests use this to create WAL segments and then start
// a fresh engine to validate recovery behavior.
func crashEngine(t *testing.T, opts StorageEngineOptions, fn func(e StorageEngineInterface)) {
	t.Helper()

	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	require.NoError(t, engine.Start())

	if fn != nil {
		fn(engine)
	}

	// Try to close underlying resources to simulate a crash. We avoid calling
	// the engine's Close() because that would perform a clean shutdown (flush
	// manifests, etc.). Instead, close the WAL and string store via the
	// public interfaces.
	if w := engine.GetWAL(); w != nil {
		_ = w.Sync()
		_ = w.Close()
		// If the WAL implementation exposes rotation, leave files as-is.
		// Do not call Purge or other maintenance.
		_ = w // keep linter happy
	}

	if ss := engine.GetStringStore(); ss != nil {
		// indexer.StringStoreInterface defines Close().
		if closer, ok := ss.(indexer.StringStoreInterface); ok {
			_ = closer.Sync()
			_ = closer.Close()
		}
	}

	// Intentionally do not call engine.Close() to simulate an unclean crash.
}
