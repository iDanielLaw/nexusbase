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

	// Replace the real TagIndexManager with a no-op implementation for
	// crash-only test runs. This prevents the full TagIndexManager from
	// starting background goroutines (flush/compaction) which can leak
	// across tests and interfere with deterministic test sequences.
	// We do this after Start() so the engine is otherwise initialized the
	// same way; the noop manager is used only to avoid background activity.
	if a, ok := engine.(*Engine2Adapter); ok {
		// best-effort swap; if NewNoopTagIndexManager isn't available,
		// the original manager will remain in place.
		if noop := indexer.NewNoopTagIndexManager(); noop != nil {
			a.tagIndexManager = noop
		}
	}

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

	// Attempt best-effort shutdown of background managers to avoid leaking
	// goroutines into subsequent tests. We still avoid a full, graceful
	// engine.Close() because that would flush memtables and persist manifests
	// which would change the on-disk state and defeat the purpose of simulating
	// an unclean crash. However, stopping background loops (e.g. tag index
	// manager) prevents interference with later tests while leaving on-disk
	// artifacts intact.
	if a, ok := engine.(*Engine2Adapter); ok {
		if a.tagIndexManager != nil {
			a.tagIndexManager.Stop()
		}

		// Best-effort: stop any other TagIndexManager instances that may have
		// been created (tests exercise multiple engine lifecycles). This avoids
		// leaked background goroutines interfering with later tests.
		indexer.StopAllTagIndexManagersForTest()
		if a.manifestMgr != nil {
			a.manifestMgr.Close()
		}
		if a.leaderWal != nil {
			_ = a.leaderWal.Close()
		}
		// avoid leaving active memtable references reachable
		a.mem = nil
		a.started.Store(false)
	}

	// Intentionally do not call engine.Close() to simulate an unclean crash.
}
