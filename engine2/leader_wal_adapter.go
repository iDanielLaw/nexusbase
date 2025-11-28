package engine2

import (
	"fmt"
	"path/filepath"

	"github.com/INLOpen/nexusbase/wal"
)

// openLeaderWAL opens a WAL from the main `wal` package under the engine2 data root.
// This WAL is used to expose a `wal.WALInterface` to replication and snapshot code
// while engine2 keeps its own simple per-engine WAL file for replay.
func openLeaderWAL(dataRoot string) (wal.WALInterface, error) {
	// Use `wal/` directory for the leader WAL so tests that inspect
	// `dataDir/wal` find the expected segment files.
	dir := filepath.Join(dataRoot, "wal")
	opts := wal.Options{
		Dir: dir,
	}
	w, _, err := wal.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open leader WAL at %s: %w", dir, err)
	}
	return w, nil
}
