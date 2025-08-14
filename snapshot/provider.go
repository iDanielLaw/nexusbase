package snapshot

import (
	"context"
	"log/slog"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/nexusbase/wal"
	"go.opentelemetry.io/otel/trace"
)

// EngineProvider defines the set of methods the snapshot manager needs
// to access the internal state of the storage engine. This interface
// acts as a bridge, decoupling the snapshot logic from the main engine implementation.
type EngineProvider interface {
	// State & Config
	CheckStarted() error
	GetWAL() wal.WALInterface
	GetClock() utils.Clock
	GetLogger() *slog.Logger
	GetTracer() trace.Tracer
	GetHookManager() hooks.HookManager
	GetLevelsManager() levels.Manager
	GetTagIndexManager() indexer.TagIndexManagerInterface
	GetPrivateStringStore() internal.PrivateManagerStore
	GetPrivateSeriesIDStore() internal.PrivateManagerStore
	GetSSTableCompressionType() string
	GetSequenceNumber() uint64

	// Locking & State Manipulation
	Lock()
	Unlock()
	GetMemtablesForFlush() (memtables []*memtable.Memtable, newMemtable *memtable.Memtable)
	FlushMemtableToL0(mem *memtable.Memtable, parentCtx context.Context) error

	// State Access
	GetDeletedSeries() map[string]uint64
	GetRangeTombstones() map[string][]core.RangeTombstone
}
