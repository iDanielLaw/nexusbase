package engine

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/INLOpen/nexusbase/cache"
	"github.com/INLOpen/nexusbase/checkpoint"
	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/INLOpen/nexuscore/utils/clock"

	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/memtable"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexusbase/wal"

	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

var (
	ErrEngineClosed         = errors.New("engine is closed or not started")
	ErrEngineAlreadyStarted = errors.New("engine is already started")
	ErrFlushInProgress      = errors.New("engine is inprogress flush")
)

type RoundingRule struct {
	QueryDurationThreshold time.Duration
	RoundingDuration       time.Duration
}

type StorageEngineOptions struct {
	DataDir                        string
	MemtableThreshold              int64
	MemtableFlushIntervalMs        int
	BlockCacheCapacity             int
	L0CompactionTriggerSize        int64
	WriteBufferSize                int
	Metrics                        *EngineMetrics
	MaxL0Files                     int
	CompactionIntervalSeconds      int
	TargetSSTableSize              int64
	LevelsTargetSizeMultiplier     int
	MaxLevels                      int
	MaxConcurrentLNCompactions     int
	BloomFilterFalsePositiveRate   float64
	SSTableDefaultBlockSize        int
	InitialSequenceNumber          uint64
	CheckpointIntervalSeconds      int
	TracerProvider                 trace.TracerProvider
	TestingOnlyFailFlushCount      *atomic.Int32
	ErrorOnSSTableLoadFailure      bool
	SSTableCompressor              core.Compressor
	TestingOnlyInjectWALCloseError error
	WALSyncMode                    core.WALSyncMode
	WALBatchSize                   int
	WALFlushIntervalMs             int
	WALPurgeKeepSegments           int
	WALMaxSegmentSize              int64
	RetentionPeriod                string
	MetadataSyncIntervalSeconds    int
	EnableTagBloomFilter           bool
	IndexMemtableThreshold         int64
	IndexFlushIntervalMs           int
	IndexCompactionIntervalSeconds int
	IndexMaxL0Files                int
	IndexMaxLevels                 int
	IndexBaseTargetSize            int64
	CompactionTombstoneWeight      float64
	CompactionOverlapWeight        float64
	Logger                         *slog.Logger
	Clock                          clock.Clock

	// EnableSSTablePreallocate controls whether the engine will attempt to
	// preallocate space for new SSTable segment files. This setting is
	// provided by configuration and can be used to disable preallocation
	// on platforms or deployments where it's undesirable.
	EnableSSTablePreallocate bool

	SelfMonitoringEnabled      bool
	SelfMonitoringPrefix       string
	SelfMonitoringIntervalMs   int
	RelativeQueryRoundingRules []RoundingRule
	CompactionFallbackStrategy levels.CompactionFallbackStrategy

	IntraL0CompactionTriggerFiles     int
	IntraL0CompactionMaxFileSizeBytes int64
	ReplicationMode                   string
	LeaderAddress                     string // สำหรับ self-monitoring metrics โดยเฉพาะ
}

type storageEngine struct {
	opts StorageEngineOptions
	mu   sync.RWMutex

	nextSSTableID                 atomic.Uint64
	validator                     *core.Validator
	isStarted                     atomic.Bool
	isClosing                     atomic.Bool
	sequenceNumber                atomic.Uint64
	replicationMode               string
	stateLoader                   *StateLoader
	serviceManager                *ServiceManager
	mutableMemtable               *memtable.Memtable
	processImmutableMemtablesFunc func(writeCheckpoint bool)
	triggerPeriodicFlushFunc      func()
	syncMetadataFunc              func()
	immutableMemtables            []*memtable.Memtable

	wal           wal.WALInterface
	levelsManager levels.Manager
	blockCache    cache.Interface
	compactor     CompactionManagerInterface

	flushChan      chan struct{}
	forceFlushChan chan chan error
	shutdownChan   chan struct{}
	wg             sync.WaitGroup
	logger         *slog.Logger

	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	sstDir           string
	dlqDir           string
	snapshotsBaseDir string
	seriesLogFile    sys.FileHandle
	seriesLogMu      sync.Mutex
	activeSeries     map[string]struct{}
	stringStore      indexer.StringStoreInterface
	activeSeriesMu   sync.RWMutex

	deletedSeries     map[string]uint64
	deletedSeriesMu   sync.RWMutex
	rangeTombstones   map[string][]core.RangeTombstone
	rangeTombstonesMu sync.RWMutex

	snapshotManager   snapshot.ManagerInterface
	tagIndexManager   indexer.TagIndexManagerInterface
	tagIndexManagerMu sync.RWMutex

	seriesIDStore   indexer.SeriesIDStoreInterface
	engineStartTime time.Time
	bitmapCache     cache.Interface
	pubsub          PubSubInterface
	metrics         *EngineMetrics
	hookManager     hooks.HookManager

	internalFile internalFileManage

	clock clock.Clock

	setCompactorFactory func(StorageEngineOptions, *storageEngine) (CompactionManagerInterface, error)
	putBatchInterceptor func(ctx context.Context, points []core.DataPoint) error
}

var _ StorageEngineInterface = (*storageEngine)(nil)

func NewStorageEngine(opts StorageEngineOptions) (engine StorageEngineInterface, err error) {
	concreteEngine, err := initializeStorageEngine(opts)
	return concreteEngine, err
}

func initializeStorageEngine(opts StorageEngineOptions) (engine *storageEngine, err error) {
	defer func() {
		if err != nil && engine != nil {
			engine.CleanupEngine()
		}
	}()

	var logger *slog.Logger
	if opts.Logger == nil {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn})).With("component", "StorageEngine")
	} else {
		logger = opts.Logger.With("component", "StorageEngine")
	}

	var clk clock.Clock
	if opts.Clock == nil {
		clk = clock.SystemClockDefault
	} else {
		clk = opts.Clock
	}

	if opts.BloomFilterFalsePositiveRate < 0 || opts.BloomFilterFalsePositiveRate > 1 {
		logger.Warn("BloomFilter False Positive Rate must be between 0 and 1. Defaulting to 0.01.")
		opts.BloomFilterFalsePositiveRate = 0.01
	}

	concreteEngine := &storageEngine{
		validator:          core.NewValidator(),
		opts:               opts,
		replicationMode:    opts.ReplicationMode,
		immutableMemtables: make([]*memtable.Memtable, 0),
		flushChan:          make(chan struct{}, 1),
		forceFlushChan:     make(chan chan error),
		shutdownChan:       make(chan struct{}),
		engineStartTime:    clk.Now(),
		activeSeries:       make(map[string]struct{}),
		deletedSeries:      make(map[string]uint64),
		pubsub:             NewPubSub(),
		rangeTombstones:    make(map[string][]core.RangeTombstone),
		logger:             logger,
		metrics:            opts.Metrics,
		hookManager:        hooks.NewHookManager(logger.With("component", "HookManager")),
		clock:              clk,
		internalFile: internalFileManage{
			Create:   sys.Create,
			Open:     sys.Open,
			OpenFile: sys.OpenFile,
		},
	}

	if opts.TracerProvider != nil {
		concreteEngine.tracer = opts.TracerProvider.Tracer("github.com/INLOpen/nexusbase/engine")
	} else {
		concreteEngine.tracer = noop.NewTracerProvider().Tracer("")
	}

	concreteEngine.hookManager = hooks.NewHookManager(logger.With("component", "HookManager"))
	concreteEngine.stringStore = indexer.NewStringStore(logger.With("sub_component", "StringStore"), concreteEngine.hookManager)
	concreteEngine.bitmapCache = cache.NewLRUCache(opts.BlockCacheCapacity, nil, nil, nil)
	concreteEngine.seriesIDStore = indexer.NewSeriesIDStore(concreteEngine.logger.With("sub_component", "SeriesIDStore"), concreteEngine.hookManager)

	if concreteEngine.opts.SSTableCompressor == nil {
		concreteEngine.opts.SSTableCompressor = &compressors.NoCompressionCompressor{}
	}

	concreteEngine.stateLoader = NewStateLoader(concreteEngine)
	concreteEngine.serviceManager = NewServiceManager(concreteEngine)

	concreteEngine.processImmutableMemtablesFunc = concreteEngine.processImmutableMemtables
	concreteEngine.triggerPeriodicFlushFunc = concreteEngine.triggerPeriodicFlush
	concreteEngine.syncMetadataFunc = concreteEngine.syncMetadata

	concreteEngine.sstDir = filepath.Join(opts.DataDir, "sst")
	concreteEngine.dlqDir = filepath.Join(opts.DataDir, "dlq")
	concreteEngine.snapshotsBaseDir = filepath.Join(opts.DataDir, "snapshots")
	if err = concreteEngine.initializeDirectories(); err != nil {
		err = fmt.Errorf("failed to initialize directories: %w", err)
		engine = concreteEngine
		return
	}

	concreteEngine.snapshotManager = snapshot.NewManager(concreteEngine)

	return concreteEngine, nil
}

func (e *storageEngine) Start() error {
	if err := e.hookManager.Trigger(context.Background(), hooks.NewPreStartEngineEvent()); err != nil {
		return fmt.Errorf("engine start cancelled by pre-hook: %w", err)
	}

	if !e.isStarted.CompareAndSwap(false, true) {
		return ErrEngineAlreadyStarted
	}
	e.isClosing.Store(false)

	testFilePath := filepath.Join(e.opts.DataDir, ".writable_test")
	if testFile, testErr := os.Create(testFilePath); testErr != nil {
		e.logger.Error("Data directory is not writable.", "path", e.opts.DataDir, "error", testErr)
		return fmt.Errorf("data directory %s is not writable: %w", e.opts.DataDir, testErr)
	} else {
		_ = testFile.Close()
		_ = os.Remove(testFilePath)
	}

	e.initializeMetrics()
	if err := e.initializeLSMTreeComponents(); err != nil {
		e.logger.Error("NewStorageEngine failed during initializeLSMTreeComponents.", "error", err)
		return fmt.Errorf("failed to initialize LSM tree components: %w", err)
	}
	if err := e.initializeTagIndexManager(); err != nil {
		return fmt.Errorf("failed to initialize tag index manager: %w", err)
	}

	if err := e.stateLoader.Load(); err != nil {
		e.logger.Error("Failed to load engine state.", "error", err)
		return err
	}

	seriesLogPath := filepath.Join(e.opts.DataDir, "series.log")
	seriesFile, err := e.internalFile.OpenFile(seriesLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open series log file for writing: %w", err)
	}
	e.seriesLogFile = seriesFile
	e.serviceManager.Start()
	e.logger.Info("StorageEngine background services started.", "data_dir", e.opts.DataDir)

	e.hookManager.Trigger(context.Background(), hooks.NewPostStartEngineEvent())

	return nil
}

func (e *storageEngine) GetNextSSTableID() uint64 {
	if err := e.CheckStarted(); err != nil {
		panic(fmt.Errorf("GetNextSSTableID called on a non-running engine: %w", err))
	}
	return e.nextSSTableID.Add(1)
}

func (e *storageEngine) initializeDirectories() error {
	if e.opts.DataDir == "" {
		return fmt.Errorf("data directory must be specified")
	}

	info, err := os.Stat(e.opts.DataDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("data directory %s exists but is not a directory", e.opts.DataDir)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to stat data directory %s: %w", e.opts.DataDir, err)
	}
	if err := os.MkdirAll(e.opts.DataDir, 0755); err != nil {
		e.logger.Error("failed to create data directory", "path", e.opts.DataDir, "error", err)
		return fmt.Errorf("failed to create data directory %s: %w", e.opts.DataDir, err)
	}
	if err := os.MkdirAll(e.sstDir, 0755); err != nil {
		e.logger.Error("failed to create sst directory", "path", e.sstDir, "error", err)
		return fmt.Errorf("failed to create sst directory %s: %w", e.sstDir, err)
	}
	if err := os.MkdirAll(e.dlqDir, 0755); err != nil {
		e.logger.Error("failed to create DLQ directory", "path", e.dlqDir, "error", err)
		return fmt.Errorf("failed to create DLQ directory %s: %w", e.dlqDir, err)
	}
	if err := os.MkdirAll(e.snapshotsBaseDir, 0755); err != nil {
		e.logger.Error("failed to create snapshots base directory", "path", e.snapshotsBaseDir, "error", err)
		return fmt.Errorf("failed to create snapshots base directory %s: %w", e.snapshotsBaseDir, err)
	}
	return nil
}

func (e *storageEngine) CleanupEngine() {
	e.logger.Info("Cleaning up engine resources after initialization failure...")

	if e.compactor != nil {
		e.compactor.Stop()
	}
	if e.tagIndexManager != nil {
		e.tagIndexManager.Stop()
	}
	if e.seriesIDStore != nil {
		e.seriesIDStore.Close()
	}
	if e.stringStore != nil {
		e.stringStore.Close()
	}
	e.closeWAL()
	e.closeSSTables()
	e.shutdownTracer()
}

func (e *storageEngine) Close() error {
	if !e.isStarted.Load() {
		e.logger.Info("Close called on a non-running or already closed engine.")
		return nil
	}
	if err := e.hookManager.Trigger(context.Background(), hooks.NewPreCloseEngineEvent()); err != nil {
		return fmt.Errorf("engine close cancelled by pre-hook: %w", err)
	}

	if !e.isClosing.CompareAndSwap(false, true) {
		e.logger.Info("Close operation already in progress.")
		return nil
	}

	if e.serviceManager != nil {
		e.serviceManager.Stop()
	}

	if e.hookManager != nil {
		e.hookManager.Stop()
	}

	var closeErr error
	closeErr = errors.Join(closeErr, e.flushRemainingMemtables())
	closeErr = errors.Join(closeErr, e.seriesIDStore.Close())
	if e.wal != nil {
		lastFlushedSegment := e.wal.ActiveSegmentIndex()
		if lastFlushedSegment > 0 {
			cp := core.Checkpoint{LastSafeSegmentIndex: lastFlushedSegment}
			if writeErr := checkpoint.Write(e.opts.DataDir, cp); writeErr != nil {
				e.logger.Error("Failed to write final checkpoint during close.", "error", writeErr)
				closeErr = errors.Join(closeErr, writeErr)
			} else {
				e.purgeWALSegments(lastFlushedSegment)
			}
		}
	}
	if e.seriesLogFile != nil {
		e.seriesLogFile.Close()
		e.seriesLogFile = nil
	}
	closeErr = errors.Join(closeErr, e.closeWAL())
	closeErr = errors.Join(closeErr, e.closeSSTables())
	closeErr = errors.Join(closeErr, e.shutdownTracer())

	if closeErr != nil {
		return fmt.Errorf("errors during close: %w", closeErr)
	}

	e.isStarted.Store(false)
	e.hookManager.Trigger(context.Background(), hooks.NewPostCloseEngineEvent())

	e.logger.Info("Shutdown complete.")
	return nil
}

func (e *storageEngine) CheckStarted() error {
	if !e.isStarted.Load() {
		return ErrEngineClosed
	}
	return nil
}

func (e *storageEngine) wipeDataDirectory() error {
	itemsToRemove := []string{
		e.sstDir,
		e.dlqDir,
		filepath.Join(e.opts.DataDir, "wal"),
		filepath.Join(e.opts.DataDir, core.CurrentFileName),
		filepath.Join(e.opts.DataDir, core.CheckpointFileName),
		filepath.Join(e.opts.DataDir, "string_mapping.log"),
		filepath.Join(e.opts.DataDir, "series_mapping.log"),
		filepath.Join(e.opts.DataDir, "series.log"),
		filepath.Join(e.opts.DataDir, "deleted_series.bin"),
		filepath.Join(e.opts.DataDir, "range_tombstones.bin"),
		filepath.Join(e.opts.DataDir, "tag_index"),
	}

	files, err := filepath.Glob(filepath.Join(e.opts.DataDir, "MANIFEST*"))
	if err != nil {
		return fmt.Errorf("failed to glob for manifest files to wipe: %w", err)
	}
	itemsToRemove = append(itemsToRemove, files...)

	var firstErr error
	for _, itemPath := range itemsToRemove {
		if err := os.RemoveAll(itemPath); err != nil {
			e.logger.Warn("Failed to remove item during data wipe.", "path", itemPath, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if err := e.initializeDirectories(); err != nil {
		return fmt.Errorf("failed to re-initialize directories after wipe: %w", err)
	}

	return firstErr
}

func CopyFile(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("failed to stat source file %s for copying: %w", src, err)
	}
	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s for copying: %w", src, err)
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s for copying: %w", dst, err)
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	if err != nil {
		return fmt.Errorf("failed to copy data from %s to %s: %w", src, dst, err)
	}
	return nil
}

func (e *storageEngine) initializeMetrics() {
	if e.metrics == nil {
		e.metrics = NewEngineMetrics(true, "engine_")
	}

	if e.metrics.PublishedGlobally {
		publishExpvarFunc("engine_cache_hit_rate", func() interface{} {
			if e.blockCache == nil {
				return 0.0
			}
			return e.blockCache.GetHitRate()
		})
		publishExpvarFunc("engine_mutable_memtable_size_bytes", func() interface{} {
			e.mu.RLock()
			defer e.mu.RUnlock()
			if e.mutableMemtable == nil {
				return 0
			}
			return e.mutableMemtable.Size()
		})
		publishExpvarFunc("engine_active_time_series_count", func() interface{} {
			e.activeSeriesMu.RLock()
			defer e.activeSeriesMu.RUnlock()
			return len(e.activeSeries)
		})
		publishExpvarFunc("engine_immutable_memtables_count", func() interface{} {
			e.mu.RLock()
			defer e.mu.RUnlock()
			return len(e.immutableMemtables)
		})
		publishExpvarFunc("engine_immutable_memtables_total_size_bytes", func() interface{} {
			e.mu.RLock()
			defer e.mu.RUnlock()
			var totalSize int64
			for _, mt := range e.immutableMemtables {
				if mt != nil {
					totalSize += mt.Size()
				}
			}
			return totalSize
		})
		for i := 0; i < e.opts.MaxLevels; i++ {
			levelNum := i
			publishExpvarFunc(fmt.Sprintf("engine_level_%d_table_count", levelNum), func() interface{} {
				if e.levelsManager == nil {
					return 0
				}
				return len(e.levelsManager.GetTablesForLevel(levelNum))
			})
			publishExpvarFunc(fmt.Sprintf("engine_level_%d_size_bytes", levelNum), func() interface{} {
				if e.levelsManager == nil {
					return 0
				}
				return e.levelsManager.GetTotalSizeForLevel(levelNum)
			})
		}
		publishExpvarFunc("engine_ingestion_rate_points_per_second", func() interface{} {
			if e.metrics.PutTotal == nil || e.engineStartTime.IsZero() {
				return 0.0
			}
			durationSeconds := e.clock.Now().Sub(e.engineStartTime).Seconds()
			if durationSeconds == 0 {
				return 0.0
			}
			return float64(e.metrics.PutTotal.Value()) / durationSeconds
		})
		publishExpvarFunc("engine_queries_per_second", func() interface{} {
			if e.metrics.QueryTotal == nil || e.engineStartTime.IsZero() {
				return 0.0
			}
			durationSeconds := e.clock.Now().Sub(e.engineStartTime).Seconds()
			if durationSeconds == 0 {
				return 0.0
			}
			return float64(e.metrics.QueryTotal.Value()) / durationSeconds
		})
		publishExpvarFunc("engine_buffer_pool_hits_total", func() any {
			h, _, _, _ := core.BufferPool.GetMetrics()
			return h
		})
		publishExpvarFunc("engine_buffer_pool_misses_total", func() any {
			_, m, _, _ := core.BufferPool.GetMetrics()
			return m
		})
		publishExpvarFunc("engine_buffer_pool_created_total", func() any {
			_, _, c, _ := core.BufferPool.GetMetrics()
			return c
		})
		publishExpvarFunc("engine_buffer_pool_size_bytes", func() any {
			_, _, _, s := core.BufferPool.GetMetrics()
			return s
		})

		// Publish preallocation counters from the sys package so operators can
		// observe how often preallocation succeeds, fails, or is unsupported.
		publishExpvarFunc("engine_prealloc_successes_total", func() interface{} {
			return int64(sys.PreallocSuccessCount())
		})
		publishExpvarFunc("engine_prealloc_failures_total", func() interface{} {
			return int64(sys.PreallocFailureCount())
		})
		publishExpvarFunc("engine_prealloc_unsupported_total", func() interface{} {
			return int64(sys.PreallocUnsupportedCount())
		})
		publishExpvarFunc("engine_memtable_key_pool_hits_total", func() any {
			h, _, _ := memtable.KeyPool.GetMetrics()
			return h
		})
		publishExpvarFunc("engine_memtable_key_pool_misses_total", func() any {
			_, m, _ := memtable.KeyPool.GetMetrics()
			return m
		})
		publishExpvarFunc("engine_memtable_key_pool_size", func() any {
			_, _, s := memtable.KeyPool.GetMetrics()
			return s
		})
		publishExpvarFunc("engine_memtable_entry_pool_hits_total", func() any {
			h, _, _ := memtable.EntryPool.GetMetrics()
			return h
		})
		publishExpvarFunc("engine_memtable_entry_pool_misses_total", func() any {
			_, m, _ := memtable.EntryPool.GetMetrics()
			return m
		})
		publishExpvarFunc("engine_memtable_entry_pool_size", func() any {
			_, _, s := memtable.EntryPool.GetMetrics()
			return s
		})
	}

	e.metrics.activeSeriesCountFunc = func() interface{} {
		e.activeSeriesMu.RLock()
		defer e.activeSeriesMu.RUnlock()
		return len(e.activeSeries)
	}

	// Expose whether SSTable preallocation is enabled via metrics (1 = enabled, 0 = disabled)
	if e.metrics.PreallocateEnabled != nil {
		if e.opts.EnableSSTablePreallocate {
			e.metrics.PreallocateEnabled.Set(1)
		} else {
			e.metrics.PreallocateEnabled.Set(0)
		}
	}

	// Populate the preallocation counters initially so non-global metric
	// collectors still have baseline values.
	if e.metrics.PreallocSuccesses != nil {
		e.metrics.PreallocSuccesses.Set(int64(sys.PreallocSuccessCount()))
	}
	if e.metrics.PreallocFailures != nil {
		e.metrics.PreallocFailures.Set(int64(sys.PreallocFailureCount()))
	}
	if e.metrics.PreallocUnsupported != nil {
		e.metrics.PreallocUnsupported.Set(int64(sys.PreallocUnsupportedCount()))
	}

	// If metrics are not published globally, spawn a background updater that
	// periodically refreshes the preallocation counters so non-global expvar
	// collectors see near-real-time values. The updater listens on the engine
	// shutdown channel and is synchronized with the engine WaitGroup.
	if !e.metrics.PublishedGlobally {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					if e.metrics.PreallocSuccesses != nil {
						e.metrics.PreallocSuccesses.Set(int64(sys.PreallocSuccessCount()))
					}
					if e.metrics.PreallocFailures != nil {
						e.metrics.PreallocFailures.Set(int64(sys.PreallocFailureCount()))
					}
					if e.metrics.PreallocUnsupported != nil {
						e.metrics.PreallocUnsupported.Set(int64(sys.PreallocUnsupportedCount()))
					}
				case <-e.shutdownChan:
					return
				}
			}
		}()
	}
	e.metrics.mutableMemtableSizeFunc = func() interface{} {
		e.mu.RLock()
		defer e.mu.RUnlock()
		if e.mutableMemtable == nil {
			return 0
		}
		return e.mutableMemtable.Size()
	}
	e.metrics.immutableMemtablesCountFunc = func() interface{} {
		e.mu.RLock()
		defer e.mu.RUnlock()
		return len(e.immutableMemtables)
	}
	e.metrics.immutableMemtablesTotalSizeBytesFunc = func() interface{} {
		e.mu.RLock()
		defer e.mu.RUnlock()
		var totalSize int64
		for _, mt := range e.immutableMemtables {
			if mt != nil {
				totalSize += mt.Size()
			}
		}
		return totalSize
	}
}

func (e *storageEngine) initializeLSMTreeComponents() error {
	e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
	onEvictedWithHook := func(key string, value interface{}) {
		if buf, ok := value.(*bytes.Buffer); ok {
			core.BufferPool.Put(buf)
		}
		e.hookManager.Trigger(context.Background(), hooks.NewOnCacheEvictionEvent(hooks.CachePayload{Key: key}))
	}
	onHitWithHook := func(key string) {
		e.hookManager.Trigger(context.Background(), hooks.NewOnCacheHitEvent(hooks.CachePayload{Key: key}))
	}
	onMissWithHook := func(key string) {
		e.hookManager.Trigger(context.Background(), hooks.NewOnCacheMissEvent(hooks.CachePayload{Key: key}))
	}

	e.blockCache = cache.NewLRUCache(
		e.opts.BlockCacheCapacity,
		onEvictedWithHook,
		onHitWithHook,
		onMissWithHook,
	)
	if e.metrics != nil && e.metrics.CacheHits != nil && e.metrics.CacheMisses != nil {
		if cacheWithMetrics, ok := e.blockCache.(interface {
			SetMetrics(*expvar.Int, *expvar.Int)
		}); ok {
			cacheWithMetrics.SetMetrics(e.metrics.CacheHits, e.metrics.CacheMisses)
		}
	}
	lm, err := levels.NewLevelsManager(
		e.opts.MaxLevels,
		e.opts.MaxL0Files,
		e.opts.TargetSSTableSize,
		e.tracer,
		e.opts.CompactionFallbackStrategy,
		e.opts.CompactionTombstoneWeight,
		e.opts.CompactionOverlapWeight)
	if err != nil {
		e.logger.Error("failed to create levels manager", "error", err)
		return fmt.Errorf("failed to create levels manager: %w", err)
	}
	e.levelsManager = lm

	if e.setCompactorFactory == nil {
		cmParams := CompactionManagerParams{
			Engine:  e,
			DataDir: e.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 e.opts.MaxL0Files,
				L0CompactionTriggerSize:    e.opts.L0CompactionTriggerSize,
				TargetSSTableSize:          e.opts.TargetSSTableSize,
				LevelsTargetSizeMultiplier: e.opts.LevelsTargetSizeMultiplier,
				CompactionIntervalSeconds:  e.opts.CompactionIntervalSeconds,
				MaxConcurrentLNCompactions: e.opts.MaxConcurrentLNCompactions,
				SSTableCompressor:          e.opts.SSTableCompressor,
				RetentionPeriod:            e.opts.RetentionPeriod,

				IntraL0CompactionTriggerFiles:     e.opts.IntraL0CompactionTriggerFiles,
				IntraL0CompactionMaxFileSizeBytes: e.opts.IntraL0CompactionMaxFileSizeBytes,
			},
			LevelsManager:        lm,
			Logger:               e.logger,
			Tracer:               e.tracer,
			IsSeriesDeleted:      e.isSeriesDeleted,
			IsRangeDeleted:       e.isCoveredByRangeTombstone,
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			Metrics:              e.metrics,
			BlockCache:           e.blockCache,
			FileRemover:          nil,
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
				return sstable.NewSSTableWriter(opts)
			},
			ShutdownChan: e.shutdownChan,
		}

		cm, cmErr := NewCompactionManager(cmParams)

		if cmErr != nil {
			e.logger.Error("failed to create compaction manager", "error", cmErr)
			return fmt.Errorf("failed to create compaction manager: %w", cmErr)
		}
		e.compactor = cm

		e.compactor.SetMetricsCounters(
			e.metrics.CompactionTotal, e.metrics.CompactionLatencyHist,
			e.metrics.CompactionDataReadBytesTotal,
			e.metrics.CompactionDataWrittenBytesTotal,
			e.metrics.CompactionTablesMergedTotal,
		)
	} else {
		compactor, err := e.setCompactorFactory(e.opts, e)
		if err != nil {
			return err
		}
		e.compactor = compactor
	}
	return nil
}

func (e *storageEngine) initializeTagIndexManager() error {
	tagIndexDeps := &indexer.TagIndexDependencies{
		StringStore:     e.stringStore,
		SeriesIDStore:   e.seriesIDStore,
		DeletedSeries:   e.deletedSeries,
		DeletedSeriesMu: &e.deletedSeriesMu,
		SSTNextID:       e.GetNextSSTableID,
	}

	timOpts := indexer.TagIndexManagerOptions{
		DataDir:                   e.opts.DataDir,
		MemtableThreshold:         e.opts.IndexMemtableThreshold,
		FlushIntervalMs:           e.opts.IndexFlushIntervalMs,
		CompactionIntervalSeconds: e.opts.IndexCompactionIntervalSeconds,
		L0CompactionTriggerSize:   e.opts.L0CompactionTriggerSize,
		MaxL0Files:                e.opts.IndexMaxL0Files,
		MaxLevels:                 e.opts.IndexMaxLevels,
		BaseTargetSize:            e.opts.IndexBaseTargetSize,
		Clock:                     e.clock,
	}

	var err error
	e.tagIndexManager, err = indexer.NewTagIndexManager(timOpts, tagIndexDeps, e.logger, e.tracer)
	return err
}

func (e *storageEngine) GetWALPath() string {
	if err := e.CheckStarted(); err != nil {
		return ""
	}
	if e.wal == nil {
		return ""
	}
	return e.wal.Path()
}

func (e *storageEngine) GetWAL() wal.WALInterface {
	return e.wal
}

func (e *storageEngine) GetSnapshotsBaseDir() string {
	return e.snapshotsBaseDir
}

func (e *storageEngine) GetDLQDir() string {
	return e.dlqDir
}

func getTableIDs(tables []*sstable.SSTable) []uint64 {
	ids := make([]uint64, len(tables))
	for i, tbl := range tables {
		ids[i] = tbl.ID()
	}
	return ids
}

type realFileRemover struct{}

func (r *realFileRemover) Remove(name string) error { return os.Remove(name) }

func (e *storageEngine) closeWAL() error {
	if e.wal == nil {
		return nil
	}
	e.logger.Info("Closing WAL...")

	closeErr := e.wal.Close()

	if closeErr != nil {
		e.logger.Error("Error closing WAL.", "error", closeErr)
	}

	e.wal = nil
	return closeErr
}

func (e *storageEngine) closeSSTables() error {
	if e.levelsManager == nil {
		return nil
	}
	e.logger.Info("Closing all SSTables managed by LevelsManager...")
	err := e.levelsManager.Close()
	if err != nil {
		e.logger.Error("Error closing SSTables via LevelsManager.", "error", err)
	}
	e.levelsManager = nil
	return err
}

func (e *storageEngine) shutdownTracer() error {
	if e.tracerProvider != nil {
		if tp, ok := e.tracerProvider.(interface{ Shutdown(context.Context) error }); ok {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := tp.Shutdown(shutdownCtx); err != nil {
				e.logger.Error("Error shutting down tracer provider.", "error", err)
				return err
			}
			e.logger.Info("Tracer provider shut down.")
		}
		e.tracerProvider = nil
		e.tracer = nil
	}
	return nil
}

func (e *storageEngine) VerifyDataConsistency() []error {
	if err := e.CheckStarted(); err != nil {
		return []error{err}
	}
	var allErrors []error
	if e.levelsManager != nil {
		if lmErrors := e.levelsManager.VerifyConsistency(); len(lmErrors) > 0 {
			allErrors = append(allErrors, lmErrors...)
		}
	}
	return allErrors
}

func (e *storageEngine) GetDataDir() string {
	if e == nil || e.opts.DataDir == "" {
		return ""
	}
	return e.opts.DataDir
}

func (e *storageEngine) GetHookManager() hooks.HookManager {
	return e.hookManager
}

func (e *storageEngine) GetPubSub() (PubSubInterface, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	return e.pubsub, nil
}

func (e *storageEngine) TriggerCompaction() {
	if e.compactor != nil {
		e.compactor.Trigger()
	}
}

func (e *storageEngine) Metrics() (*EngineMetrics, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	return e.metrics, nil
}

func (e *storageEngine) GetFileManage() internalFileManage {
	return e.internalFile
}

func (e *storageEngine) GetSnapshotManager() snapshot.ManagerInterface {
	return e.snapshotManager
}
