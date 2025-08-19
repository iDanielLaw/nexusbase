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
	"runtime"
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
	// ErrEngineClosed is returned when an operation is attempted on a closed or not-yet-started engine.
	ErrEngineClosed = errors.New("engine is closed or not started")
	// ErrEngineAlreadyStarted is returned when Start() is called on an already running engine.
	ErrEngineAlreadyStarted = errors.New("engine is already started")
	ErrFlushInProgress      = errors.New("engine is inprogress flush")
)

// RoundingRule defines a rule for rounding relative query time ranges for caching.
type RoundingRule struct {
	// If the query duration is less than or equal to this threshold, the rule applies.
	QueryDurationThreshold time.Duration
	RoundingDuration       time.Duration
}

// StorageEngineOptions holds configuration for the StorageEngine.
type StorageEngineOptions struct {
	DataDir                        string
	MemtableThreshold              int64
	MemtableFlushIntervalMs        int // New: Interval for periodic memtable flushes (in milliseconds)
	BlockCacheCapacity             int
	L0CompactionTriggerSize        int64 // New: Size-based trigger for L0 compaction
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
	CheckpointIntervalSeconds      int // New: Interval for periodic durable checkpoints
	TracerProvider                 trace.TracerProvider
	TestingOnlyFailFlushCount      *atomic.Int32 // For testing: number of times flush should fail before succeeding.
	ErrorOnSSTableLoadFailure      bool
	SSTableCompressor              core.Compressor
	TestingOnlyInjectWALCloseError error // For testing purposes, allows injection of errors during WAL close or recover
	WALSyncMode                    core.WALSyncMode
	WALBatchSize                   int
	WALFlushIntervalMs             int
	WALPurgeKeepSegments           int // New: Number of WAL segments to keep behind the last checkpointed one.
	WALMaxSegmentSize              int64
	RetentionPeriod                string
	MetadataSyncIntervalSeconds    int  // Interval for periodic metadata persistence
	EnableTagBloomFilter           bool // Enable bloom filter for tag values
	IndexMemtableThreshold         int64
	IndexFlushIntervalMs           int
	IndexCompactionIntervalSeconds int
	IndexMaxL0Files                int
	IndexMaxLevels                 int
	IndexBaseTargetSize            int64
	CompactionTombstoneWeight      float64 // New: Weight for tombstone score
	CompactionOverlapWeight        float64 // New: Weight for overlap penalty
	Logger                         *slog.Logger
	Clock                          clock.Clock // Clock interface for testing, defaults to SystemClock

	SelfMonitoringEnabled    bool
	SelfMonitoringPrefix     string
	SelfMonitoringIntervalMs int
	// Rules for rounding relative query time ranges for caching. Must be sorted by QueryDurationThreshold.
	RelativeQueryRoundingRules []RoundingRule
	CompactionFallbackStrategy levels.CompactionFallbackStrategy
	ReplicationSyncTimeoutMs   int // New: Timeout for synchronous replication waits.

	// New fields for Intra-L0 Compaction
	IntraL0CompactionTriggerFiles     int   // Number of small files in L0 to trigger an intra-L0 compaction.
	IntraL0CompactionMaxFileSizeBytes int64 // Max size of a file to be considered for intra-L0 compaction.
}

// DefaultStorageEngineOptions returns a new StorageEngineOptions struct with sensible default values.
func DefaultStorageEngineOptions() StorageEngineOptions {
	return StorageEngineOptions{
		DataDir:                        "./data",
		MemtableThreshold:              64 * 1024 * 1024, // 64MB
		MemtableFlushIntervalMs:        1000,             // 1s
		BlockCacheCapacity:             10000,
		L0CompactionTriggerSize:        0, // Disabled by default, MaxL0Files is primary trigger
		MaxL0Files:                     4,
		CompactionIntervalSeconds:      60,
		TargetSSTableSize:              128 * 1024 * 1024, // 128MB
		LevelsTargetSizeMultiplier:     10,
		MaxLevels:                      7,
		MaxConcurrentLNCompactions:     runtime.NumCPU(),
		BloomFilterFalsePositiveRate:   0.01,
		SSTableDefaultBlockSize:        sstable.DefaultBlockSize,
		CheckpointIntervalSeconds:      300, // 5 minutes
		ErrorOnSSTableLoadFailure:      false,
		SSTableCompressor:              &compressors.SnappyCompressor{},
		WALSyncMode:                    core.WALSyncInterval,
		WALBatchSize:                   1024,
		WALFlushIntervalMs:             1000, // 1s
		WALPurgeKeepSegments:           3,
		WALMaxSegmentSize:              core.WALMaxSegmentSize, // 128MB
		RetentionPeriod:                "",                     // Disabled by default
		MetadataSyncIntervalSeconds:    60,
		EnableTagBloomFilter:           true,
		IndexMemtableThreshold:         16 * 1024 * 1024, // 16MB
		IndexFlushIntervalMs:           1000,             // 1s
		IndexCompactionIntervalSeconds: 60,
		IndexMaxL0Files:                4,
		IndexMaxLevels:                 7,
		IndexBaseTargetSize:            16 * 1024 * 1024, // 16MB
		CompactionTombstoneWeight:      1.5,
		CompactionOverlapWeight:        1.0,
		Logger:                         slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})),
		Clock:                          clock.SystemClockDefault,
		SelfMonitoringEnabled:          true,
		SelfMonitoringPrefix:           "__nexus_internal_",
		SelfMonitoringIntervalMs:       15000, // 15s
		IntraL0CompactionTriggerFiles:     4,
		ReplicationSyncTimeoutMs:       0, // Default to async
		IntraL0CompactionMaxFileSizeBytes: 16 * 1024 * 1024, // 16MB
	}
}

// storageEngine is the main struct that manages the LSM-tree components.
type storageEngine struct {
	opts StorageEngineOptions
	mu   sync.RWMutex

	nextSSTableID   atomic.Uint64
	validator       *core.Validator
	isStarted       atomic.Bool // Tracks if the engine has been started
	isClosing       atomic.Bool // Prevents concurrent Close calls
	sequenceNumber  atomic.Uint64
	stateLoader     *StateLoader    // New: Handles loading state from disk
	serviceManager  *ServiceManager // New: Manages background services
	mutableMemtable *memtable.Memtable
	// For testing override, bool indicates if checkpoint should be written
	processImmutableMemtablesFunc func(writeCheckpoint bool)
	triggerPeriodicFlushFunc      func()
	syncMetadataFunc              func()
	immutableMemtables            []*memtable.Memtable

	wal           wal.WALInterface
	levelsManager levels.Manager
	blockCache    cache.Interface // Shared cache for data blocks
	compactor     CompactionManagerInterface

	flushChan      chan struct{}
	forceFlushChan chan chan error // New channel for synchronous flush requests
	shutdownChan   chan struct{}
	wg             sync.WaitGroup
	logger         *slog.Logger

	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	sstDir           string
	dlqDir           string
	snapshotsBaseDir string
	seriesLogFile    sys.FileInterface // New: File handle for the series log
	seriesLogMu      sync.Mutex        // New: Mutex to protect writes to the series log
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
	bitmapCache     cache.Interface // Cache for Roaring Bitmaps
	pubsub          PubSubInterface // For real-time subscriptions
	metrics         *EngineMetrics
	hookManager     hooks.HookManager // NEW: Hook manager

	internalFile internalFileManage
	replicationTracker *core.ReplicationTracker

	clock clock.Clock

	// test internal only
	setCompactorFactory func(StorageEngineOptions, *storageEngine) (CompactionManagerInterface, error)
	putBatchInterceptor func(ctx context.Context, points []core.DataPoint) error
}

var _ StorageEngineInterface = (*storageEngine)(nil)

// NewStorageEngine initializes and returns a new StorageEngine.
func NewStorageEngine(opts StorageEngineOptions) (engine StorageEngineInterface, err error) {
	concreteEngine, err := initializeStorageEngine(opts)
	return concreteEngine, err
}

func initializeStorageEngine(opts StorageEngineOptions) (engine *storageEngine, err error) {
	// Defer the cleanup function. It will check if `err` is non-nil upon returning from this function.
	// This ensures that if any part of the initialization fails, all successfully initialized
	// components are cleaned up properly.
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
		opts.BloomFilterFalsePositiveRate = 0.01 // Set Default
	}

	concreteEngine := &storageEngine{
		validator:          core.NewValidator(),
		opts:               opts,
		immutableMemtables: make([]*memtable.Memtable, 0), // Corrected initialization
		flushChan:          make(chan struct{}, 1),        // For async flushes from Put
		forceFlushChan:     make(chan chan error),         // For sync flushes, unbuffered
		shutdownChan:       make(chan struct{}),
		engineStartTime:    clk.Now(),
		activeSeries:       make(map[string]struct{}),
		deletedSeries:      make(map[string]uint64),
		pubsub:             NewPubSub(), // Initialize PubSub
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

	// Initialize bitmap cache and stores before other components
	concreteEngine.hookManager = hooks.NewHookManager(logger.With("component", "HookManager"))
	concreteEngine.stringStore = indexer.NewStringStore(logger.With("sub_component", "StringStore"), concreteEngine.hookManager)
	concreteEngine.bitmapCache = cache.NewLRUCache(opts.BlockCacheCapacity, nil, nil, nil)
	concreteEngine.seriesIDStore = indexer.NewSeriesIDStore(concreteEngine.logger.With("sub_component", "SeriesIDStore"), concreteEngine.hookManager)

	if concreteEngine.opts.SSTableCompressor == nil {
		concreteEngine.opts.SSTableCompressor = &compressors.NoCompressionCompressor{}
	}

	// Create the state loader and service manager AFTER all options have been defaulted.
	concreteEngine.stateLoader = NewStateLoader(concreteEngine)
	concreteEngine.serviceManager = NewServiceManager(concreteEngine)

	// Set default implementations for function fields, allowing tests to override them.
	concreteEngine.processImmutableMemtablesFunc = concreteEngine.processImmutableMemtables
	concreteEngine.triggerPeriodicFlushFunc = concreteEngine.triggerPeriodicFlush
	concreteEngine.syncMetadataFunc = concreteEngine.syncMetadata

	concreteEngine.sstDir = filepath.Join(opts.DataDir, "sst")
	concreteEngine.dlqDir = filepath.Join(opts.DataDir, "dlq")
	concreteEngine.snapshotsBaseDir = filepath.Join(opts.DataDir, "snapshots")
	// Initialize directories first, as other components depend on them.
	if err = concreteEngine.initializeDirectories(); err != nil {
		err = fmt.Errorf("failed to initialize directories: %w", err)
		engine = concreteEngine // Assign to return var for cleanup
		return
	}

	// Initialize the snapshot manager, passing the engine itself as the provider.
	concreteEngine.snapshotManager = snapshot.NewManager(concreteEngine)

	return concreteEngine, nil // Return the concrete type which satisfies the interface
}

// Start starts the background processes of the storage engine
func (e *storageEngine) Start() error {
	// --- Pre-Start Hook ---
	// Runs before the Engine starts loading files and various components.
	if err := e.hookManager.Trigger(context.Background(), hooks.NewPreStartEngineEvent()); err != nil {
		return fmt.Errorf("engine start cancelled by pre-hook: %w", err)
	}

	// Use CompareAndSwap to ensure Start is only executed once while the engine is not running.
	if !e.isStarted.CompareAndSwap(false, true) {
		return ErrEngineAlreadyStarted
	}
	// Reset the closing flag in case a previous Close() failed partway.
	e.isClosing.Store(false)

	// Initialize mutable memtable after series ID store is ready
	testFilePath := filepath.Join(e.opts.DataDir, ".writable_test") // Check writability early
	if testFile, testErr := os.Create(testFilePath); testErr != nil {
		e.logger.Error("Data directory is not writable.", "path", e.opts.DataDir, "error", testErr)
		return fmt.Errorf("data directory %s is not writable: %w", e.opts.DataDir, testErr)
	} else {
		_ = testFile.Close()
		_ = os.Remove(testFilePath)
	}

	// --- Initialize components that were previously in initializeStorageEngine ---
	e.initializeMetrics()
	if err := e.initializeLSMTreeComponents(); err != nil {
		e.logger.Error("NewStorageEngine failed during initializeLSMTreeComponents.", "error", err)
		return fmt.Errorf("failed to initialize LSM tree components: %w", err)
	}
	if err := e.initializeTagIndexManager(); err != nil {
		return fmt.Errorf("failed to initialize tag index manager: %w", err)
	}
	// --- End of moved block ---

	// --- Phase 2: Load all state from disk using the StateLoader ---
	if err := e.stateLoader.Load(); err != nil {
		e.logger.Error("Failed to load engine state.", "error", err)
		return err // The loader will have logged more specific errors.
	}

	// --- Phase 3: Open series log for appending ---
	seriesLogPath := filepath.Join(e.opts.DataDir, "series.log")
	seriesFile, err := e.internalFile.OpenFile(seriesLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open series log file for writing: %w", err)
	}
	e.seriesLogFile = seriesFile
	e.serviceManager.Start()
	e.logger.Info("StorageEngine background services started.", "data_dir", e.opts.DataDir)

	// --- Post-Start Hook ---
	// ทำงานหลังจากที่ Engine พร้อมใช้งานแล้ว
	e.hookManager.Trigger(context.Background(), hooks.NewPostStartEngineEvent())

	return nil
}

func (e *storageEngine) GetNextSSTableID() uint64 {
	if err := e.CheckStarted(); err != nil {
		// This indicates a severe logic error in the engine's lifecycle management.
		// A component is trying to get a new file ID when the engine is not running.
		// This should not happen in a correctly functioning system. Panicking makes
		// this unrecoverable error loud and clear, preventing silent data corruption.
		panic(fmt.Errorf("GetNextSSTableID called on a non-running engine: %w", err))
	}
	// เพิ่มค่าและคืนค่าใหม่แบบ Atomic เพื่อให้แน่ใจว่าได้ ID ที่ไม่ซ้ำกันเสมอ
	// ในการเริ่มต้นเอนจิน ควรจะเริ่มต้น nextSSTableID ด้วยค่าที่มากกว่า ID สูงสุดของ SSTable ที่มีอยู่แล้ว
	// แต่สำหรับตอนนี้ การเพิ่มจาก 0 หรือ 1 ก็จะช่วยแก้ปัญหาการชนกันในกรณีทดสอบได้
	return e.nextSSTableID.Add(1) // This is now safe due to the checkStarted() call
}

// initializeDirectories creates the necessary data and DLQ directories.
func (e *storageEngine) initializeDirectories() error {
	if e.opts.DataDir == "" {
		return fmt.Errorf("data directory must be specified")
	}

	// Explicitly check if the path exists and is a file (not a directory)
	info, err := os.Stat(e.opts.DataDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("data directory %s exists but is not a directory", e.opts.DataDir)
		}
	} else if !os.IsNotExist(err) {
		// Some other error occurred while stat-ing the path
		return fmt.Errorf("failed to stat data directory %s: %w", e.opts.DataDir, err)
	}
	if err := os.MkdirAll(e.opts.DataDir, 0755); err != nil { // Create base data directory
		e.logger.Error("failed to create data directory", "path", e.opts.DataDir, "error", err)
		return fmt.Errorf("failed to create data directory %s: %w", e.opts.DataDir, err)
	}
	if err := os.MkdirAll(e.sstDir, 0755); err != nil { // Create sst subdirectory
		e.logger.Error("failed to create sst directory", "path", e.sstDir, "error", err)
		return fmt.Errorf("failed to create sst directory %s: %w", e.sstDir, err)
	}
	if err := os.MkdirAll(e.dlqDir, 0755); err != nil { // Create DLQ subdirectory
		e.logger.Error("failed to create DLQ directory", "path", e.dlqDir, "error", err)
		return fmt.Errorf("failed to create DLQ directory %s: %w", e.dlqDir, err)
	}
	if err := os.MkdirAll(e.snapshotsBaseDir, 0755); err != nil { // Create snapshots subdirectory
		e.logger.Error("failed to create snapshots base directory", "path", e.snapshotsBaseDir, "error", err)
		return fmt.Errorf("failed to create snapshots base directory %s: %w", e.snapshotsBaseDir, err)
	}
	return nil
}

// CleanupEngine safely closes and releases resources held by the StorageEngine.
// It's intended to be called in error paths of NewStorageEngine to prevent resource leaks
// if initialization fails partway through.
func (e *storageEngine) CleanupEngine() {
	e.logger.Info("Cleaning up engine resources after initialization failure...")

	// Stop compactor if it was started (it handles its own wg)
	if e.compactor != nil {
		e.compactor.Stop()
	}
	// Stop the tag index manager
	if e.tagIndexManager != nil {
		e.tagIndexManager.Stop()
	}
	// Close the series ID store to release its file handle.
	if e.seriesIDStore != nil {
		e.seriesIDStore.Close()
	}
	if e.stringStore != nil {
		e.stringStore.Close()
	}
	// Close other components that might have been opened
	e.closeWAL()
	e.closeSSTables()
	e.shutdownTracer()
}

// Close gracefully shuts down the StorageEngine.
func (e *storageEngine) Close() error {
	// Check if started and not already closing.
	if !e.isStarted.Load() {
		e.logger.Info("Close called on a non-running or already closed engine.")
		return nil
	}
	// --- Pre-Close Hook ---
	// ทำงานก่อนที่ Engine จะเริ่มกระบวนการปิดตัว
	if err := e.hookManager.Trigger(context.Background(), hooks.NewPreCloseEngineEvent()); err != nil {
		return fmt.Errorf("engine close cancelled by pre-hook: %w", err)
	}

	// Atomically set the isClosing flag to prevent concurrent Close calls.
	if !e.isClosing.CompareAndSwap(false, true) {
		e.logger.Info("Close operation already in progress.")
		return nil // Another goroutine is already closing.
	}

	// 1. Stop all background services and wait for them to finish.
	if e.serviceManager != nil {
		e.serviceManager.Stop()
	}

	// Stop the hook manager and wait for async listeners
	if e.hookManager != nil {
		e.hookManager.Stop()
	}

	// 2. Now that all background activity is stopped, flush any remaining memtables.
	var closeErr error
	closeErr = errors.Join(closeErr, e.flushRemainingMemtables())
	closeErr = errors.Join(closeErr, e.seriesIDStore.Close())
	// NEW: Write final checkpoint after all data is flushed and before WAL is closed.
	if e.wal != nil {
		lastFlushedSegment := e.wal.ActiveSegmentIndex()
		if lastFlushedSegment > 0 {
			cp := core.Checkpoint{LastSafeSegmentIndex: lastFlushedSegment}
			if writeErr := checkpoint.Write(e.opts.DataDir, cp); writeErr != nil {
				e.logger.Error("Failed to write final checkpoint during close.", "error", writeErr)
				closeErr = errors.Join(closeErr, writeErr)
			} else {
				// Purge after successful checkpoint, respecting safety margin
				e.purgeWALSegments(lastFlushedSegment)
			}
		}
	}
	// Close the series log file
	if e.seriesLogFile != nil {
		e.seriesLogFile.Close()
		e.seriesLogFile = nil
	}
	closeErr = errors.Join(closeErr, e.closeWAL())
	closeErr = errors.Join(closeErr, e.closeSSTables())
	closeErr = errors.Join(closeErr, e.shutdownTracer())

	if closeErr != nil {
		// The error from errors.Join is already well-formatted.
		// We wrap it for additional context.
		return fmt.Errorf("errors during close: %w", closeErr)
	}

	// Finally, mark the engine as fully stopped.
	e.isStarted.Store(false)
	// --- Post-Close Hook ---
	// ทำงานหลังจากที่ Engine ปิดตัวลงอย่างสมบูรณ์
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

// wipeDataDirectory removes all engine-managed files and subdirectories from the data directory.
// This is a helper function for the restore process.
func (e *storageEngine) wipeDataDirectory() error {
	// List of directories and files to remove.
	// We don't remove the top-level data directory itself, just its contents.
	itemsToRemove := []string{
		e.sstDir,
		e.dlqDir,
		filepath.Join(e.opts.DataDir, "wal"),
		filepath.Join(e.opts.DataDir, core.CurrentFileName),
		filepath.Join(e.opts.DataDir, core.CheckpointFileName),
		filepath.Join(e.opts.DataDir, "string_mapping.log"),
		filepath.Join(e.opts.DataDir, "series_mapping.log"),
		filepath.Join(e.opts.DataDir, "series.log"),
		filepath.Join(e.opts.DataDir, "deleted_series.json"),
		filepath.Join(e.opts.DataDir, "range_tombstones.json"),
		filepath.Join(e.opts.DataDir, "tag_index"), // The directory for the tag index
	}

	// Also remove any MANIFEST files.
	files, err := filepath.Glob(filepath.Join(e.opts.DataDir, "MANIFEST*"))
	if err != nil {
		return fmt.Errorf("failed to glob for manifest files to wipe: %w", err)
	}
	itemsToRemove = append(itemsToRemove, files...)

	var firstErr error
	for _, itemPath := range itemsToRemove {
		if err := os.RemoveAll(itemPath); err != nil {
			// Log the error but continue trying to remove other items.
			e.logger.Warn("Failed to remove item during data wipe.", "path", itemPath, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Re-create the essential directories after wiping.
	if err := e.initializeDirectories(); err != nil {
		return fmt.Errorf("failed to re-initialize directories after wipe: %w", err)
	}

	return firstErr
}

// CopyFile copies a file from src to dst.
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

// initializeMetrics sets up the engine's metrics instance.
func (e *storageEngine) initializeMetrics() {
	if e.metrics == nil {
		e.metrics = NewEngineMetrics(true, "engine_")

		histMapsToInit := []*expvar.Map{
			e.metrics.PutLatencyHist, e.metrics.GetLatencyHist, e.metrics.DeleteLatencyHist,
			e.metrics.RangeScanLatencyHist, e.metrics.AggregationQueryLatencyHist,
		}
		for _, m := range histMapsToInit {
			m.Set("count", new(expvar.Int))
			m.Set("sum", new(expvar.Float))
			for _, b := range latencyBuckets {
				m.Set(fmt.Sprintf("le_%.3f", b), new(expvar.Int))
			}
			m.Set("le_inf", new(expvar.Int))
		}
	}

	// The `expvar.Func` metrics need to be published regardless of whether
	// the main metrics struct was injected or created here.
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

	// --- Pool Metrics (as individual funcs for live data) ---
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

	// --- Memtable Pool Metrics (as individual funcs) ---
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

	e.metrics.activeSeriesCountFunc = func() interface{} {
		e.activeSeriesMu.RLock()
		defer e.activeSeriesMu.RUnlock()
		return len(e.activeSeries)
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

// initializeLSMTreeComponents sets up memtables, block cache, levels manager, and compaction manager.
func (e *storageEngine) initializeLSMTreeComponents() error {
	e.mutableMemtable = memtable.NewMemtable(e.opts.MemtableThreshold, e.clock)
	// The cache will store *bytes.Buffer objects from a pool.
	// We provide callbacks to return buffers to the pool and to trigger hooks.
	onEvictedWithHook := func(key string, value interface{}) {
		// Return buffer to pool
		if buf, ok := value.(*bytes.Buffer); ok {
			core.BufferPool.Put(buf)
		}
		// Trigger eviction hook
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
	// Wire up the engine's metrics to the cache instance.
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
			Engine:  e, // Pass the engine instance itself
			DataDir: e.sstDir,
			Opts: CompactionOptions{
				MaxL0Files:                 e.opts.MaxL0Files,
				L0CompactionTriggerSize:    e.opts.L0CompactionTriggerSize, // Pass the new option
				TargetSSTableSize:          e.opts.TargetSSTableSize,
				LevelsTargetSizeMultiplier: e.opts.LevelsTargetSizeMultiplier,
				CompactionIntervalSeconds:  e.opts.CompactionIntervalSeconds,
				MaxConcurrentLNCompactions: e.opts.MaxConcurrentLNCompactions, // Default value, can be made configurable
				SSTableCompressor:          e.opts.SSTableCompressor,
				RetentionPeriod:            e.opts.RetentionPeriod,

				// Pass Intra-L0 compaction options to the compactor
				IntraL0CompactionTriggerFiles:     e.opts.IntraL0CompactionTriggerFiles,
				IntraL0CompactionMaxFileSizeBytes: e.opts.IntraL0CompactionMaxFileSizeBytes,
			},
			LevelsManager:        lm,
			Logger:               e.logger,
			Tracer:               e.tracer,
			IsSeriesDeleted:      e.isSeriesDeleted,
			IsRangeDeleted:       e.isCoveredByRangeTombstone,
			ExtractSeriesKeyFunc: func(key []byte) ([]byte, error) { return key[:len(key)-8], nil },
			Metrics:              e.metrics,    // Pass the engine's metrics instance
			BlockCache:           e.blockCache, // Pass the block cache
			FileRemover:          nil,          // Use default real file remover
			SSTableWriterFactory: func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) { // This is the default factory that creates real SSTableWriters.
				// Tests can override this in their CompactionManagerParams.
				return sstable.NewSSTableWriter(opts)
			},
			ShutdownChan: e.shutdownChan, // Pass the engine's shutdown channel
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

// GetWALPath returns the file path of the WAL.
// This is primarily for testing purposes.
func (e *storageEngine) GetWALPath() string {
	if err := e.CheckStarted(); err != nil {
		return ""
	}
	if e.wal == nil {
		return ""
	}
	return e.wal.Path()
}

// GetWAL return the wal object.
func (e *storageEngine) GetWAL() wal.WALInterface {
	return e.wal
}

// GetSnapshotsBaseDir returns the base directory path for storing snapshots.
func (e *storageEngine) GetSnapshotsBaseDir() string {
	return e.snapshotsBaseDir
}

// GetDLQDir returns the directory path for the Dead Letter Queue.
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

// realFileRemover is a concrete implementation of FileRemover using os.Remove.
type realFileRemover struct{}

func (r *realFileRemover) Remove(name string) error { return os.Remove(name) }

// closeWAL closes the Write-Ahead Log file. This is a helper for Close() and CleanupEngine().
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

// closeSSTables closes all SSTable files managed by the LevelsManager. This is a helper for Close() and CleanupEngine().
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

// shutdownTracer shuts down the OpenTelemetry tracer provider if it supports it. This is a helper for Close() and CleanupEngine().
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

// GetDataDir returns the data directory path used by the StorageEngine.
func (e *storageEngine) GetDataDir() string {
	if e == nil || e.opts.DataDir == "" {
		return ""
	}
	return e.opts.DataDir
}

func (e *storageEngine) GetHookManager() hooks.HookManager {
	return e.hookManager
}

// GetPubSub returns the PubSub instance for the engine.
func (e *storageEngine) GetPubSub() (PubSubInterface, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	return e.pubsub, nil
}

// TriggerCompaction manually signals the compaction manager to check for and
// potentially start a compaction cycle.
func (e *storageEngine) TriggerCompaction() {
	if e.compactor != nil {
		e.compactor.Trigger()
	}
}

// Metrics returns the Metrics instance for the engine.
func (e *storageEngine) Metrics() (*EngineMetrics, error) {
	if err := e.CheckStarted(); err != nil {
		return nil, err
	}
	return e.metrics, nil
}

func (e *storageEngine) GetFileManage() internalFileManage {
	return e.internalFile
}

// ApplyReplicatedEntry applies a WAL entry received from a leader.
// This method is central to the follower's logic. It bypasses writing to its own WAL
// and directly applies the change to the memtable using the sequence number from the leader.
func (e *storageEngine) ApplyReplicatedEntry(ctx context.Context, entry *core.WALEntry) error {
	if err := e.CheckStarted(); err != nil {
		return err
	}

	// Ensure we don't apply old entries. This is an important idempotency check.
	if entry.SeqNum <= e.sequenceNumber.Load() {
		e.logger.Debug("Skipping already applied WAL entry", "entry_seq_num", entry.SeqNum, "engine_seq_num", e.sequenceNumber.Load())
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	switch entry.EntryType {
	case core.EntryTypePutEvent:
		// The key and value are already encoded from the leader.
		// We just need to put them into the memtable with the leader's sequence number.
		if err := e.mutableMemtable.Put(entry.Key, entry.Value, entry.EntryType, entry.SeqNum); err != nil {
			return fmt.Errorf("failed to apply replicated PUT to memtable: %w", err)
		}

	case core.EntryTypeDelete:
		// Apply the point tombstone to the memtable.
		if err := e.mutableMemtable.Put(entry.Key, nil, entry.EntryType, entry.SeqNum); err != nil {
			return fmt.Errorf("failed to apply replicated DELETE to memtable: %w", err)
		}

	case core.EntryTypeDeleteSeries:
		// Apply the series tombstone.
		seriesKeyStr := string(entry.Key)
		e.deletedSeriesMu.Lock()
		e.deletedSeries[seriesKeyStr] = entry.SeqNum
		e.deletedSeriesMu.Unlock()

		// Remove from active series tracking and tag index.
		seriesID, found := e.seriesIDStore.GetID(seriesKeyStr)
		if found {
			e.tagIndexManager.RemoveSeries(seriesID)
			e.removeActiveSeries(seriesKeyStr)
		}

	case core.EntryTypeDeleteRange:
		// Apply the range tombstone.
		minTs, maxTs, err := core.DecodeRangeTombstoneValue(entry.Value)
		if err != nil {
			return fmt.Errorf("failed to decode replicated range tombstone: %w", err)
		}
		e.rangeTombstonesMu.Lock()
		e.rangeTombstones[string(entry.Key)] = append(e.rangeTombstones[string(entry.Key)], core.RangeTombstone{
			MinTimestamp: minTs,
			MaxTimestamp: maxTs,
			SeqNum:       entry.SeqNum,
		})
		e.rangeTombstonesMu.Unlock()

	default:
		return fmt.Errorf("unknown replicated WAL entry type: %v", entry.EntryType)
	}

	// CRITICAL: Update the engine's sequence number to the one from the leader.
	e.sequenceNumber.Store(entry.SeqNum)

	return nil
}

// SetSequenceNumber forces the engine's sequence number to a specific value.
func (e *storageEngine) SetSequenceNumber(seqNum uint64) {
	e.sequenceNumber.Store(seqNum)
	e.logger.Info("Engine sequence number force-set", "new_seq_num", seqNum)
}

// GetSequenceNumber returns the current sequence number of the engine.
func (e *storageEngine) GetSequenceNumber() uint64 {
	return e.sequenceNumber.Load()
}
