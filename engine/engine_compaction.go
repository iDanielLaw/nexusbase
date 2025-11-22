package engine

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/INLOpen/nexusbase/cache"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/iterator"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/INLOpen/nexuscore/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type CompactionManagerInterface interface {
	Start(wg *sync.WaitGroup)
	SetMetricsCounters(
		compactionCount *expvar.Int, compactionLatencyHist *expvar.Map,
		dataReadBytes *expvar.Int,
		dataWrittenBytes *expvar.Int,
		tablesMerged *expvar.Int,
	)
	Stop()
	Trigger()
}

// CompactionManager is responsible for managing and executing compaction tasks.
// Corresponds to FR5.1.
type CompactionManager struct {
	Engine        StorageEngineInterface
	levelsManager levels.Manager
	dataDir       string
	opts          CompactionOptions

	compactionChan        chan struct{}
	shutdownChan          chan struct{}
	compactionWg          sync.WaitGroup // Add WaitGroup for active compaction tasks
	l0CompactionActive    atomic.Bool    // Tracks if an L0 compaction is currently active
	lnCompactionSemaphore chan struct{}  // Limits concurrent LN compactions

	logger *slog.Logger // Structured logger
	// Metrics
	compactionCount                   *expvar.Int     // Total compactions
	compactionLatencyHist             *expvar.Map     // Histogram for compaction durations
	metricsCompactionDataReadBytes    *expvar.Int     // Bytes read during compaction
	metricsCompactionDataWrittenBytes *expvar.Int     // Bytes written during compaction
	metricsCompactionTablesMerged     *expvar.Int     // Number of tables merged in a compaction
	blockCache                        cache.Interface // Reference to the shared block cache

	// Dependency injection for file removal (for testing)
	fileRemover core.FileRemover // Interface for file removal

	// Dependency injection for SSTableWriter creation (for testing)
	sstableWriterFactory core.SSTableWriterFactory
	tracer               trace.Tracer // For creating spans
	// Functions passed from StorageEngine for tombstone checking during merge
	isSeriesDeletedChecker      iterator.SeriesDeletedChecker
	isRangeDeletedChecker       iterator.RangeDeletedChecker
	extractSeriesKeyFuncForIter iterator.SeriesKeyExtractorFunc
	sstableCompressor           core.Compressor // Changed to use the interface

	metrics *EngineMetrics
}

var _ CompactionManagerInterface = (*CompactionManager)(nil)

// compactionTask holds all the necessary information to run a single compaction job.
type compactionTask struct {
	sourceLevel    int
	targetLevel    int
	inputTables    []*sstable.SSTable
	isL0Compaction bool // Special handling for L0
	parentSpanCtx  context.Context
}

// runCompactionTask executes a generic compaction task, handling merging, level updates, and cleanup.
func (cm *CompactionManager) runCompactionTask(task *compactionTask) ([]*sstable.SSTable, error) {
	ctx, span := cm.tracer.Start(task.parentSpanCtx, "CompactionManager.runCompactionTask")
	defer span.End()
	span.SetAttributes(
		attribute.Int("compaction.source_level", task.sourceLevel),
		attribute.Int("compaction.target_level", task.targetLevel),
		attribute.Int("compaction.input_tables_count", len(task.inputTables)),
	)

	newTables, err := cm.mergeMultipleSSTables(ctx, task.inputTables, task.targetLevel)
	if err != nil {
		// Handle unrecoverable corruption errors by quarantining input tables.
		if errors.Is(err, sstable.ErrCorrupted) {
			cm.logger.Warn("Unrecoverable corruption during compaction. Quarantining source tables.", "source_level", task.sourceLevel, "error", err)
			cm.quarantineSSTables(ctx, task.inputTables)
			return nil, nil // Handled by quarantine, do not propagate error to stop the compactor loop.
		}
		return nil, err // Return other types of errors.
	}

	// --- Pre-SSTable-Delete Hook ---
	// Trigger this hook BEFORE the level manager state is updated, so we can still determine the level of the old tables.
	for _, oldTable := range task.inputTables {
		level, _ := cm.levelsManager.GetLevelForTable(oldTable.ID())
		preSSTableDeletePayload := hooks.SSTablePayload{
			ID:    oldTable.ID(),
			Level: level,
			Path:  oldTable.FilePath(),
			Size:  oldTable.Size(),
		}
		if hookErr := cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreSSTableDeleteEvent(preSSTableDeletePayload)); hookErr != nil {
			cm.logger.Error("PreSSTableDelete hook failed, aborting compaction task.", "table_id", oldTable.ID(), "error", hookErr, "trace_id", span.SpanContext().TraceID().String())
			return nil, fmt.Errorf("PreSSTableDelete hook failed for table %d: %w", oldTable.ID(), hookErr)
		}
	}

	if err := cm.levelsManager.ApplyCompactionResults(task.sourceLevel, task.targetLevel, newTables, task.inputTables); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_apply_compaction_results")
		return nil, fmt.Errorf("failed to apply compaction results: %w", err)
	}

	if err := cm.removeAndCleanupSSTables(ctx, task.inputTables); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed_to_cleanup_old_tables")
		// Do not return the error, just log it, as the main compaction succeeded.
		// The compactor already logs this error.
	}
	span.SetAttributes(attribute.Int("compaction.output_tables_count", len(newTables)))
	return newTables, nil
}

// compactIntraL0 performs a compaction of small files within Level 0.
func (cm *CompactionManager) compactIntraL0(ctx context.Context) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.compactIntraL0")
	defer span.End()

	// 1. Pick the small L0 files to compact.
	inputTables := cm.levelsManager.PickIntraL0CompactionCandidates(cm.opts.IntraL0CompactionTriggerFiles, cm.opts.IntraL0CompactionMaxFileSizeBytes)
	if len(inputTables) == 0 {
		cm.logger.Info("Intra-L0 compaction was triggered, but no candidate files were picked. Skipping.")
		return nil
	}
	cm.logger.Info("Compacting files within L0.", "input_tables_count", len(inputTables), "input_table_ids", levels.GetTableIDs(inputTables))
	span.SetAttributes(
		attribute.Int("input.l0_tables_count", len(inputTables)),
	)

	// 2. Create the compaction task. Source and Target level are both 0.
	task := &compactionTask{
		sourceLevel:    0,
		targetLevel:    0, // Compacting within L0
		inputTables:    inputTables,
		isL0Compaction: true, // Treat it like an L0 compaction for locking/priority purposes
		parentSpanCtx:  ctx,
	}

	// 3. Run the task (merge, apply results, cleanup)
	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		cm.logger.Error("Intra-L0 compaction failed during merge", "error", err)
		return err
	}

	cm.logger.Info("Intra-L0 compaction completed.", "removed_old_tables_count", len(inputTables), "created_new_tables_count", len(newTables))
	return nil
}

// CompactionOptions holds configuration for the compaction process.
type CompactionOptions struct {
	MaxL0Files                 int   // Trigger for L0->L1 compaction
	L0CompactionTriggerSize    int64 // New: Size-based trigger for L0 compaction
	TargetSSTableSize          int64 // Target size for newly created SSTables
	LevelsTargetSizeMultiplier int   // Multiplier for target size of next level (for LN -> LN+1)
	CompactionIntervalSeconds  int
	MaxConcurrentLNCompactions int // Maximum number of LN->LN+1 compactions to run in parallel
	// New fields
	IntraL0CompactionTriggerFiles     int
	IntraL0CompactionMaxFileSizeBytes int64

	SSTableCompressor core.Compressor // Changed to use the interface
	RetentionPeriod   string          // e.g., "30d", "1y". If empty, no retention.
	// Add other options as needed
}

// CompactionManagerParams is a struct to group parameters for NewCompactionManager.
type CompactionManagerParams struct {
	Engine               StorageEngineInterface
	LevelsManager        levels.Manager
	DataDir              string
	Opts                 CompactionOptions
	Logger               *slog.Logger
	Tracer               trace.Tracer
	IsSeriesDeleted      iterator.SeriesDeletedChecker
	IsRangeDeleted       iterator.RangeDeletedChecker
	ExtractSeriesKeyFunc iterator.SeriesKeyExtractorFunc
	BlockCache           cache.Interface // Add BlockCache to parameters
	Metrics              *EngineMetrics

	// Dependency injection for file removal.
	FileRemover core.FileRemover

	// Factory for creating SSTableWriter instances. Used for dependency injection in tests.
	SSTableWriterFactory core.SSTableWriterFactory
	ShutdownChan         chan struct{} // To signal shutdown from the parent engine
}

func (cm *CompactionManager) compactL0ToL1(ctx context.Context) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.compactL0ToL1")
	defer span.End()

	l0Tables := cm.levelsManager.GetTablesForLevel(0)
	if len(l0Tables) == 0 {
		cm.logger.Info("compactL0ToL1 called, but L0 is empty.")
		return nil
	}

	minL0Key, maxL0Key := getOverallKeyRange(l0Tables)
	l1OverlapTables := cm.levelsManager.GetOverlappingTables(1, minL0Key, maxL0Key)
	cm.logger.Info("Compacting L0->L1.", "l0_tables_count", len(l0Tables), "min_l0_key", string(minL0Key), "max_l0_key", string(maxL0Key), "l1_overlap_tables_count", len(l1OverlapTables))
	span.SetAttributes(
		attribute.Int("input.l0_tables_count", len(l0Tables)),
		attribute.Int("input.l1_overlap_tables_count", len(l1OverlapTables)),
	)

	inputTables := append(l0Tables, l1OverlapTables...)
	if len(inputTables) == 0 {
		cm.logger.Info("No input tables for L0->L1 compaction.")
		return nil
	}

	cm.logger.Debug("Total input tables for L0->L1 compaction.", "count", len(inputTables))
	task := &compactionTask{
		sourceLevel:    0,
		targetLevel:    1,
		inputTables:    inputTables,
		isL0Compaction: true,
		parentSpanCtx:  ctx,
	}

	// --- Pre-Compaction Hook ---
	preCompactionPayload := hooks.PreCompactionPayload{
		SourceLevel: 0,
		TargetLevel: 1,
	}
	cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreCompactionEvent(preCompactionPayload))

	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		// runCompactionTask handles corruption by quarantining and returning nil.
		// Any error returned here is a non-corruption, retryable error.
		cm.logger.Error("L0->L1 compaction failed during merge", "error", err)
		return err
	}

	// --- Post-Compaction Hook ---
	// Trigger a post-compaction hook for L0->L1 for consistency with LN->LN+1.
	newTableInfos := make([]hooks.CompactedTableInfo, len(newTables))
	for i, t := range newTables {
		newTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	oldTableInfos := make([]hooks.CompactedTableInfo, len(inputTables))
	for i, t := range inputTables {
		oldTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	postCompactionPayload := hooks.PostCompactionPayload{
		SourceLevel: 0,
		TargetLevel: 1,
		NewTables:   newTableInfos,
		OldTables:   oldTableInfos,
	}
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostCompactionEvent(postCompactionPayload))
	cm.logger.Info("L0->L1 compaction completed.", "removed_old_tables_count", len(inputTables))
	return nil
}

func (cm *CompactionManager) compactLevelNToLevelNPlus1(ctx context.Context, levelN int) error {
	_, span := cm.tracer.Start(ctx, fmt.Sprintf("CompactionManager.compactL%dToL%d", levelN, levelN+1))
	defer span.End()
	span.SetAttributes(attribute.Int("compaction.source_level", levelN))

	if levelN <= 0 {
		cm.logger.Error("compactLevelNToLevelNPlus1 called with invalid levelN.", "level", levelN)
		span.SetStatus(codes.Error, "invalid_source_level")
		return fmt.Errorf("invalid levelN: %d for LN->LN+1 compaction", levelN)
	}
	cm.logger.Info("Starting LN->LN+1 compaction.", "source_level", levelN, "target_level", levelN+1)

	tableToCompactN := cm.levelsManager.PickCompactionCandidateForLevelN(levelN)
	if tableToCompactN == nil {
		cm.logger.Info("No tables in source level to compact.", "level", levelN)
		span.SetAttributes(attribute.Bool("compaction.performed", false), attribute.String("compaction.skipped_reason", "no_candidate_table"))
		return nil
	}

	minKey, maxKey := tableToCompactN.MinKey(), tableToCompactN.MaxKey()
	tablesToCompactNPlus1 := cm.levelsManager.GetOverlappingTables(levelN+1, minKey, maxKey)
	cm.logger.Info("Compacting LN->LN+1.",
		"source_level", levelN, "source_table_id", tableToCompactN.ID(),
		"source_min_key", string(minKey), "source_max_key", string(maxKey),
		"overlap_target_level_tables_count", len(tablesToCompactNPlus1), "target_level", levelN+1,
	)
	span.SetAttributes(
		attribute.Int64("input.source_table_id", int64(tableToCompactN.ID())),
		attribute.Int("input.overlap_target_tables_count", len(tablesToCompactNPlus1)),
	)

	inputTables := []*sstable.SSTable{tableToCompactN}
	inputTables = append(inputTables, tablesToCompactNPlus1...)

	task := &compactionTask{
		sourceLevel:    levelN,
		targetLevel:    levelN + 1,
		inputTables:    inputTables,
		isL0Compaction: false,
		parentSpanCtx:  ctx,
	}

	// --- Pre-Compaction Hook ---
	preCompactionPayload := hooks.PreCompactionPayload{
		SourceLevel: levelN,
		TargetLevel: levelN + 1,
	}
	cm.Engine.GetHookManager().Trigger(ctx, hooks.NewPreCompactionEvent(preCompactionPayload))

	newTables, err := cm.runCompactionTask(task)
	if err != nil {
		cm.logger.Error("LN->LN+1 compaction failed.", "source_level", levelN, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, fmt.Sprintf("l%d_compaction_failed", levelN))
		return err
	}

	// --- Post-Compaction Hook ---
	// Extract table info before triggering the hook to avoid data races with async listeners.
	newTableInfos := make([]hooks.CompactedTableInfo, len(newTables))
	for i, t := range newTables {
		newTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	oldTableInfos := make([]hooks.CompactedTableInfo, len(inputTables))
	for i, t := range inputTables {
		oldTableInfos[i] = hooks.CompactedTableInfo{ID: t.ID(), Size: t.Size(), Path: t.FilePath()}
	}
	postCompactionPayload := hooks.PostCompactionPayload{
		SourceLevel: levelN,
		TargetLevel: levelN + 1,
		NewTables:   newTableInfos,
		OldTables:   oldTableInfos,
	}
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostCompactionEvent(postCompactionPayload))
	cm.logger.Info("LN->LN+1 compaction completed.", "source_level", levelN, "target_level", levelN+1, "removed_old_tables_count", len(inputTables))
	return nil
}

// NewCompactionManager creates a new CompactionManager.
func NewCompactionManager(
	params CompactionManagerParams,
) (CompactionManagerInterface, error) {
	shutdownChan := params.ShutdownChan
	if shutdownChan == nil {
		// If no channel is provided, create one. This is useful for tests
		// that instantiate CompactionManager directly without an engine.
		shutdownChan = make(chan struct{})
	}
	cm := &CompactionManager{
		Engine:                      params.Engine,
		levelsManager:               params.LevelsManager,
		dataDir:                     params.DataDir,
		opts:                        params.Opts,
		shutdownChan:                shutdownChan, // Use the provided or newly created channel
		compactionChan:              make(chan struct{}, 1),
		logger:                      params.Logger.With("component", "CompactionManager"),
		tracer:                      params.Tracer,
		isSeriesDeletedChecker:      params.IsSeriesDeleted,
		isRangeDeletedChecker:       params.IsRangeDeleted,
		extractSeriesKeyFuncForIter: params.ExtractSeriesKeyFunc,
		sstableCompressor:           params.Opts.SSTableCompressor, // Store it
		blockCache:                  params.BlockCache,             // Store the block cache
		metrics:                     params.Metrics,
	}

	// Set the FileRemover. If not provided, use the default real implementation.
	if params.FileRemover != nil {
		cm.fileRemover = params.FileRemover
	} else {
		cm.fileRemover = &realFileRemover{}
	}

	if params.Opts.MaxConcurrentLNCompactions <= 0 {
		cm.logger.Info("MaxConcurrentLNCompactions not set or invalid, defaulting to number of CPUs.", "provided_value", params.Opts.MaxConcurrentLNCompactions, "default_value", runtime.NumCPU())
		params.Opts.MaxConcurrentLNCompactions = runtime.NumCPU()
	} else if params.Opts.MaxConcurrentLNCompactions > runtime.NumCPU() {
		cm.logger.Warn("MaxConcurrentLNCompactions is set higher than the number of available CPUs. This may not improve performance and could lead to increased resource consumption.", "provided_value", params.Opts.MaxConcurrentLNCompactions, "num_cpu", runtime.NumCPU())
		params.Opts.MaxConcurrentLNCompactions = runtime.NumCPU() // Set to NumCPU if it's higher
	}

	cm.lnCompactionSemaphore = make(chan struct{}, params.Opts.MaxConcurrentLNCompactions)

	// Set the SSTableWriterFactory. If not provided, use the default real implementation.
	if params.SSTableWriterFactory != nil {
		cm.sstableWriterFactory = params.SSTableWriterFactory
	} else {
		cm.sstableWriterFactory = func(opts core.SSTableWriterOptions) (core.SSTableWriterInterface, error) {
			return sstable.NewSSTableWriter(opts)
		}
	}
	return cm, nil
}

// SetMetricsCounters allows the StorageEngine to provide metric counters.
func (cm *CompactionManager) SetMetricsCounters(
	compactionCount *expvar.Int,
	compactionLatencyHist *expvar.Map,
	dataReadBytes *expvar.Int,
	dataWrittenBytes *expvar.Int,
	tablesMerged *expvar.Int,
) {
	cm.compactionCount = compactionCount
	cm.compactionLatencyHist = compactionLatencyHist
	cm.metricsCompactionDataReadBytes = dataReadBytes
	cm.metricsCompactionDataWrittenBytes = dataWrittenBytes
	cm.metricsCompactionTablesMerged = tablesMerged
}

// Start begins the background compaction loop.
func (cm *CompactionManager) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Ensure CompactionIntervalSeconds is positive to avoid panic with NewTicker
		interval := time.Duration(cm.opts.CompactionIntervalSeconds) * time.Second
		if cm.opts.CompactionIntervalSeconds <= 0 {
			cm.logger.Warn("Invalid CompactionIntervalSeconds, defaulting to 60 seconds.", "interval_seconds", cm.opts.CompactionIntervalSeconds, "default_seconds", 60)
			interval = 60 * time.Second // Default to 60 seconds if not set or invalid
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cm.performCompactionCycle()
			case <-cm.compactionChan:
				cm.performCompactionCycle()
			case <-cm.shutdownChan:
				cm.logger.Info("Shutting down compaction loop.")
				return
			}
		}
	}()
	cm.logger.Info("Started background compaction loop.")
}

// Stop signals the compaction loop to shut down and waits for it to complete.
func (cm *CompactionManager) Stop() {
	if cm.shutdownChan != nil {
		// Safely close the channel only if it's not already closed.
		select {
		case <-cm.shutdownChan:
			// Channel already closed or being closed by another goroutine.
		default:
			close(cm.shutdownChan)
		}
		cm.compactionWg.Wait() // Wait for any active compaction goroutines to finish
	}
	cm.logger.Info("Compaction loop stopped.")
}

// Trigger manually signals the compaction loop to perform a check.
func (cm *CompactionManager) Trigger() {
	select {
	case cm.compactionChan <- struct{}{}:
		cm.logger.Info("Manual compaction check triggered.")
	default:
		cm.logger.Info("Compaction check already pending, skipping manual trigger.")
	}
}

// runL0CompactionTask is a helper to wrap the execution of L0-related compactions (Intra-L0, L0->L1).
// It handles common logic like goroutine management, locking, tracing, and metrics.
func (cm *CompactionManager) runL0CompactionTask(parentCtx context.Context, compactionType string, compactionFunc func(context.Context) error) {
	cm.compactionWg.Add(1)
	go func() {
		if cm.metrics != nil && cm.metrics.CompactionsInProgress != nil {
			cm.metrics.CompactionsInProgress.Add(1)
			defer cm.metrics.CompactionsInProgress.Add(-1)
		}
		// Ensure the L0 active flag is cleared before signaling the WaitGroup.
		// Defers run in LIFO order, so add Done first, then Store(false) so
		// Store(false) is executed before Done when the goroutine exits.
		defer cm.compactionWg.Done()
		defer cm.l0CompactionActive.Store(false)

		ctx, span := cm.tracer.Start(parentCtx, fmt.Sprintf("CompactionManager.%sWorker", strings.ReplaceAll(compactionType, "->", "To")))
		defer span.End()
		span.SetAttributes(attribute.String("compaction.type", compactionType))

		clock := cm.Engine.GetClock()
		startTime := clock.Now()

		if err := compactionFunc(ctx); err == nil {
			duration := clock.Now().Sub(startTime).Seconds()
			if cm.compactionLatencyHist != nil {
				observeLatency(cm.compactionLatencyHist, duration)
			}
			span.SetAttributes(attribute.Float64("compaction.duration_seconds", duration))
			cm.logger.Info(fmt.Sprintf("%s compaction finished successfully.", compactionType), "duration_seconds", duration)
			if cm.compactionCount != nil {
				cm.compactionCount.Add(1)
			}
			span.SetAttributes(attribute.Bool("compaction.performed", true))
		} else {
			cm.logger.Error(fmt.Sprintf("%s compaction failed.", compactionType), "error", err)
			span.SetStatus(codes.Error, fmt.Sprintf("%s compaction failed: %v", compactionType, err))
			if cm.Engine != nil {
				if concreteEngine, ok := cm.Engine.(*storageEngine); ok && concreteEngine.metrics.CompactionErrorsTotal != nil {
					concreteEngine.metrics.CompactionErrorsTotal.Add(1)
				}
			}
		}
	}()
}

// performCompactionCycle checks if compaction is needed and runs it.
func (cm *CompactionManager) performCompactionCycle() {
	ctx, span := cm.tracer.Start(context.Background(), "CompactionManager.performCompactionCycle")
	defer span.End()
	cm.logger.Debug("Checking for compaction needs...", "trace_id", span.SpanContext().TraceID().String())
	var compactionInitiatedThisCycle bool
	// NEW: Check for Intra-L0 compaction first. This is a cheaper operation that can reduce L0->L1 pressure.
	if cm.levelsManager.NeedsIntraL0Compaction(cm.opts.IntraL0CompactionTriggerFiles, cm.opts.IntraL0CompactionMaxFileSizeBytes) {
		if cm.l0CompactionActive.CompareAndSwap(false, true) {
			compactionInitiatedThisCycle = true
			cm.logger.Info("Intra-L0 compaction needed, starting task.", "trigger_files", cm.opts.IntraL0CompactionTriggerFiles)
			cm.runL0CompactionTask(ctx, "Intra-L0", cm.compactIntraL0)
		} else {
			cm.logger.Info("Skipping Intra-L0 compaction as another L0 compaction is already active.")
		}
	}

	// Check for L0->L1 compaction only if an Intra-L0 compaction was NOT initiated in this cycle.
	if !compactionInitiatedThisCycle && cm.levelsManager.NeedsL0Compaction(cm.opts.MaxL0Files, cm.opts.L0CompactionTriggerSize) {
		// Try to start an L0 compaction if one isn't already running.
		if cm.l0CompactionActive.CompareAndSwap(false, true) {
			compactionInitiatedThisCycle = true
			cm.logger.Info("L0 compaction needed, starting L0->L1 compaction task.", "max_l0_files", cm.opts.MaxL0Files)
			cm.runL0CompactionTask(ctx, "L0->L1", cm.compactL0ToL1)
		} else {
			cm.logger.Info("Skipping L0 compaction as one is already active.")
			span.SetAttributes(attribute.String("compaction.skipped_reason", "l0_already_active"))
		}
	}

	// Check other levels for compaction needs.
	for levelN := 1; levelN < cm.levelsManager.MaxLevels()-1; levelN++ {
		if cm.levelsManager.NeedsLevelNCompaction(levelN, cm.opts.LevelsTargetSizeMultiplier) {
			// Try to acquire a semaphore slot without blocking.
			select {
			case cm.lnCompactionSemaphore <- struct{}{}:
				// Acquired a slot, start the compaction in a goroutine.
				compactionInitiatedThisCycle = true
				cm.compactionWg.Add(1)
				cm.logger.Info("LN compaction needed, starting task.", "source_level", levelN)

				go func(lvl int, parentCtx context.Context) {
					if cm.metrics != nil && cm.metrics.CompactionsInProgress != nil {
						cm.metrics.CompactionsInProgress.Add(1)
						defer cm.metrics.CompactionsInProgress.Add(-1)
					}
					defer func() {
						<-cm.lnCompactionSemaphore // Release the semaphore slot when done.
						cm.compactionWg.Done()
					}()

					lnCtx, lnSpan := cm.tracer.Start(parentCtx, fmt.Sprintf("CompactionManager.LNCompactionWorker.L%d", lvl))
					defer lnSpan.End()
					lnSpan.SetAttributes(attribute.String("compaction.type", fmt.Sprintf("L%d->L%d", lvl, lvl+1)))

					clock := cm.Engine.GetClock()
					startTime := clock.Now()

					if err := cm.compactLevelNToLevelNPlus1(lnCtx, lvl); err == nil {
						duration := clock.Now().Sub(startTime).Seconds()
						if cm.compactionLatencyHist != nil {
							observeLatency(cm.compactionLatencyHist, duration)
						}
						lnSpan.SetAttributes(attribute.Float64("compaction.duration_seconds", duration))
						cm.logger.Info("LN->LN+1 compaction finished successfully.", "source_level", lvl, "target_level", lvl+1, "duration_seconds", duration)
						if cm.compactionCount != nil {
							cm.compactionCount.Add(1)
						}
						lnSpan.SetAttributes(attribute.Bool("compaction.performed", true))
					} else {
						cm.logger.Error("LN->LN+1 compaction failed.", "source_level", lvl, "target_level", lvl+1, "error", err)
						lnSpan.SetStatus(codes.Error, fmt.Sprintf("L%d->L%d compaction failed: %v", lvl, lvl+1, err))
						if cm.Engine != nil {
							if concreteEngine, ok := cm.Engine.(*storageEngine); ok && concreteEngine.metrics.CompactionErrorsTotal != nil {
								concreteEngine.metrics.CompactionErrorsTotal.Add(1)
							}
						}
					}
				}(levelN, ctx)
			default:
				// Could not acquire a semaphore slot, so we skip this level for now.
				cm.logger.Debug("Skipping LN compaction due to concurrency limit.", "level", levelN, "max_concurrent", cap(cm.lnCompactionSemaphore))
				span.SetAttributes(attribute.String("compaction.skipped_reason", "concurrency_limit"), attribute.Int("compaction.skipped_level", levelN))
			}
		}
	}

	span.SetAttributes(attribute.Bool("compaction.any_initiated", compactionInitiatedThisCycle))
	if !compactionInitiatedThisCycle {
		cm.logger.Debug("No compaction needed or initiated in this cycle.")
	}
}

// removeAndCleanupSSTables closes and removes the physical files for a slice of SSTables.
func (cm *CompactionManager) removeAndCleanupSSTables(ctx context.Context, tables []*sstable.SSTable) error {
	_, span := cm.tracer.Start(ctx, "CompactionManager.removeAndCleanupSSTables")
	defer span.End()
	span.SetAttributes(attribute.Int("tables_to_remove_count", len(tables)))

	var allErrors []error
	for _, oldTable := range tables {
		cm.logger.Info("Deleting old SSTable file after compaction.", "path", oldTable.FilePath(), "id", oldTable.ID())
		if errClose := oldTable.Close(); errClose != nil {
			cm.logger.Error("Error closing old SSTable before deletion.", "path", oldTable.FilePath(), "id", oldTable.ID(), "error", errClose)
			allErrors = append(allErrors, fmt.Errorf("failed to close old SSTable %s: %w", oldTable.FilePath(), errClose))
		}

		if err := cm.fileRemover.Remove(oldTable.FilePath()); err != nil {
			cm.logger.Error("Error deleting old SSTable file.", "path", oldTable.FilePath(), "error", err)
			// Log the error but continue trying to clean up other files.
			allErrors = append(allErrors, fmt.Errorf("failed to delete old SSTable file %s: %w", oldTable.FilePath(), err))
		} else {
			// Successfully removed the file, so increment the metric.
			if cm.metrics != nil && cm.metrics.SSTablesDeletedTotal != nil {
				cm.metrics.SSTablesDeletedTotal.Add(1)
			}
		}
	}
	err := errors.Join(allErrors...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "one_or_more_files_failed_to_cleanup")
	}
	return err
}

// quarantineSSTables moves corrupted or problematic SSTables to a quarantine directory
// and removes them from the level manager's state. This is a recovery mechanism
// to prevent a single bad file from halting all future compactions.
func (cm *CompactionManager) quarantineSSTables(ctx context.Context, tables []*sstable.SSTable) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.quarantineSSTables")
	defer span.End()

	if len(tables) == 0 {
		return
	}

	// Increment the error metric once for the quarantine event.
	if cm.Engine != nil {
		if concreteEngine, ok := cm.Engine.(*storageEngine); ok && concreteEngine.metrics.CompactionErrorsTotal != nil {
			concreteEngine.metrics.CompactionErrorsTotal.Add(1)
		}
	}

	dlqDir := filepath.Join(filepath.Dir(cm.dataDir), "dlq")
	if err := os.MkdirAll(dlqDir, 0755); err != nil {
		cm.logger.Error("Failed to create quarantine directory. Corrupted files will remain in sst dir.", "path", dlqDir, "error", err)
		return
	}

	tableIDs := levels.GetTableIDs(tables)
	cm.logger.Warn("Quarantining SSTables.", "count", len(tables), "ids", tableIDs)

	// Remove from level manager state first.
	// For L0 compaction, tables can be in L0 or L1. We attempt removal from both.
	if err := cm.levelsManager.RemoveTables(0, tableIDs); err != nil {
		cm.logger.Error("Failed to remove quarantined tables from L0 state.", "error", err)
	}
	if err := cm.levelsManager.RemoveTables(1, tableIDs); err != nil {
		cm.logger.Error("Failed to remove quarantined tables from L1 state.", "error", err)
	}

	// Now move the physical files.
	for _, table := range tables {
		table.Close() // Close the file handle before moving.
		destPath := filepath.Join(dlqDir, filepath.Base(table.FilePath()))
		cm.logger.Info("Moving corrupted SSTable to quarantine.", "from", table.FilePath(), "to", destPath)
		if err := sys.Rename(table.FilePath(), destPath); err != nil {
			cm.logger.Error("Failed to move SSTable to quarantine directory.", "path", table.FilePath(), "error", err)
		}
	}
}

// startNewSSTableWriter creates a new SSTable writer and updates the provided fileID.
func (cm *CompactionManager) startNewSSTableWriter(fileID *uint64) (core.SSTableWriterInterface, error) {
	*fileID = cm.Engine.GetNextSSTableID()
	estimatedKeysForNewTable := uint64(cm.opts.TargetSSTableSize / 100)
	if estimatedKeysForNewTable == 0 {
		estimatedKeysForNewTable = 100
	}

	newWriter, writerErr := cm.sstableWriterFactory(core.SSTableWriterOptions{
		DataDir:                      cm.dataDir,
		ID:                           *fileID,
		EstimatedKeys:                estimatedKeysForNewTable,
		BloomFilterFalsePositiveRate: 0.01,
		BlockSize:                    sstable.DefaultBlockSize,
		Tracer:                       cm.tracer,
		Compressor:                   cm.sstableCompressor,
		Logger:                       cm.logger.With("sstable_writer_id", *fileID),
	}) // Use the injected factory
	if writerErr != nil {
		return nil, fmt.Errorf("failed to create new sstable writer: %w", writerErr)
	}
	return newWriter, nil
}

// writeCompactedEntry writes an entry to the current writer. If the writer becomes full,
// it finalizes it, adds it to the list of new tables, and starts a new writer.
// It modifies the state of the compaction process by taking pointers to the state variables.
func (cm *CompactionManager) writeCompactedEntry(
	currentWriter *core.SSTableWriterInterface,
	newSSTables *[]*sstable.SSTable,
	currentFileID *uint64,
	key, value []byte,
	entryType core.EntryType,
	pointID uint64,
) error {
	// Check if a new writer needs to be started because the current one is full.
	// Also, only roll over if the writer is not empty and a valid target size is set.
	if *currentWriter != nil && (*currentWriter).CurrentSize() > 0 && cm.opts.TargetSSTableSize > 0 && (*currentWriter).CurrentSize() >= cm.opts.TargetSSTableSize {
		if err := (*currentWriter).Finish(); err != nil {
			(*currentWriter).Abort()
			*currentWriter = nil
			return fmt.Errorf("failed to finish sstable: %w", err)
		}

		finalPath := (*currentWriter).FilePath()
		sstLoadOpts := sstable.LoadSSTableOptions{FilePath: finalPath, ID: *currentFileID, BlockCache: cm.blockCache, Tracer: cm.tracer, Logger: cm.logger}
		loadedTable, loadErr := sstable.LoadSSTable(sstLoadOpts)
		if loadErr != nil {
			return fmt.Errorf("failed to load newly created sstable %s: %w", finalPath, loadErr)
		}
		if cm.metrics != nil && cm.metrics.SSTablesCreatedTotal != nil {
			cm.metrics.SSTablesCreatedTotal.Add(1)
		}

		*newSSTables = append(*newSSTables, loadedTable)

		// Start a new writer for subsequent entries.
		var writerErr error
		*currentWriter, writerErr = cm.startNewSSTableWriter(currentFileID)
		if writerErr != nil {
			return fmt.Errorf("failed to create new sstable writer during rollover: %w", writerErr)
		}
	}

	// Add the entry to the current writer.
	if err := (*currentWriter).Add(key, value, entryType, pointID); err != nil {
		(*currentWriter).Abort()
		*currentWriter = nil
		return fmt.Errorf("failed to add entry to sstable writer (key: %s): %w", string(key), err)
	}
	return nil
}

func (cm *CompactionManager) createMergingIterator(ctx context.Context, tables []*sstable.SSTable) (core.IteratorInterface[*core.IteratorNode], error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.createMergingIterator")
	defer span.End()

	if len(tables) == 0 {
		return iterator.NewEmptyIterator(), nil
	}

	var iters []core.IteratorInterface[*core.IteratorNode]
	for _, table := range tables {
		iter, err := table.NewIterator(nil, nil, nil, types.Ascending)
		if err != nil {
			// Close already opened iterators before returning
			for _, openedIter := range iters {
				openedIter.Close()
			}
			return nil, fmt.Errorf("failed to create iterator for table %d: %w", table.ID(), err)
		}
		iters = append(iters, iter)
		if cm.metricsCompactionDataReadBytes != nil {
			cm.metricsCompactionDataReadBytes.Add(table.Size())
		}
	}

	mergeParams := iterator.MergingIteratorParams{
		Iters:                iters,
		IsSeriesDeleted:      cm.isSeriesDeletedChecker,
		IsRangeDeleted:       cm.isRangeDeletedChecker,
		ExtractSeriesKeyFunc: cm.extractSeriesKeyFuncForIter,
		DecodeTsFunc:         core.DecodeTimestamp,
	}
	return iterator.NewMergingIteratorWithTombstones(mergeParams)
}

func (cm *CompactionManager) processMergedEntries(
	ctx context.Context,
	mergedIter core.IteratorInterface[*core.IteratorNode],
	retentionCutoffTime int64,
	currentWriter *core.SSTableWriterInterface,
	newSSTables *[]*sstable.SSTable,
	currentFileID *uint64,
) (int64, error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.processMergedEntries")
	defer span.End()

	var totalBytesWritten int64 = 0
	for mergedIter.Next() {
		// key, value, entryType, pointID := mergedIter.At()
		cur, errCur := mergedIter.At()
		if errCur != nil {
			return 0, errCur
		}
		key, value, entryType, pointID := cur.Key, cur.Value, cur.EntryType, cur.SeqNum

		// Retention policy check
		if retentionCutoffTime > 0 && entryType == core.EntryTypePutEvent {
			ts, err := core.DecodeTimestamp(key[len(key)-8:])
			if err != nil {
				cm.logger.Warn("Failed to decode timestamp during retention check, skipping entry.", "key_hex", fmt.Sprintf("%x", key), "error", err)
				continue
			}
			if ts < retentionCutoffTime {
				cm.logger.Debug("Skipping entry due to retention policy", "timestamp", ts, "cutoff", retentionCutoffTime)
				continue
			}
		}

		// Write entry to the current SSTable writer, handling rollovers
		err := cm.writeCompactedEntry(currentWriter, newSSTables, currentFileID, key, value, entryType, pointID)
		if err != nil {
			return totalBytesWritten, err
		}
		totalBytesWritten += int64(len(key) + len(value))
	}

	if err := mergedIter.Error(); err != nil {
		return totalBytesWritten, fmt.Errorf("merging iterator error: %w", err)
	}

	return totalBytesWritten, nil
}

func (cm *CompactionManager) finalizeCompactionWriter(
	ctx context.Context,
	currentWriter core.SSTableWriterInterface,
	currentFileID uint64,
	level int,
) (*sstable.SSTable, error) {
	if currentWriter == nil {
		return nil, nil
	}

	_, span := cm.tracer.Start(ctx, "CompactionManager.finalizeCompactionWriter")
	defer span.End()

	// If the writer has no data, abort it.
	if currentWriter.CurrentSize() == 0 {
		if err := currentWriter.Abort(); err != nil {
			cm.logger.Error("Failed to abort empty final writer", "error", err)
		}
		return nil, nil
	}

	// Finish writing the SSTable
	if err := currentWriter.Finish(); err != nil {
		currentWriter.Abort()
		return nil, fmt.Errorf("failed to finish final sstable: %w", err)
	}

	// Load the newly created SSTable
	finalPath := currentWriter.FilePath()
	loadedTable, loadErr := sstable.LoadSSTable(sstable.LoadSSTableOptions{
		FilePath:   finalPath,
		ID:         currentFileID,
		BlockCache: cm.blockCache,
		Tracer:     cm.tracer,
		Logger:     cm.logger,
	})

	if loadErr != nil {
		return nil, fmt.Errorf("failed to load final newly created sstable %s: %w", finalPath, loadErr)
	}

	// Increment the metric for SSTable creation
	if cm.metrics != nil && cm.metrics.SSTablesCreatedTotal != nil {
		cm.metrics.SSTablesCreatedTotal.Add(1)
	}

	// --- Post-SSTable-Create Hook ---
	// This hook is triggered after a new SSTable is successfully created from a compaction.
	postSSTableCreatePayload := hooks.SSTablePayload{
		ID:    loadedTable.ID(),
		Level: level,
		Path:  loadedTable.FilePath(),
		Size:  loadedTable.Size(),
	}
	// This is a post-hook, so it's typically async and we don't handle the error.
	cm.Engine.GetHookManager().Trigger(context.Background(), hooks.NewPostSSTableCreateEvent(postSSTableCreatePayload))

	return loadedTable, nil
}

func (cm *CompactionManager) calculateRetentionCutoffTime(span trace.Span) int64 {
	if cm.opts.RetentionPeriod == "" || cm.Engine == nil {
		return 0
	}
	clock := cm.Engine.GetClock()
	duration, err := time.ParseDuration(cm.opts.RetentionPeriod)
	if err != nil {
		cm.logger.Error("Invalid retention_period format, disabling retention for this cycle.", "retention_period", cm.opts.RetentionPeriod, "error", err)
		return 0
	}

	cutoffTime := clock.Now().Add(-duration).UnixNano()
	if span != nil {
		span.SetAttributes(attribute.Int64("compaction.retention_cutoff_ns", cutoffTime))
	}
	cm.logger.Debug("Retention policy applied", "cutoff_time", time.Unix(0, cutoffTime).Format(time.RFC3339Nano))
	return cutoffTime
}

func (cm *CompactionManager) mergeMultipleSSTables(ctx context.Context, tables []*sstable.SSTable, targetLevelNum int) ([]*sstable.SSTable, error) {
	_, span := cm.tracer.Start(ctx, "CompactionManager.mergeMultipleSSTables")
	defer span.End()
	span.SetAttributes(attribute.Int("input.tables_count", len(tables)), attribute.Int("compaction.target_level", targetLevelNum))

	if len(tables) == 0 {
		return nil, nil
	}

	if cm.metricsCompactionTablesMerged != nil {
		cm.metricsCompactionTablesMerged.Add(int64(len(tables)))
	}

	// 1. Create Merging Iterator
	mergedIter, err := cm.createMergingIterator(ctx, tables)
	if err != nil {
		return nil, err // createMergingIterator already logs and cleans up
	}
	defer mergedIter.Close()

	// 2. Setup for processing
	var newSSTables []*sstable.SSTable
	var currentWriter core.SSTableWriterInterface
	var currentFileID uint64
	retentionCutoffTime := cm.calculateRetentionCutoffTime(span)

	// 3. Start the first writer
	currentWriter, err = cm.startNewSSTableWriter(&currentFileID)
	if err != nil {
		return nil, err
	}

	// 4. Process all entries
	totalBytesWritten, err := cm.processMergedEntries(ctx, mergedIter, retentionCutoffTime, &currentWriter, &newSSTables, &currentFileID)
	if err != nil {
		if currentWriter != nil {
			currentWriter.Abort()
		}
		return nil, err // processMergedEntries already wraps the error
	}

	// 5. Finalize the last writer
	finalTable, err := cm.finalizeCompactionWriter(ctx, currentWriter, currentFileID, targetLevelNum)
	if err != nil {
		// The newSSTables created so far are valid, but the last one failed.
		// This is a tricky state. For now, we'll discard everything.
		// A more robust implementation might try to keep the successfully created tables.
		var cleanupErrors []error
		for _, tbl := range newSSTables {
			if closeErr := tbl.Close(); closeErr != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to close intermediate sstable %d on error path: %w", tbl.ID(), closeErr))
			}
			if removeErr := sys.Remove(tbl.FilePath()); removeErr != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to remove intermediate sstable %s on error path: %w", tbl.FilePath(), removeErr))
			}
		}
		return nil, errors.Join(append(cleanupErrors, err)...)
	}
	if finalTable != nil {
		newSSTables = append(newSSTables, finalTable)
	}

	// 6. Update metrics
	if cm.metricsCompactionDataWrittenBytes != nil {
		cm.metricsCompactionDataWrittenBytes.Add(totalBytesWritten)
	}

	span.SetAttributes(attribute.Int("output.merged_sstable_count", len(newSSTables)))
	return newSSTables, nil
}
