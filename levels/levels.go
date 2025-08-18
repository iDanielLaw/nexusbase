package levels

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"

	"github.com/INLOpen/nexusbase/sstable"
	"go.opentelemetry.io/otel/trace"
)

// CompactionFallbackStrategy defines the strategy for picking a compaction candidate
// when no table has significant overlap with the next level.
type CompactionFallbackStrategy int

const (
	// PickOldest selects the table with the smallest ID (oldest). This is the default.
	PickOldest CompactionFallbackStrategy = iota
	// PickLargest selects the table with the largest size.
	PickLargest
	// PickSmallest selects the table with the smallest size.
	PickSmallest
	// PickMostKeys selects the table with the most keys.
	PickMostKeys
	// PickSmallestAvgKeySize selects the table with the smallest average data size per key.
	PickSmallestAvgKeySize
	// PickOldestByTimestamp selects the table with the smallest MinKey (representing the oldest timestamp).
	PickOldestByTimestamp
	// PickFewestKeys selects the table with the fewest keys, useful for cleaning up small, sparse tables.
	PickFewestKeys
	// PickRandom selects a random table to prevent starvation from other deterministic strategies.
	PickRandom
	// PickLargestAvgKeySize selects the table with the largest average data size per key.
	PickLargestAvgKeySize
)

func GetTableIDs(tables []*sstable.SSTable) []uint64 {
	ids := make([]uint64, len(tables))
	for i, tbl := range tables {
		ids[i] = tbl.ID()
	}
	return ids
}

// LevelsManager manages the different levels of SSTables in the LSM tree.
// It handles adding new SSTables (from memtable flushes or compactions)
// and orchestrates compactions between levels.
// Corresponds to FR5.
type LevelsManager struct {
	mu             sync.RWMutex
	levels         []*LevelState // Slice of levels, L0, L1, L2, ...
	maxLevels      int           // Maximum number of levels
	maxL0Files     int           // Trigger for L0->L1 compaction
	baseTargetSize int64         // Target size for L1 SSTables, subsequent levels are multiples
	tracer         trace.Tracer  // For creating spans
	// fallbackStrategy defines the logic to use when no table has a clear overlap advantage.
	fallbackStrategy CompactionFallbackStrategy
}

var _ Manager = (*LevelsManager)(nil)

// NewLevelsManager creates a new LevelsManager.
// Corresponds to FR5.1, FR5.2.
func NewLevelsManager(maxLevels int, maxL0Files int, baseTargetSize int64, tracer trace.Tracer, fallbackStrategy CompactionFallbackStrategy) (*LevelsManager, error) {
	lm := &LevelsManager{
		levels:           make([]*LevelState, maxLevels),
		maxLevels:        maxLevels,
		maxL0Files:       maxL0Files,
		baseTargetSize:   baseTargetSize,
		tracer:           tracer,
		fallbackStrategy: fallbackStrategy,
	}
	for i := 0; i < maxLevels; i++ {
		lm.levels[i] = newLevelState(i)
	}
	return lm, nil
}

// AddL0Table adds a newly flushed SSTable to Level 0.
// L0 tables are typically sorted by creation time (newest first for lookups).
// Corresponds to FR3.3 (partially, when flush completes) and FR5.2.
func (lm *LevelsManager) AddL0Table(table *sstable.SSTable) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	// The LevelState.Add method now correctly handles prepending for L0
	// and checks for duplicates.
	return lm.levels[0].Add(table)
}

// AddTableToLevel adds an SSTable to a specific level.
// This is primarily used during recovery from manifest.
func (lm *LevelsManager) AddTableToLevel(levelNum int, table *sstable.SSTable) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.addTableToLevelUnsafe(levelNum, table)
}

// AddTablesToLevel adds multiple SSTables to a specific level.
// This is more efficient than calling AddTableToLevel in a loop,
// especially for L1+ levels. It's intended for use during recovery.
func (lm *LevelsManager) AddTablesToLevel(levelNum int, tables []*sstable.SSTable) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if levelNum < 0 || levelNum >= len(lm.levels) {
		return fmt.Errorf("invalid level number %d", levelNum)
	}
	if len(tables) == 0 {
		return nil
	}

	return lm.levels[levelNum].AddBatch(tables)
}
func (lm *LevelsManager) addTableToLevelUnsafe(levelNum int, table *sstable.SSTable) error {
	if levelNum < 0 || levelNum >= len(lm.levels) {
		return fmt.Errorf("invalid level number %d", levelNum)
	}

	level := lm.levels[levelNum]
	level.Add(table)
	return nil
}

func (lm *LevelsManager) RemoveTables(levelNum int, tablesToRemove []uint64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	return lm.removeTablesUnsafe(levelNum, tablesToRemove)
}

func (lm *LevelsManager) removeTablesUnsafe(levelNum int, tablesToRemove []uint64) error {
	if levelNum < 0 || levelNum >= lm.maxLevels {
		return fmt.Errorf("invalid level number %d", levelNum)
	}

	for _, tableID := range tablesToRemove {
		lm.levels[levelNum].Remove(tableID)
	}

	return nil
}

// GetSSTablesForRead returns a snapshot of SSTables across all levels for read operations (Get, RangeScan).
// The order of tables returned is important for correct data retrieval (L0 newest first, then L1, L2, ...).
// The caller MUST call the returned unlock function to release the read lock.
// This pattern avoids copying the entire level structure for every read operation.
// Corresponds to FR1.3, FR1.4.
func (lm *LevelsManager) GetSSTablesForRead() ([]*LevelState, func()) {
	lm.mu.RLock()
	return lm.levels, lm.mu.RUnlock
}

// NeedsL0Compaction checks if Level 0 needs compaction based on the number of files.
// It now also considers a total size trigger to avoid compacting too frequently
// when many small memtables are flushed.
func (lm *LevelsManager) NeedsL0Compaction(maxL0Files int, l0CompactionTriggerSize int64) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	if len(lm.levels) == 0 {
		return false
	}
	l0Size := lm.levels[0].Size()
	l0TotalBytes := lm.getTotalSizeForLevelUnSafe(0)
	return l0Size >= maxL0Files || (l0Size > 0 && l0TotalBytes >= l0CompactionTriggerSize)
}

// GetTotalSizeForLevel returns the total size of all SSTables in a given level.
func (lm *LevelsManager) GetTotalSizeForLevel(levelNum int) int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.getTotalSizeForLevelUnSafe(levelNum)
}

func (lm *LevelsManager) getTotalSizeForLevelUnSafe(levelNum int) int64 {

	if levelNum < 0 || levelNum >= len(lm.levels) {
		return 0
	}
	return lm.levels[levelNum].TotalSize()
}

// NeedsLevelNCompaction checks if a level N (N > 0) needs compaction based on its total size.
// Target size for LN is typically baseTargetSize * (multiplier ^ (N-1)).
func (lm *LevelsManager) NeedsLevelNCompaction(levelNum int, levelsTargetSizeMultiplier int) bool {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	// If it's the highest level, it never needs compaction to a higher level.
	if levelNum >= lm.maxLevels-1 {
		return false
	}
	if levelNum <= 0 { // L0 is handled by NeedsL0Compaction
		return false
	}

	currentSize := lm.getTotalSizeForLevelUnSafe(levelNum) // Use the pre-calculated totalSize
	targetSize := lm.baseTargetSize
	for i := 1; i < levelNum; i++ {
		targetSize *= int64(levelsTargetSizeMultiplier)
	}
	return currentSize >= targetSize
}

// GetTablesForLevel returns a slice of SSTables for a specific level.
// The returned slice is a copy to prevent modification issues.
func (lm *LevelsManager) GetTablesForLevel(levelNum int) []*sstable.SSTable {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.getTablesForLevelUnsafe(levelNum)
}

func (lm *LevelsManager) getTablesForLevelUnsafe(levelNum int) []*sstable.SSTable {
	if levelNum < 0 || levelNum >= len(lm.levels) {
		return nil // Or an empty slice
	}
	return lm.levels[levelNum].GetTables()
}

// NeedsIntraL0Compaction checks if an intra-L0 compaction should be triggered.
// This happens if there are enough "small" files in L0.
func (lm *LevelsManager) NeedsIntraL0Compaction(triggerFileCount int, maxFileSizeBytes int64) bool {
	if triggerFileCount <= 0 || maxFileSizeBytes <= 0 {
		return false // Feature is disabled if not configured properly.
	}

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	l0Tables := lm.levels[0].GetTables()
	smallFileCount := 0
	for _, table := range l0Tables {
		if table.Size() <= maxFileSizeBytes {
			smallFileCount++
		}
	}

	return smallFileCount >= triggerFileCount
}

// PickIntraL0CompactionCandidates selects the set of small files from L0 to be compacted together.
func (lm *LevelsManager) PickIntraL0CompactionCandidates(triggerFileCount int, maxFileSizeBytes int64) []*sstable.SSTable {
	if triggerFileCount <= 0 || maxFileSizeBytes <= 0 {
		return nil
	}

	lm.mu.RLock()
	defer lm.mu.RUnlock()

	l0Tables := lm.levels[0].GetTables()
	candidates := make([]*sstable.SSTable, 0, triggerFileCount)
	for _, table := range l0Tables {
		if table.Size() <= maxFileSizeBytes {
			candidates = append(candidates, table)
		}
	}

	// Only return candidates if we have enough to trigger a compaction.
	if len(candidates) >= triggerFileCount {
		return candidates
	}

	return nil
}

// GetTotalTableCount returns the total number of SSTables across all levels.
func (lm *LevelsManager) GetTotalTableCount() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	count := 0
	for _, level := range lm.levels {
		count += level.Size()
	}
	return count
}

func (lm *LevelsManager) GetLevelTableCounts() (map[int]int, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	counts := make(map[int]int, len(lm.levels))
	for i, level := range lm.levels {

		// The level controller's lock is assumed to be sufficient to safely read the table count.
		counts[i] = len(level.tables)
	}
	return counts, nil
}

// getOverlappingTablesLocked is the unlocked version of GetOverlappingTables.
// It must be called with the LevelsManager's read lock held.
func (lm *LevelsManager) getOverlappingTablesLocked(levelNum int, minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	if levelNum < 0 || levelNum >= len(lm.levels) {
		return nil
	}

	var overlappingTables []*sstable.SSTable

	if levelNum == 0 {
		srcSST := lm.levels[0].GetTables()
		for _, table := range srcSST {
			if (maxRangeKey == nil || bytes.Compare(table.MinKey(), maxRangeKey) <= 0) &&
				(minRangeKey == nil || bytes.Compare(table.MaxKey(), minRangeKey) >= 0) {
				overlappingTables = append(overlappingTables, table)
			}
		}
	} else {
		// For L1+ levels, tables are sorted by MinKey and are non-overlapping.
		tables := lm.levels[levelNum].GetTables()
		// The first candidate is the first table whose MaxKey is >= minRangeKey.
		startIndex := sort.Search(len(tables), func(i int) bool {
			return bytes.Compare(tables[i].MaxKey(), minRangeKey) >= 0
		})

		// Iterate from the first candidate.
		for i := startIndex; i < len(tables); i++ {
			table := tables[i]
			// If the table's MinKey is already beyond maxRangeKey, no further tables will overlap.
			if maxRangeKey != nil && bytes.Compare(table.MinKey(), maxRangeKey) > 0 {
				break
			}
			overlappingTables = append(overlappingTables, table)
		}
	}
	return overlappingTables
}

// GetOverlappingTables returns SSTables from a given level that overlap with the provided key range [minKey, maxKey].
// For L0, all tables are considered overlapping if the range itself is valid (as L0 tables can overlap arbitrarily).
// For L1+, tables are sorted by minKey and non-overlapping, so we can find relevant tables more efficiently.
func (lm *LevelsManager) GetOverlappingTables(levelNum int, minRangeKey, maxRangeKey []byte) []*sstable.SSTable {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	return lm.getOverlappingTablesLocked(levelNum, minRangeKey, maxRangeKey)
}

// PickCompactionCandidateForLevelN selects an SSTable from level N (N > 0) for compaction.
// The primary strategy is to pick the table with the smallest total size of overlapping tables in level N+1.
// This helps reduce write amplification by minimizing the amount of data that needs to be re-written from the next level.
// If one or more tables have no overlap (the ideal case), it falls back to a configurable strategy (e.g., oldest or largest) to ensure
// compaction can still proceed.
func (lm *LevelsManager) PickCompactionCandidateForLevelN(levelNum int) *sstable.SSTable {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if levelNum <= 0 || levelNum >= lm.maxLevels-1 {
		return nil
	}

	level := lm.levels[levelNum]
	if level.Size() == 0 {
		return nil
	}

	tables := level.GetTables()
	var bestTableByOverlap *sstable.SSTable
	// Initialize minOverlapSize to a value that's guaranteed to be larger than any possible overlap.
	minOverlapSize := int64(math.MaxInt64)
	foundACandidate := false

	for _, table := range tables {
		minKey, maxKey := table.MinKey(), table.MaxKey()
		overlappingTables := lm.getOverlappingTablesLocked(levelNum+1, minKey, maxKey)

		var currentOverlapSize int64
		for _, overlapTable := range overlappingTables {
			currentOverlapSize += overlapTable.Size()
		}

		if !foundACandidate || currentOverlapSize < minOverlapSize {
			minOverlapSize = currentOverlapSize
			bestTableByOverlap = table
			foundACandidate = true
		}
	}

	// If the best candidate we could find based on overlap still has some overlap (> 0), we choose it
	// as it minimizes write amplification for this compaction.
	if minOverlapSize > 0 {
		return bestTableByOverlap
	}

	// If we are here, it means minOverlapSize is 0. This is the ideal case for write amplification.
	// It implies one or more tables have no overlap with the next level.
	// We can now use a fallback strategy to choose among ALL tables in the level to address
	// other concerns like age, size, or fragmentation, without a write amplification penalty.
	{
		// Fallback logic is now configurable
		switch lm.fallbackStrategy {
		case PickLargest:
			// Fallback: Pick the largest table in the current level.
			var largestTable *sstable.SSTable
			var maxSize int64 = -1
			for _, table := range tables {
				if table.Size() > maxSize {
					maxSize = table.Size()
					largestTable = table
				}
			}
			return largestTable
		case PickSmallest:
			// Fallback: Pick the smallest table in the current level.
			var smallestTable *sstable.SSTable
			for _, table := range tables {
				if smallestTable == nil || table.Size() < smallestTable.Size() {
					smallestTable = table
				}
			}
			return smallestTable
		case PickMostKeys:
			// Fallback: Pick the table with the most keys.
			var mostKeysTable *sstable.SSTable
			for _, table := range tables {
				if mostKeysTable == nil || table.KeyCount() > mostKeysTable.KeyCount() {
					mostKeysTable = table
				}
			}
			return mostKeysTable
		case PickSmallestAvgKeySize:
			// Fallback: Pick the table with the smallest average size per key.
			// This can be useful for compacting tables with many small, inefficiently stored keys.
			var smallestAvgSizeTable *sstable.SSTable
			minAvgSize := math.MaxFloat64

			for _, table := range tables {
				if table.KeyCount() == 0 {
					continue // Avoid division by zero; tables with no keys are not candidates.
				}
				avgSize := float64(table.Size()) / float64(table.KeyCount())
				if avgSize < minAvgSize {
					minAvgSize = avgSize
					smallestAvgSizeTable = table
				}
			}
			return smallestAvgSizeTable
		case PickLargestAvgKeySize:
			// Fallback: Pick the table with the largest average size per key.
			// This can be useful for compacting tables with few, very large keys.
			var largestAvgSizeTable *sstable.SSTable
			maxAvgSize := -1.0

			for _, table := range tables {
				if table.KeyCount() == 0 {
					continue // Avoid division by zero.
				}
				avgSize := float64(table.Size()) / float64(table.KeyCount())
				if avgSize > maxAvgSize {
					maxAvgSize = avgSize
					largestAvgSizeTable = table
				}
			}
			return largestAvgSizeTable
		case PickOldestByTimestamp:
			// Fallback: Pick the table with the smallest MinKey.
			// Since tables in L1+ are already sorted by MinKey, this is the first table.
			if len(tables) > 0 {
				return tables[0]
			}
			return nil
		case PickFewestKeys:
			// Fallback: Pick the table with the fewest keys.
			// Useful for cleaning up small, sparse tables.
			var fewestKeysTable *sstable.SSTable
			minKeyCount := uint64(math.MaxUint64)
			for _, table := range tables {
				if table.KeyCount() < minKeyCount {
					minKeyCount = table.KeyCount()
					fewestKeysTable = table
				}
			}
			return fewestKeysTable
		case PickRandom:
			// Fallback: Pick a random table.
			// This helps prevent starvation where the same tables are always
			// ignored by other fallback strategies.
			if len(tables) > 0 {
				return tables[rand.Intn(len(tables))]
			}
			return nil
		case PickOldest:
			fallthrough // Fallthrough to the default case
		default:
			// Default Fallback: Pick the oldest table (smallest ID) in the current level.
			var oldestTable *sstable.SSTable
			for _, table := range tables {
				if oldestTable == nil || table.ID() < oldestTable.ID() {
					oldestTable = table
				}
			}
			return oldestTable
		}
	}

	// This part is reached only if the fallback strategy doesn't have a default return,
	// or if minOverlapSize was 0. Returning the first-found table with minimum overlap
	// is a reasonable default.
	return bestTableByOverlap
}

// ApplyCompactionResults updates the levels structure after a compaction.
// It removes oldTables and adds newTables to their respective levels.
// For L0->L1 compaction: sourceLevelNum = 0, targetLevelNum = 1.
// oldTables will contain tables from L0 and potentially overlapping tables from L1.
// newTables will be added to L1.
// Corresponds to FR5.4.
func (lm *LevelsManager) ApplyCompactionResults(
	sourceLevelNum int, // For L0->L1, this is 0. For LN->LN+1, this is N.
	targetLevelNum int, // For L0->L1, this is 1. For LN->LN+1, this is N+1.
	newTables []*sstable.SSTable,
	oldTables []*sstable.SSTable,
) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	// Assuming context might be passed down or a background context is used for internal operations.
	// For simplicity, if lm.tracer is nil (e.g. in tests not setting it up), we skip tracing.
	var span trace.Span
	if lm.tracer != nil {
		_, span = lm.tracer.Start(context.Background(), "LevelsManager.ApplyCompactionResults")
		defer span.End()
	}

	if sourceLevelNum < 0 || sourceLevelNum >= lm.maxLevels ||
		targetLevelNum < 0 || targetLevelNum >= lm.maxLevels {
		return fmt.Errorf("invalid source or target level number in ApplyCompactionResults")
	}

	// Create a map of old table IDs for efficient removal
	oldTableIDs := GetTableIDs(oldTables)
	// Remove old tables from source level
	if err := lm.removeTablesUnsafe(sourceLevelNum, oldTableIDs); err != nil {
		return fmt.Errorf("failed to remove tables from source level %d: %w", sourceLevelNum, err)
	}

	if err := lm.levels[targetLevelNum].AddBatch(newTables); err != nil {
		return fmt.Errorf("failed to add new tables to target level %d: %w", targetLevelNum, err)
	}

	// If old tables overlap with the target level, remove them from the target level as well.
	// This is necessary because overlapping tables in the target level are included in the oldTables list.
	if sourceLevelNum != targetLevelNum {
		if err := lm.removeTablesUnsafe(targetLevelNum, oldTableIDs); err != nil {
			return fmt.Errorf("failed to remove overlapping tables from target level %d: %w", targetLevelNum, err)
		}
	}

	return nil
}

// MaxLevels returns the configured maximum number of levels.
func (lm *LevelsManager) MaxLevels() int {
	// No lock needed as maxLevels is immutable after creation.
	return lm.maxLevels
}

// SetBaseTargetSize sets the base target size for L1.
// This is primarily used for testing to trigger compaction deterministically.
func (lm *LevelsManager) SetBaseTargetSize(size int64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.baseTargetSize = size
}

// GetBaseTargetSize returns the base target size for L1.
func (lm *LevelsManager) GetBaseTargetSize() int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.baseTargetSize
}

// VerifyConsistency checks the structural integrity of the levels.
// This corresponds to parts of FR5.3.
// Returns a slice of errors found.
func (lm *LevelsManager) VerifyConsistency() []error {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	var errs []error

	// Check L1+ levels for sorted order and non-overlapping tables
	for levelNum := 1; levelNum < lm.maxLevels; levelNum++ {
		tables := lm.levels[levelNum].GetTables()
		if len(tables) == 0 {
			continue
		}

		// Check if tables in L1+ are sorted by MinKey
		for i := 0; i < len(tables)-1; i++ {
			if tables[i].MinKey() == nil || tables[i+1].MinKey() == nil {
				errs = append(errs, fmt.Errorf("level %d: table ID %d or %d has nil MinKey", levelNum, tables[i].ID(), tables[i+1].ID()))
				continue // Skip further comparison if MinKey is nil
			}
			if bytes.Compare(tables[i].MinKey(), tables[i+1].MinKey()) > 0 {
				errs = append(errs, fmt.Errorf("level %d: SSTable ID %d (MinKey %s) is not sorted correctly before SSTable ID %d (MinKey %s)",
					levelNum, tables[i].ID(), string(tables[i].MinKey()),
					tables[i+1].ID(), string(tables[i+1].MinKey())))
			}
		}

		// Check for overlaps in L1+
		for i := 0; i < len(tables)-1; i++ {
			tableA := tables[i]
			tableB := tables[i+1]
			// MaxKey of tableA must be less than MinKey of tableB
			if bytes.Compare(tableA.MaxKey(), tableB.MinKey()) >= 0 {
				errs = append(errs, fmt.Errorf("level %d: SSTable ID %d (MaxKey %s) overlaps with SSTable ID %d (MinKey %s)",
					levelNum, tableA.ID(), string(tableA.MaxKey()),
					tableB.ID(), string(tableB.MinKey())))
			}
		}
	}

	// Check individual SSTable integrity (delegated to SSTable.VerifyIntegrity)
	for levelNum := 0; levelNum < lm.maxLevels; levelNum++ {
		for _, table := range lm.levels[levelNum].GetTables() {
			if tableErrs := table.VerifyIntegrity(true); len(tableErrs) > 0 { // Pass true for deepCheck
				for i := range tableErrs { // Add context to errors from SSTable
					tableErrs[i] = fmt.Errorf("level %d, table ID %d: %w", levelNum, table.ID(), tableErrs[i])
				}
				errs = append(errs, tableErrs...)
			}
		}
	}
	return errs
}

// GetLevels returns the slice of LevelState. Exported for testing purposes.
func (lm *LevelsManager) GetLevels() []*LevelState {
	return lm.levels
}

func (lm *LevelsManager) Close() error {
	lm.mu.Lock() // Use full lock as we are modifying the state (potentially nil-ing out tables if they track closed state)
	defer lm.mu.Unlock()

	var firstErr error
	for _, levelState := range lm.levels {
		for _, table := range levelState.GetTables() {
			if table != nil { // Check if table is not nil
				if err := table.Close(); err != nil && firstErr == nil {
					firstErr = fmt.Errorf("failed to close table ID %d in level %d: %w", table.ID(), levelState.levelNumber, err)
				}
			}
		}
	}
	return firstErr
}

// GetLevelForTable finds the level number for a given SSTable ID.
// It returns the level number and true if found, otherwise -1 and false.
func (lm *LevelsManager) GetLevelForTable(tableID uint64) (int, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	for _, levelState := range lm.levels {
		if _, exists := levelState.tableMap[tableID]; exists {
			return levelState.levelNumber, true
		}
	}
	return -1, false
}
