package levels

import (
	"bytes"
	"math"
	"math/rand"
	"sort"

	"github.com/INLOpen/nexusbase/sstable"
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
	// PickNewest selects the table with the largest ID (newest).
	PickNewest
	// PickHighestTombstoneDensity selects the table with the highest ratio of tombstones to keys.
	PickHighestTombstoneDensity
)

// PickCompactionCandidateForLevelN selects an SSTable from level N (N > 0) for compaction.
// It uses a scoring model to balance two factors:
// 1. Tombstone Density: Prioritizes tables with a high ratio of deleted entries to reclaim disk space.
// 2. Overlap Penalty: Penalizes tables that overlap with a large amount of data in the next level.
// If multiple tables have the same highest score, a fallback strategy is used to break the tie.
//
// This method must be called with lm.mu held (either RLock or Lock).
func (lm *LevelsManager) pickCompactionCandidateLocked(levelNum int) *sstable.SSTable {
	if levelNum <= 0 || levelNum >= lm.maxLevels-1 {
		return nil
	}

	level := lm.levels[levelNum]
	if level.Size() == 0 {
		return nil
	}

	tables := level.GetTables()
	var candidates []*sstable.SSTable
	bestScore := -math.MaxFloat64

	for _, table := range tables {
		score := lm.calculateCompactionScore(table, levelNum)
		if score > bestScore {
			bestScore = score
			candidates = []*sstable.SSTable{table} // Start a new list of candidates
		} else if score == bestScore {
			candidates = append(candidates, table) // Add to the list of candidates
		}
	}

	if len(candidates) == 0 {
		return nil // No tables in the level.
	}

	// If there's only one table with the best score, return it.
	if len(candidates) == 1 {
		return candidates[0]
	}

	// If multiple tables have the same best score (e.g., zero overlap),
	// use the configured fallback strategy to break the tie.
	return lm.pickFromCandidatesUsingFallback(candidates)
}

// calculateCompactionScore computes a score for a table based on tombstone density
// and overlap with the next level. Higher score means higher priority for compaction.
func (lm *LevelsManager) calculateCompactionScore(table *sstable.SSTable, levelNum int) float64 {
	// 1. Calculate Tombstone Density Score
	var tombstoneDensity float64
	if table.KeyCount() > 0 {
		tombstoneDensity = float64(table.TombstoneCount()) / float64(table.KeyCount())
	}

	// 2. Calculate Overlap Penalty
	minKey, maxKey := table.MinKey(), table.MaxKey()
	overlappingTables := lm.getOverlappingTablesLocked(levelNum+1, minKey, maxKey)
	var currentOverlapSize int64
	for _, overlapTable := range overlappingTables {
		currentOverlapSize += overlapTable.Size()
	}
	var overlapPenalty float64
	if table.Size() > 0 {
		// Normalize overlap by table size to get a write amplification factor for this compaction
		overlapPenalty = float64(currentOverlapSize) / float64(table.Size())
	}

	// 3. Combine into a final score using configured weights. Higher is better.
	return (lm.tombstoneWeight * tombstoneDensity) - (lm.overlapWeight * overlapPenalty)
}

// pickFromCandidatesUsingFallback applies the configured fallback strategy to select a single
// SSTable from a list of candidates that all have the same best compaction score.
func (lm *LevelsManager) pickFromCandidatesUsingFallback(candidates []*sstable.SSTable) *sstable.SSTable {
	if len(candidates) == 0 {
		return nil
	}

	// For most strategies, we can sort the candidates slice and pick the first element.
	// This simplifies the logic and avoids repetitive loops.
	switch lm.fallbackStrategy {
	case PickLargest:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Size() > candidates[j].Size()
		})
	case PickSmallest:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Size() < candidates[j].Size()
		})
	case PickMostKeys:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].KeyCount() > candidates[j].KeyCount()
		})
	case PickFewestKeys:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].KeyCount() < candidates[j].KeyCount()
		})
	case PickSmallestAvgKeySize:
		sort.Slice(candidates, func(i, j int) bool {
			// Treat tables with 0 keys as having an infinitely large average size.
			if candidates[i].KeyCount() == 0 {
				return false // i is not smaller than j
			}
			if candidates[j].KeyCount() == 0 {
				return true // i is smaller than j (which has 0 keys)
			}
			avgSizeI := float64(candidates[i].Size()) / float64(candidates[i].KeyCount())
			avgSizeJ := float64(candidates[j].Size()) / float64(candidates[j].KeyCount())
			return avgSizeI < avgSizeJ
		})
	case PickLargestAvgKeySize:
		sort.Slice(candidates, func(i, j int) bool {
			// Treat tables with 0 keys as having the smallest average size.
			if candidates[i].KeyCount() == 0 {
				return false // i is not larger than j
			}
			if candidates[j].KeyCount() == 0 {
				return true // i is larger than j (which has 0 keys)
			}
			avgSizeI := float64(candidates[i].Size()) / float64(candidates[i].KeyCount())
			avgSizeJ := float64(candidates[j].Size()) / float64(candidates[j].KeyCount())
			return avgSizeI > avgSizeJ
		})
	case PickOldestByTimestamp:
		sort.Slice(candidates, func(i, j int) bool {
			return bytes.Compare(candidates[i].MinKey(), candidates[j].MinKey()) < 0
		})
	case PickNewest:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].ID() > candidates[j].ID()
		})
	case PickHighestTombstoneDensity:
		sort.Slice(candidates, func(i, j int) bool {
			var densityI, densityJ float64
			if candidates[i].KeyCount() > 0 {
				densityI = float64(candidates[i].TombstoneCount()) / float64(candidates[i].KeyCount())
			}
			if candidates[j].KeyCount() > 0 {
				densityJ = float64(candidates[j].TombstoneCount()) / float64(candidates[j].KeyCount())
			}
			return densityI > densityJ
		})
	case PickRandom:
		// PickRandom is the only strategy that doesn't involve sorting.
		return candidates[rand.Intn(len(candidates))]
	case PickOldest:
		fallthrough
	default:
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].ID() < candidates[j].ID()
		})
	}

	// After sorting, the best candidate is the first one.
	return candidates[0]
}
