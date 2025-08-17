package levels

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/sstable"
	"go.opentelemetry.io/otel/trace"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLevelsManager(t *testing.T) {
	lm, err := NewLevelsManager(7, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldest)
	require.NoError(t, err)
	require.NotNil(t, lm)
	defer lm.Close()

	assert.Len(t, lm.levels, 7, "Expected 7 levels")
	assert.Equal(t, 4, lm.maxL0Files, "Expected maxL0Files to be 4")
	assert.Equal(t, int64(1024), lm.baseTargetSize, "Expected baseTargetSize to be 1024")
	assert.Equal(t, PickOldest, lm.fallbackStrategy, "Expected default fallback strategy")

	for i, level := range lm.levels {
		assert.Equal(t, i, level.levelNumber, "Level %d has incorrect levelNumber", i)
		assert.Zero(t, level.Size(), "Level %d should be initialized with 0 tables", i)
	}
}

func TestLevelsManager_AddL0Table(t *testing.T) {
	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("keyA"), []byte("v")}})
	defer tbl1.Close()
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("keyC"), []byte("v")}})
	defer tbl2.Close()

	err := lm.AddL0Table(tbl1)
	require.NoError(t, err)
	l0Tables := lm.GetTablesForLevel(0)
	require.Len(t, l0Tables, 1)
	assert.Equal(t, uint64(1), l0Tables[0].ID())

	err = lm.AddL0Table(tbl2) // Should be prepended
	require.NoError(t, err)
	l0Tables = lm.GetTablesForLevel(0)
	require.Len(t, l0Tables, 2)
	assert.Equal(t, uint64(2), l0Tables[0].ID(), "newest table should be first")
	assert.Equal(t, uint64(1), l0Tables[1].ID())

	// Test adding duplicate
	err = lm.AddL0Table(tbl1)
	require.Error(t, err, "should get an error when adding a duplicate table")
}

func TestLevelsManager_NeedsL0Compaction(t *testing.T) {
	maxL0 := 2
	// The overhead of a single-entry SSTable (header, index, bloom filter, footer, etc.)
	// is non-trivial. Set a trigger size that is clearly larger than this overhead.
	triggerSize := int64(500) // 500 bytes
	lm, _ := NewLevelsManager(3, maxL0, triggerSize, nil, PickOldest)
	defer lm.Close()

	assert.False(t, lm.NeedsL0Compaction(maxL0, triggerSize), "Expected NeedsL0Compaction to be false for empty L0")

	// Test 1: Add one small table. Should not trigger.
	tbl_l0_1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("l0_1_a"), makeValue(10)}})
	defer tbl_l0_1.Close()
	require.NoError(t, lm.AddL0Table(tbl_l0_1))
	assert.False(t, lm.NeedsL0Compaction(maxL0, triggerSize), "Expected NeedsL0Compaction to be false for L0 with 1 small table")

	// Test 2: Add another small table. Should trigger by count.
	tbl_l0_2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("l0_2_a"), makeValue(10)}})
	defer tbl_l0_2.Close()
	require.NoError(t, lm.AddL0Table(tbl_l0_2))
	assert.True(t, lm.NeedsL0Compaction(maxL0, triggerSize), "Expected NeedsL0Compaction to be true for L0 with 2 tables (maxL0=2)")

	// Test 3: Reset and test size-based trigger
	lm, _ = NewLevelsManager(3, 10, triggerSize, nil, PickOldest) // High file count, low size trigger
	defer lm.Close()

	tbl_l0_large := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("l0_large"), makeValue(int(triggerSize))}})
	defer tbl_l0_large.Close()
	require.NoError(t, lm.AddL0Table(tbl_l0_large))
	assert.True(t, lm.NeedsL0Compaction(10, triggerSize), "Expected NeedsL0Compaction to be true for L0 with 1 large table")

	// Test 4: Reset and test size-based trigger with multiple files
	lm, _ = NewLevelsManager(3, 10, triggerSize, nil, PickOldest) // High file count, low size trigger
	defer lm.Close()
	tbl_l0_part1 := newTestSSTable(t, 4, []struct{ key, value []byte }{{[]byte("l0_part1"), makeValue(int(triggerSize / 2))}})
	defer tbl_l0_part1.Close()
	tbl_l0_part2 := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("l0_part2"), makeValue(int(triggerSize / 2))}})
	defer tbl_l0_part2.Close()
	require.NoError(t, lm.AddL0Table(tbl_l0_part1))
	require.NoError(t, lm.AddL0Table(tbl_l0_part2))
	assert.True(t, lm.NeedsL0Compaction(10, triggerSize), "Expected NeedsL0Compaction to be true for L0 with 2 tables meeting size trigger")
}

func TestLevelsManager_GetOverlappingTables(t *testing.T) {
	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	// L0 tables
	l0t1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("banana"), []byte("v")}, {[]byte("beet"), []byte("v")}}) // Min: banana, Max: beet
	defer l0t1.Close()
	l0t2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("kiwi"), []byte("v")}, {[]byte("kumquat"), []byte("v")}}) // Min: kiwi, Max: kumquat
	defer l0t2.Close()
	l0t3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("cat"), []byte("v")}, {[]byte("dog"), []byte("v")}}) // Min: cat, Max: dog
	defer l0t3.Close()
	require.NoError(t, lm.AddL0Table(l0t1))
	require.NoError(t, lm.AddL0Table(l0t2))
	require.NoError(t, lm.AddL0Table(l0t3))

	// L1 tables (non-overlapping, sorted)
	l1t1 := newTestSSTable(t, 101, []struct{ key, value []byte }{{[]byte("ant"), []byte("v")}, {[]byte("apple"), []byte("v")}})
	defer l1t1.Close()
	l1t2 := newTestSSTable(t, 102, []struct{ key, value []byte }{{[]byte("cat"), []byte("v")}, {[]byte("cow"), []byte("v")}})
	defer l1t2.Close()
	l1t3 := newTestSSTable(t, 103, []struct{ key, value []byte }{{[]byte("eel"), []byte("v")}, {[]byte("emu"), []byte("v")}})
	defer l1t3.Close()
	l1t4 := newTestSSTable(t, 104, []struct{ key, value []byte }{{[]byte("gnu"), []byte("v")}, {[]byte("goat"), []byte("v")}})
	defer l1t4.Close()
	// Manually set for test. SetTables will sort them.
	lm.levels[1].SetTables([]*sstable.SSTable{l1t4, l1t2, l1t1, l1t3})

	t.Run("L0_Overlap", func(t *testing.T) {
		l0Overlap := lm.GetOverlappingTables(0, []byte("apple"), []byte("dog"))
		expectedL0OverlapIDs := []uint64{l0t1.ID(), l0t3.ID()}
		actualL0OverlapIDs := GetTableIDs(l0Overlap)
		assert.ElementsMatch(t, expectedL0OverlapIDs, actualL0OverlapIDs, "L0 overlap check failed")
	})

	t.Run("L1_Overlap", func(t *testing.T) {
		// Range ["capybara", "eel"] should overlap with l1t2 ["cat", "cow"] and l1t3 ["eel", "emu"]
		l1Overlap := lm.GetOverlappingTables(1, []byte("capybara"), []byte("eel"))
		expectedL1OverlapIDs := []uint64{l1t2.ID(), l1t3.ID()}
		actualL1OverlapIDs := GetTableIDs(l1Overlap)
		assert.ElementsMatch(t, expectedL1OverlapIDs, actualL1OverlapIDs, "L1 overlap check failed")
	})

	t.Run("L1_NoOverlap", func(t *testing.T) {
		l1NoOverlap := lm.GetOverlappingTables(1, []byte("zebra"), []byte("yak"))
		assert.Empty(t, l1NoOverlap, "L1 should have no overlapping tables for this range")
	})
}

func TestLevelsManager_ApplyCompactionResults(t *testing.T) {
	t.Run("L0_to_L1", func(t *testing.T) {
		lm, _ := NewLevelsManager(3, 2, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldest)
		defer lm.Close()

		// Initial L0 tables
		l0t1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("l0a"), []byte("v")}})
		defer l0t1.Close()
		l0t2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("l0c"), []byte("v")}})
		defer l0t2.Close()
		require.NoError(t, lm.AddL0Table(l0t1))
		require.NoError(t, lm.AddL0Table(l0t2))

		// Initial L1 table (will overlap)
		l1t_old := newTestSSTable(t, 101, []struct{ key, value []byte }{{[]byte("l0b"), []byte("v")}})
		defer l1t_old.Close()
		require.NoError(t, lm.AddTableToLevel(1, l1t_old))

		// New table resulting from L0->L1 compaction
		l1t_new1 := newTestSSTable(t, 201, []struct{ key, value []byte }{{[]byte("l0merged"), []byte("v")}})
		defer l1t_new1.Close()

		oldTables := []*sstable.SSTable{l0t1, l0t2, l1t_old}
		newTables := []*sstable.SSTable{l1t_new1}

		err := lm.ApplyCompactionResults(0, 1, newTables, oldTables)
		require.NoError(t, err)

		// Check L0
		assert.Zero(t, lm.levels[0].Size(), "Expected L0 to be empty")

		// Check L1
		l1Tables := lm.GetTablesForLevel(1)
		require.Len(t, l1Tables, 1, "Expected L1 to contain 1 table")
		assert.Equal(t, l1t_new1.ID(), l1Tables[0].ID(), "L1 should contain the new table")
	})

	t.Run("LN_to_LN+1", func(t *testing.T) {
		lm, _ := NewLevelsManager(3, 2, 1024, nil, PickOldest)
		defer lm.Close()

		l1_source := newTestSSTable(t, 301, []struct{ key, value []byte }{{[]byte("l1m"), []byte("v")}})
		defer l1_source.Close()
		l2_overlap := newTestSSTable(t, 302, []struct{ key, value []byte }{{[]byte("l1n"), []byte("v")}})
		defer l2_overlap.Close()
		l2_non_overlap := newTestSSTable(t, 303, []struct{ key, value []byte }{{[]byte("l2z"), []byte("v")}})
		defer l2_non_overlap.Close()

		require.NoError(t, lm.AddTableToLevel(1, l1_source))
		require.NoError(t, lm.AddTableToLevel(2, l2_overlap))
		require.NoError(t, lm.AddTableToLevel(2, l2_non_overlap))

		l2_new := newTestSSTable(t, 401, []struct{ key, value []byte }{{[]byte("l1merged"), []byte("v")}})
		defer l2_new.Close()
		oldLNTables := []*sstable.SSTable{l1_source, l2_overlap}
		newLNTables := []*sstable.SSTable{l2_new}

		err := lm.ApplyCompactionResults(1, 2, newLNTables, oldLNTables)
		require.NoError(t, err)

		assert.Zero(t, lm.levels[1].Size(), "Expected L1 to be empty after L1->L2 compaction")

		l2Tables := lm.GetTablesForLevel(2)
		// Expected tables in L2: the new one and the non-overlapping one.
		expectedL2IDs := []uint64{l2_new.ID(), l2_non_overlap.ID()}
		actualL2IDs := GetTableIDs(l2Tables)
		assert.ElementsMatch(t, expectedL2IDs, actualL2IDs, "L2 content mismatch after compaction")
	})
}

func TestLevelsManager_NeedsLevelNCompaction(t *testing.T) {

	baseTarget := int64(1024)
	multiplier := 2
	lm, _ := NewLevelsManager(4, 2, baseTarget, nil, PickOldest) // Re-init lm with new baseTarget
	defer lm.Close()

	assert.False(t, lm.NeedsLevelNCompaction(0, multiplier), "NeedsLevelNCompaction should be false for L0")
	assert.False(t, lm.NeedsLevelNCompaction(3, multiplier), "NeedsLevelNCompaction should be false for max level")

	// Test L1
	l1_tbl1_needs := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("l1_a"), makeValue(int(baseTarget / 2))}})
	defer l1_tbl1_needs.Close()
	require.NoError(t, lm.AddTableToLevel(1, l1_tbl1_needs))
	assert.False(t, lm.NeedsLevelNCompaction(1, multiplier), "L1 with size < target should not need compaction")

	val := makeValue(int(baseTarget))
	l1_tbl2_needs := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("l1_c"), val}})
	defer l1_tbl2_needs.Close()
	require.NoError(t, lm.AddTableToLevel(1, l1_tbl2_needs))
	assert.True(t, lm.NeedsLevelNCompaction(1, multiplier), "L1 with size >= target should need compaction")

	// Test L2
	l2_target_size := baseTarget * int64(multiplier)
	l2_tbl1_needs := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("l2_e"), makeValue(int(l2_target_size / 2))}})
	defer l2_tbl1_needs.Close()
	require.NoError(t, lm.AddTableToLevel(2, l2_tbl1_needs))
	assert.False(t, lm.NeedsLevelNCompaction(2, multiplier), "L2 with size < target should not need compaction")

	l2_tbl2_needs := newTestSSTable(t, 4, []struct{ key, value []byte }{{[]byte("l2_g"), makeValue(int(l2_target_size))}})
	defer l2_tbl2_needs.Close()
	require.NoError(t, lm.AddTableToLevel(2, l2_tbl2_needs))
	assert.True(t, lm.NeedsLevelNCompaction(2, multiplier), "L2 with size >= target should need compaction")
}

func TestLevelsManager_PickCompactionCandidateForLevelN(t *testing.T) {
	t.Run("PicksCandidateWithMostOverlap", func(t *testing.T) {
		// 1. Setup
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldest)
		defer lm.Close()

		// 2. Arrange: Create SSTables
		// L1 Tables
		// Table A overlaps with a very large table in L2
		l1_tableA := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("key_a"), []byte("v")}, {[]byte("key_c"), []byte("v")}}) // Range a-c
		defer l1_tableA.Close()
		// Table B overlaps with two small tables in L2
		l1_tableB := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("key_m"), []byte("v")}, {[]byte("key_p"), []byte("v")}}) // Range m-p
		defer l1_tableB.Close()

		// L2 Tables
		// Create Table X to be very large to maximize its overlap size
		l2_tableX_large := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_b"), makeValue(2048)}}) // Overlaps with l1_tableA
		defer l2_tableX_large.Close()

		// Create Table Y and Z to be small
		l2_tableY := newTestSSTable(t, 11, []struct{ key, value []byte }{{[]byte("key_n"), makeValue(10)}}) // Overlaps with l1_tableB
		defer l2_tableY.Close()
		l2_tableZ := newTestSSTable(t, 12, []struct{ key, value []byte }{{[]byte("key_o"), makeValue(10)}}) // Overlaps with l1_tableB
		defer l2_tableZ.Close()

		// Set up the levels
		// Overlap(A) = Size(X_large) = ~2KB+
		// Overlap(B) = Size(Y) + Size(Z) = ~20 bytes + overhead
		// We expect Table A to be chosen.
		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})
		lm.levels[2].SetTables([]*sstable.SSTable{l2_tableX_large, l2_tableY, l2_tableZ})

		// 3. Act
		candidate := lm.PickCompactionCandidateForLevelN(1)

		// 4. Assert
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableA.ID(), candidate.ID(), "Should pick Table A with the most overlap size")
	})

	t.Run("Fallback_PicksOldestCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldest)
		defer lm.Close()

		// Table A is newer (ID 10) but has a smaller MinKey
		l1_tableA := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_c"), makeValue(100)}})
		defer l1_tableA.Close()
		// Table B is older (ID 5) but has a larger MinKey
		l1_tableB := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("key_d"), makeValue(10)}})
		defer l1_tableB.Close()
		// L2 table that does not overlap with L1 tables
		l2_tableX := newTestSSTable(t, 20, []struct{ key, value []byte }{{[]byte("key_x"), []byte("v")}})
		defer l2_tableX.Close()

		// SetTables will sort them by minKey, so order is [A, B]
		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})
		lm.levels[2].SetTables([]*sstable.SSTable{l2_tableX})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		// When there is no overlap, it should pick the oldest table (B, ID 5)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the oldest table (smallest ID) when there is no overlap")
	})

	t.Run("Fallback_PicksLargestCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickLargest)
		defer lm.Close()

		// Table A is smaller but older (ID 5)
		l1_tableA := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("key_a"), makeValue(10)}})
		defer l1_tableA.Close()
		// Table B is larger but newer (ID 10)
		l1_tableB := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_b"), makeValue(100)}})
		defer l1_tableB.Close()
		// L2 table that does not overlap with L1 tables
		l2_tableX := newTestSSTable(t, 20, []struct{ key, value []byte }{{[]byte("key_x"), []byte("v")}})
		defer l2_tableX.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})
		lm.levels[2].SetTables([]*sstable.SSTable{l2_tableX})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		// When there is no overlap, it should pick the largest table (B)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the largest table when there is no overlap")
	})

	t.Run("Fallback_PicksSmallestCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickSmallest)
		defer lm.Close()

		// Table A is larger but older (ID 5)
		l1_tableA := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("key_a"), makeValue(100)}})
		defer l1_tableA.Close()
		// Table B is smaller but newer (ID 10)
		l1_tableB := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_b"), makeValue(10)}})
		defer l1_tableB.Close()
		// L2 table that does not overlap with L1 tables
		l2_tableX := newTestSSTable(t, 20, []struct{ key, value []byte }{{[]byte("key_x"), []byte("v")}})
		defer l2_tableX.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})
		lm.levels[2].SetTables([]*sstable.SSTable{l2_tableX})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		// When there is no overlap, it should pick the smallest table (B)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the smallest table when there is no overlap")
	})

	t.Run("Fallback_PicksMostKeysCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickMostKeys)
		defer lm.Close()

		// Table A is smaller, older, but has the most keys (3)
		l1_tableA := newTestSSTable(t, 5, []struct{ key, value []byte }{
			{[]byte("key_a1"), makeValue(10)},
			{[]byte("key_a2"), makeValue(10)},
			{[]byte("key_a3"), makeValue(10)},
		})
		defer l1_tableA.Close()
		// Table B is larger, newer, but has fewer keys (1)
		l1_tableB := newTestSSTable(t, 10, []struct{ key, value []byte }{
			{[]byte("key_b1"), makeValue(100)},
		})
		defer l1_tableB.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		// When there is no overlap, it should pick the table with the most keys (A)
		assert.Equal(t, l1_tableA.ID(), candidate.ID(), "Should pick the table with the most keys when there is no overlap")
	})

	t.Run("Fallback_PicksSmallestAvgKeySizeCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickSmallestAvgKeySize)
		defer lm.Close()

		// Table A: Larger size, fewer keys -> larger avg size per key
		// Size is dominated by value size. Let's make it large.
		tableA_entries := []struct{ key, value []byte }{
			{[]byte("key_a1"), makeValue(1000)}, // 1 key, ~1000 byte value
		}
		l1_tableA := newTestSSTable(t, 10, tableA_entries)
		defer l1_tableA.Close()
		// Approx avg size: > 1000

		// Table B: Smaller total size, but many more keys -> smaller avg size per key
		tableB_entries := make([]struct{ key, value []byte }, 0, 100)
		for i := 0; i < 100; i++ {
			tableB_entries = append(tableB_entries, struct{ key, value []byte }{
				key:   []byte(fmt.Sprintf("key_b%02d", i)),
				value: makeValue(5), // 100 keys, 5 byte values each
			})
		}
		l1_tableB := newTestSSTable(t, 5, tableB_entries)
		defer l1_tableB.Close()
		// Approx avg size: (100 * 5 + overhead) / 100 ~= 5 + overhead/100

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the table with the smallest average size per key")
	})

	t.Run("Fallback_PicksOldestByTimestampCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldestByTimestamp)
		defer lm.Close()

		// Table A has a larger MinKey ("key_c") but is older by ID (5)
		l1_tableA := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("key_c"), makeValue(100)}})
		defer l1_tableA.Close()
		// Table B has a smaller MinKey ("key_b") but is newer by ID (10)
		l1_tableB := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_b"), makeValue(10)}})
		defer l1_tableB.Close()

		// L2 table that does not overlap with L1 tables
		l2_tableX := newTestSSTable(t, 20, []struct{ key, value []byte }{{[]byte("key_x"), []byte("v")}})
		defer l2_tableX.Close()

		// SetTables will sort them by minKey, so the order in lm.levels[1].tables will be [B, A]
		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})
		lm.levels[2].SetTables([]*sstable.SSTable{l2_tableX})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the table with the smallest MinKey (oldest timestamp) when there is no overlap")
	})

	t.Run("Fallback_PicksFewestKeysCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickFewestKeys)
		defer lm.Close()

		// Table A has more keys (10)
		tableA_entries := make([]struct{ key, value []byte }, 10)
		for i := 0; i < 10; i++ {
			tableA_entries[i] = struct{ key, value []byte }{key: []byte(fmt.Sprintf("key_a%02d", i)), value: makeValue(10)}
		}
		l1_tableA := newTestSSTable(t, 10, tableA_entries)
		defer l1_tableA.Close()

		// Table B has the fewest keys (3)
		l1_tableB := newTestSSTable(t, 5, []struct{ key, value []byte }{
			{[]byte("key_b1"), makeValue(10)},
			{[]byte("key_b2"), makeValue(10)},
			{[]byte("key_b3"), makeValue(10)},
		})
		defer l1_tableB.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the table with the fewest keys")
	})

	t.Run("Fallback_PicksRandomCandidateWhenNoOverlap", func(t *testing.T) {
		// Seed for deterministic test result. In a real app, this is done once at startup.
		rand.Seed(1)

		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickRandom)
		defer lm.Close()

		l1_tableA := newTestSSTable(t, 5, []struct{ key, value []byte }{{[]byte("key_a"), makeValue(100)}})
		defer l1_tableA.Close()
		l1_tableB := newTestSSTable(t, 10, []struct{ key, value []byte }{{[]byte("key_b"), makeValue(10)}})
		defer l1_tableB.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB}) // Sorted order will be [A, B]

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "With a fixed seed(1), rand.Intn(2) returns 1, so it should pick table B")
	})

	t.Run("Fallback_PicksLargestAvgKeySizeCandidateWhenNoOverlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickLargestAvgKeySize)
		defer lm.Close()

		// Table A: Smaller total size, but many keys -> smaller avg size per key
		tableA_entries := make([]struct{ key, value []byte }, 100)
		for i := 0; i < 100; i++ {
			tableA_entries[i] = struct{ key, value []byte }{
				key:   []byte(fmt.Sprintf("key_a%02d", i)),
				value: makeValue(5), // 100 keys, 5 byte values each
			}
		}
		l1_tableA := newTestSSTable(t, 5, tableA_entries)
		defer l1_tableA.Close()

		// Table B: Larger total size, fewer keys -> larger avg size per key
		tableB_entries := []struct{ key, value []byte }{
			{[]byte("key_b1"), makeValue(1000)}, // 1 key, ~1000 byte value
		}
		l1_tableB := newTestSSTable(t, 10, tableB_entries)
		defer l1_tableB.Close()

		lm.levels[1].SetTables([]*sstable.SSTable{l1_tableA, l1_tableB})

		candidate := lm.PickCompactionCandidateForLevelN(1)
		require.NotNil(t, candidate)
		assert.Equal(t, l1_tableB.ID(), candidate.ID(), "Should pick the table with the largest average size per key")
	})

	t.Run("ReturnsNilForInvalidOrEmptyLevel", func(t *testing.T) {
		lm, _ := NewLevelsManager(5, 4, 1024, trace.NewNoopTracerProvider().Tracer("test"), PickOldest)
		defer lm.Close()

		assert.Nil(t, lm.PickCompactionCandidateForLevelN(0), "Should return nil for L0")
		assert.Nil(t, lm.PickCompactionCandidateForLevelN(4), "Should return nil for max level - 1")
		assert.Nil(t, lm.PickCompactionCandidateForLevelN(1), "Should return nil for empty level")
	})
}

func TestLevelsManager_VerifyConsistency(t *testing.T) {
	t.Run("Valid_L1", func(t *testing.T) {
		lm, _ := NewLevelsManager(3, 2, 100, nil, PickOldest)
		defer lm.Close()

		l1t1 := newTestSSTable(t, 101, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}, {[]byte("b"), []byte("v")}})
		defer l1t1.Close()
		l1t2 := newTestSSTable(t, 102, []struct{ key, value []byte }{{[]byte("d"), []byte("v")}, {[]byte("e"), []byte("v")}})
		defer l1t2.Close()
		lm.levels[1].SetTables([]*sstable.SSTable{l1t1, l1t2})

		errors := lm.VerifyConsistency()
		assert.Empty(t, errors, "Expected no errors for a valid L1")
	})

	t.Run("L1_Overlap", func(t *testing.T) {
		lm, _ := NewLevelsManager(3, 2, 100, nil, PickOldest)
		defer lm.Close()

		tableA := newTestSSTable(t, 301, []struct{ key, value []byte }{{[]byte("alpha"), []byte("v")}, {[]byte("gamma"), []byte("v")}})
		defer tableA.Close()
		tableB := newTestSSTable(t, 302, []struct{ key, value []byte }{{[]byte("beta"), []byte("v")}, {[]byte("delta"), []byte("v")}})
		defer tableB.Close()
		// SetTables will sort them by minKey, but they still overlap.
		lm.levels[1].SetTables([]*sstable.SSTable{tableA, tableB})

		errors := lm.VerifyConsistency()
		require.NotEmpty(t, errors, "Expected overlap error in L1")
		assert.Contains(t, errors[0].Error(), "overlaps with SSTable ID")
	})

	t.Run("L1_SortOrderError", func(t *testing.T) {
		lm, _ := NewLevelsManager(3, 2, 100, nil, PickOldest)
		defer lm.Close()

		l1t1 := newTestSSTable(t, 101, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}})
		defer l1t1.Close()
		l1t2 := newTestSSTable(t, 102, []struct{ key, value []byte }{{[]byte("d"), []byte("v")}})
		defer l1t2.Close()

		// Manually create an unsorted LevelState to bypass SetTables's auto-sorting
		unsortedLevelState := &LevelState{
			levelNumber: 1,
			tables:      []*sstable.SSTable{l1t2, l1t1}, // Intentionally wrong order
			tableMap:    map[uint64]*sstable.SSTable{l1t1.ID(): l1t1, l1t2.ID(): l1t2},
		}
		lm.levels[1] = unsortedLevelState

		errors := lm.VerifyConsistency()
		require.NotEmpty(t, errors, "Expected sort order error in L1")
		assert.Contains(t, errors[0].Error(), "is not sorted correctly")
	})
}

func TestLevelsManager_ApplyCompactionResults_InvalidLevels(t *testing.T) {
	lm, _ := NewLevelsManager(3, 2, 1024, nil, PickOldest)
	defer lm.Close()

	newTables := []*sstable.SSTable{}
	oldTables := []*sstable.SSTable{}

	tests := []struct {
		name        string
		sourceLevel int
		targetLevel int
		expectedErr string // Substring of the expected error message
	}{
		{"source_level_too_low", -1, 1, "invalid source or target level number"},
		{"source_level_too_high", 3, 1, "invalid source or target level number"},
		{"target_level_too_low", 0, -1, "invalid source or target level number"},
		{"target_level_too_high", 0, 3, "invalid source or target level number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := lm.ApplyCompactionResults(tt.sourceLevel, tt.targetLevel, newTables, oldTables)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestLevelsManager_MaxLevels(t *testing.T) {
	lm, _ := NewLevelsManager(5, 4, 1024, nil, PickOldest)
	defer lm.Close()
	assert.Equal(t, 5, lm.MaxLevels())
}

func TestLevelsManager_GetTotalSizeForLevel(t *testing.T) {

	lm, _ := NewLevelsManager(3, 2, 100, nil, PickOldest)
	defer lm.Close()

	assert.Zero(t, lm.GetTotalSizeForLevel(0), "Expected total size for L0 to be 0")

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("size_a"), makeValue(50)}})
	defer tbl1.Close()
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("size_c"), makeValue(60)}})
	defer tbl2.Close()
	require.NoError(t, lm.AddL0Table(tbl1))
	require.NoError(t, lm.AddL0Table(tbl2))

	expectedSizeL0 := tbl1.Size() + tbl2.Size()
	assert.Equal(t, expectedSizeL0, lm.GetTotalSizeForLevel(0), "Total size for L0 mismatch")
}

func TestLevelsManager_GetSSTablesForRead_Concurrency(t *testing.T) {

	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()
	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("conc_a"), []byte("v")}})
	defer tbl1.Close()
	require.NoError(t, lm.AddL0Table(tbl1))

	// Simulate concurrent read and modification (compaction)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		levelStates, unlockFunc := lm.GetSSTablesForRead()
		defer unlockFunc()
		// Perform some checks on the locked view
		assert.Len(t, levelStates[0].GetTables(), 1, "Snapshot should have 1 table in L0")
		// Simulate reading from snapshot
		time.Sleep(50 * time.Millisecond)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Ensure GetSSTablesForRead is likely called first
		// Simulate a compaction result that modifies levels
		tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("conc_c"), []byte("v")}})
		defer tbl2.Close()
		require.NoError(t, lm.ApplyCompactionResults(0, 1, []*sstable.SSTable{tbl2}, []*sstable.SSTable{tbl1}))
	}()

	wg.Wait()
	// Final check on lm state
	assert.Zero(t, lm.levels[0].Size(), "L0 should be empty after simulated compaction")
	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1, "L1 should have 1 table after compaction")
	assert.Equal(t, uint64(2), l1Tables[0].ID(), "L1 content mismatch after simulated compaction")
}

func TestLevelsManager_GetLevelForTable(t *testing.T) {
	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	// Add tables
	tbl0 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("l0a"), []byte("v")}})
	defer tbl0.Close()
	tbl1 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("l1a"), []byte("v")}})
	defer tbl1.Close()
	tbl2 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("l2a"), []byte("v")}})
	defer tbl2.Close()

	require.NoError(t, lm.AddL0Table(tbl0))
	require.NoError(t, lm.AddTableToLevel(1, tbl1))
	require.NoError(t, lm.AddTableToLevel(2, tbl2))

	// Test finding existing tables
	level, found := lm.GetLevelForTable(1)
	assert.True(t, found, "Should find table 1")
	assert.Equal(t, 0, level, "Table 1 should be in L0")

	level, found = lm.GetLevelForTable(2)
	assert.True(t, found, "Should find table 2")
	assert.Equal(t, 1, level, "Table 2 should be in L1")

	level, found = lm.GetLevelForTable(3)
	assert.True(t, found, "Should find table 3")
	assert.Equal(t, 2, level, "Table 3 should be in L2")

	// Test finding non-existent table
	level, found = lm.GetLevelForTable(99)
	assert.False(t, found, "Should not find table 99")
	assert.Equal(t, -1, level, "Level for non-existent table should be -1")

	// Test after removing a table
	require.NoError(t, lm.RemoveTables(1, []uint64{2}))
	level, found = lm.GetLevelForTable(2)
	assert.False(t, found, "Should not find table 2 after removal")
	assert.Equal(t, -1, level, "Level for removed table should be -1")
}

func TestLevelsManager_AddTablesToLevel(t *testing.T) {
	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	// Test adding to a valid level
	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("keyC"), []byte("v")}})
	defer tbl1.Close()
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("keyA"), []byte("v")}})
	defer tbl2.Close()

	tables := []*sstable.SSTable{tbl1, tbl2}
	err := lm.AddTablesToLevel(1, tables)
	require.NoError(t, err)

	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 2)
	// Check if they are sorted by MinKey
	assert.Equal(t, uint64(2), l1Tables[0].ID()) // keyA
	assert.Equal(t, uint64(1), l1Tables[1].ID()) // keyC

	// Test adding to an invalid level
	err = lm.AddTablesToLevel(3, tables) // Level 3 is out of bounds
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid level number")

	// Test adding empty slice
	err = lm.AddTablesToLevel(2, []*sstable.SSTable{})
	require.NoError(t, err, "Adding an empty slice of tables should not produce an error")
}
func TestLevelsManager_AddTableToLevel(t *testing.T) {

	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	// Test adding to a valid level
	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("keyA"), []byte("valueA")}})
	defer tbl1.Close()
	err := lm.AddTableToLevel(1, tbl1)
	require.NoError(t, err)
	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1)
	assert.Equal(t, uint64(1), l1Tables[0].ID())

	// Test adding to an invalid level
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("keyB"), []byte("valueB")}})
	defer tbl2.Close()
	err = lm.AddTableToLevel(3, tbl2) // Level 3 is out of bounds
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid level number")
}

func TestLevelsManager_RemoveTables(t *testing.T) {
	lm, _ := NewLevelsManager(3, 4, 1024, nil, PickOldest)
	defer lm.Close()

	// Add some tables to level 1
	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("keyA"), []byte("valueA")}})
	defer tbl1.Close()
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("keyB"), []byte("valueB")}})
	defer tbl2.Close()
	require.NoError(t, lm.AddTableToLevel(1, tbl1))
	require.NoError(t, lm.AddTableToLevel(1, tbl2))

	// Remove table with ID 1 from level 1
	err := lm.RemoveTables(1, []uint64{1})
	require.NoError(t, err)
	l1Tables := lm.GetTablesForLevel(1)
	require.Len(t, l1Tables, 1)
	assert.Equal(t, uint64(2), l1Tables[0].ID())
}
