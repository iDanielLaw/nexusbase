package levels

import (
	"testing"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a new LevelState for testing.
func newLevelStateForTest(levelNumber int) *LevelState {
	return newLevelState(levelNumber)
}

// Helper to assert the order of tables in a LevelState.
func assertTableOrder(t *testing.T, ls *LevelState, expectedIDs []uint64) {
	t.Helper()
	actualIDs := make([]uint64, len(ls.tables))
	for i, table := range ls.tables {
		actualIDs[i] = table.ID()
	}
	assert.Equal(t, expectedIDs, actualIDs, "table order mismatch")
	assert.Equal(t, len(expectedIDs), ls.Size(), "size mismatch")
	assert.Equal(t, len(expectedIDs), len(ls.tableMap), "tableMap size mismatch")
}

func TestLevelState_Add_L0(t *testing.T) {
	ls := newLevelStateForTest(0) // Level 0

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}})
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("b"), []byte("v")}})
	tbl3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("c"), []byte("v")}})

	// Add first table
	err := ls.Add(tbl1)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{1})

	// Add second table, should be prepended
	err = ls.Add(tbl2)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{2, 1})

	// Add third table, should be prepended
	err = ls.Add(tbl3)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{3, 2, 1})

	// Test adding nil table
	err = ls.Add(nil)
	assert.Error(t, err, "should get an error when adding a nil table")

	// Test adding duplicate table
	err = ls.Add(tbl2)
	assert.Error(t, err, "should get an error when adding a duplicate table")
	assert.Contains(t, err.Error(), "already exists in level")
	assertTableOrder(t, ls, []uint64{3, 2, 1}) // State should not change
}

func TestLevelState_Add_L1Plus(t *testing.T) {
	ls := newLevelStateForTest(1) // Level 1

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("c"), []byte("v")}})
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}})
	tbl3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("b"), []byte("v")}})

	// Add first table
	err := ls.Add(tbl1)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{1}) // [c]

	// Add second table, should be sorted by MinKey
	err = ls.Add(tbl2)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{2, 1}) // [a, c]

	// Add third table, should be sorted by MinKey
	err = ls.Add(tbl3)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{2, 3, 1}) // [a, b, c]

	// Test adding nil table
	err = ls.Add(nil)
	assert.Error(t, err, "should get an error when adding a nil table")

	// Test adding duplicate table
	err = ls.Add(tbl2)
	assert.Error(t, err, "should get an error when adding a duplicate table")
	assertTableOrder(t, ls, []uint64{2, 3, 1}) // State should not change
}

func TestLevelState_Remove(t *testing.T) {
	ls := newLevelStateForTest(0) // Use L0 for simple order

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}})
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("b"), []byte("v")}})
	tbl3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("c"), []byte("v")}})

	// Setup initial state: [3, 2, 1]
	ls.Add(tbl1)
	ls.Add(tbl2)
	ls.Add(tbl3)
	assertTableOrder(t, ls, []uint64{3, 2, 1})

	// Remove from middle
	err := ls.Remove(2)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{3, 1})
	_, ok := ls.tableMap[2]
	assert.False(t, ok, "table 2 should be removed from map")

	// Remove from start
	err = ls.Remove(3)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{1})
	_, ok = ls.tableMap[3]
	assert.False(t, ok, "table 3 should be removed from map")

	// Remove from end
	err = ls.Remove(1)
	require.NoError(t, err)
	assertTableOrder(t, ls, []uint64{})
	_, ok = ls.tableMap[1]
	assert.False(t, ok, "table 1 should be removed from map")

	// Remove non-existent
	err = ls.Remove(99)
	assert.Error(t, err, "should get an error when removing non-existent table")
	assert.Contains(t, err.Error(), "not found in LevelState")
}

func TestLevelState_RemoveBatch(t *testing.T) {
	ls := newLevelStateForTest(1) // Use L1+ for predictable order

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("a"), makeValue(10)}})
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("b"), makeValue(20)}})
	tbl3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("c"), makeValue(30)}})
	tbl4 := newTestSSTable(t, 4, []struct{ key, value []byte }{{[]byte("d"), makeValue(40)}})

	// Helper to reset state for sub-tests
	resetState := func() {
		ls.SetTables([]*sstable.SSTable{tbl1, tbl2, tbl3, tbl4})
		require.Equal(t, tbl1.Size()+tbl2.Size()+tbl3.Size()+tbl4.Size(), ls.TotalSize())
		assertTableOrder(t, ls, []uint64{1, 2, 3, 4})
	}

	t.Run("Remove subset of tables", func(t *testing.T) {
		resetState()
		ls.RemoveBatch([]uint64{2, 4}) // Remove from middle and end

		assertTableOrder(t, ls, []uint64{1, 3})
		assert.Equal(t, tbl1.Size()+tbl3.Size(), ls.TotalSize(), "Total size should be updated correctly")
		_, exists2 := ls.tableMap[2]
		_, exists4 := ls.tableMap[4]
		assert.False(t, exists2, "Table 2 should be removed from map")
		assert.False(t, exists4, "Table 4 should be removed from map")
	})

	t.Run("Remove all tables", func(t *testing.T) {
		resetState()
		ls.RemoveBatch([]uint64{1, 2, 3, 4})

		assertTableOrder(t, ls, []uint64{})
		assert.Zero(t, ls.TotalSize(), "Total size should be zero after removing all tables")
		assert.Empty(t, ls.tableMap, "Table map should be empty")
	})

	t.Run("Remove with non-existent IDs", func(t *testing.T) {
		resetState()
		// Attempt to remove existing (1, 3) and non-existent (99, 100) IDs
		ls.RemoveBatch([]uint64{1, 99, 3, 100})

		assertTableOrder(t, ls, []uint64{2, 4})
		assert.Equal(t, tbl2.Size()+tbl4.Size(), ls.TotalSize(), "Total size should be correct after removing a mix of IDs")
		_, exists1 := ls.tableMap[1]
		_, exists3 := ls.tableMap[3]
		assert.False(t, exists1, "Table 1 should be removed from map")
		assert.False(t, exists3, "Table 3 should be removed from map")
	})
}

func TestLevelState_GetTables_IsCopy(t *testing.T) {
	ls := newLevelStateForTest(0)

	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("a"), []byte("v")}})
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("b"), []byte("v")}})
	ls.Add(tbl2)
	ls.Add(tbl1) // State: [1, 2]

	// Get the tables and modify the returned slice
	tablesCopy := ls.GetTables()
	require.Len(t, tablesCopy, 2, "GetTables should return 2 tables")
	tablesCopy[0] = nil // Modify the copy

	// Get the tables again and check if the internal state was affected
	internalTables := ls.GetTables()
	assert.NotNil(t, internalTables[0], "internal state should not be modified by changes to the returned copy")
	assert.Equal(t, uint64(1), internalTables[0].ID(), "internal state should be unchanged")
	assertTableOrder(t, ls, []uint64{1, 2})
}

func TestLevelState_TotalSize(t *testing.T) {
	ls := newLevelStateForTest(1)

	assert.Zero(t, ls.TotalSize(), "Initial total size should be 0")

	// Add a table
	tbl1 := newTestSSTable(t, 1, []struct{ key, value []byte }{{[]byte("a"), makeValue(100)}})
	defer tbl1.Close()
	err := ls.Add(tbl1)
	require.NoError(t, err)
	assert.Equal(t, tbl1.Size(), ls.TotalSize(), "Total size should be equal to the first table's size")

	// Add another table
	tbl2 := newTestSSTable(t, 2, []struct{ key, value []byte }{{[]byte("b"), makeValue(200)}})
	defer tbl2.Close()
	err = ls.Add(tbl2)
	require.NoError(t, err)
	assert.Equal(t, tbl1.Size()+tbl2.Size(), ls.TotalSize(), "Total size should be the sum of both tables")

	// Remove a table
	err = ls.Remove(tbl1.ID())
	require.NoError(t, err)
	assert.Equal(t, tbl2.Size(), ls.TotalSize(), "Total size should be updated after removal")

	// Set tables
	tbl3 := newTestSSTable(t, 3, []struct{ key, value []byte }{{[]byte("c"), makeValue(300)}})
	defer tbl3.Close()
	ls.SetTables([]*sstable.SSTable{tbl3})
	assert.Equal(t, tbl3.Size(), ls.TotalSize(), "Total size should be updated after SetTables")
}
