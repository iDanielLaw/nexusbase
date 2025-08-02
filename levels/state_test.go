package levels

import (
	"testing"

	"github.com/INLOpen/nexusbase/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper to create a new LevelState for testing.
func newLevelStateForTest(levelNumber int) *LevelState {
	return &LevelState{
		levelNumber: levelNumber,
		tables:      make([]*sstable.SSTable, 0),
		tableMap:    make(map[uint64]*sstable.SSTable),
	}
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
