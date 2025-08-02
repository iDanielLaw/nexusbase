package indexer

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestStringStore is a test helper to create a new, initialized StringStore.
func newTestStringStore(t *testing.T) (*StringStore, string) {
	t.Helper()
	dataDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := NewStringStore(logger, nil)
	err := store.LoadFromFile(dataDir)
	require.NoError(t, err, "LoadFromFile on a new store should succeed")
	return store, dataDir
}

func TestStringStore_Roundtrip(t *testing.T) {
	store1, dataDir := newTestStringStore(t)

	// Create some strings
	id1, err1 := store1.GetOrCreateID("cpu.usage")
	id2, err2 := store1.GetOrCreateID("host")
	id3, err3 := store1.GetOrCreateID("server-1")
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	// Get an existing string to ensure it returns the same ID
	id1_again, err_again := store1.GetOrCreateID("cpu.usage")
	require.NoError(t, err_again)

	assert.Equal(t, uint64(1), id1, "Expected first ID to be 1")
	assert.Equal(t, uint64(2), id2, "Expected second ID to be 2")
	assert.Equal(t, uint64(3), id3, "Expected third ID to be 3")
	assert.Equal(t, id1, id1_again, "Expected getting an existing string to return the same ID")

	// Close the store, which should persist everything to disk.
	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed for store1: %v", err)
	}

	// Phase 2: Create a new store and recover from the same directory
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store2 := NewStringStore(logger, nil)
	err := store2.LoadFromFile(dataDir)
	require.NoError(t, err, "LoadFromFile on the second store should succeed")
	defer store2.Close()

	// Phase 3: Verify the loaded data
	// Check that existing strings have the correct IDs
	loaded_id1, ok1 := store2.GetID("cpu.usage")
	loaded_id2, ok2 := store2.GetID("host")
	loaded_id3, ok3 := store2.GetID("server-1")
	require.True(t, ok1 && ok2 && ok3, "All strings should be found in the loaded store")
	assert.Equal(t, id1, loaded_id1)
	assert.Equal(t, id2, loaded_id2)
	assert.Equal(t, id3, loaded_id3)

	// Check reverse mapping
	str1, ok_str1 := store2.GetString(id1)
	assert.True(t, ok_str1)
	assert.Equal(t, "cpu.usage", str1)

	// Check that creating a new string gets the next sequential ID
	id4, err4 := store2.GetOrCreateID("new.string")
	require.NoError(t, err4)
	assert.Equal(t, uint64(4), id4, "Expected next ID to be 4")
}

func TestStringStore_GetOrCreateID_Concurrent(t *testing.T) {
	store, _ := newTestStringStore(t)
	defer store.Close()

	var wg sync.WaitGroup
	numGoroutines := 50
	keys := []string{"key_alpha", "key_beta", "key_gamma"}

	// A map to store results from each goroutine to check for consistency
	results := make(map[string][]uint64)
	var resultsMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, key := range keys {
				id, err := store.GetOrCreateID(key)
				require.NoError(t, err)

				t.Logf("Goroutine %d got ID %d for key %s", i, id, key)

				resultsMu.Lock()
				if _, ok := results[key]; !ok {
					results[key] = make([]uint64, 0)
				}
				results[key] = append(results[key], id)
				resultsMu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Verification
	// 1. Check that for each key, all goroutines received the same ID.
	for key, ids := range results {
		require.NotEmpty(t, ids, "Should have results for key %s", key)
		firstID := ids[0]
		for _, id := range ids {
			assert.Equal(t, firstID, id, "All goroutines should get the same ID for key %s", key)
		}
	}

	// 2. Check that the final nextID is correct.
	// Since there are 3 unique keys, the next ID to be generated should be 3.
	// FIX: After creating 3 keys (1, 2, 3), the next available ID should be 4.
	nextId := store.nextID.Load()
	assert.Equal(t, uint64(4), nextId, "Expected nextID counter to be 4")
}

func TestStringStore_Recovery(t *testing.T) {
	tempDir := t.TempDir()

	// Phase 1: Create and populate a store
	store1 := NewStringStore(slog.Default(), nil)
	if err := store1.LoadFromFile(tempDir); err != nil {
		t.Fatalf("Recover failed for store1: %v", err)
	}

	testVals := []struct {
		str         string
		expected_id uint64
	}{
		{"metric1", 1},
		{"tagKey1", 2},
		{"tagValue1", 3},
	}
	for _, val := range testVals {
		id, err := store1.GetOrCreateID(val.str)
		if err != nil {
			t.Fatalf("GetOrCreateID failed for '%s': %v", val.str, err)
		}
		assert.Equal(t, val.expected_id, id)
	}

	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed for store1: %v", err)
	}

	// Phase 2: Create a new store and recover from the same directory
	store2 := NewStringStore(slog.Default(), nil)
	if err := store2.LoadFromFile(tempDir); err != nil {
		t.Fatalf("Recover failed for store2: %v", err)
	}
	defer store2.Close()

	// Verify recovered data
	id, ok := store2.GetID("tagKey1")
	assert.True(t, ok, "Expected to find 'tagKey1'")
	assert.Equal(t, uint64(2), id)

	str, ok := store2.GetString(3)
	assert.True(t, ok, "Expected to find ID 3")
	assert.Equal(t, "tagValue1", str)

	// Verify nextID is correct
	nextID := store2.nextID.Load()
	assert.Equal(t, uint64(4), nextID, "Expected nextID to be 4 after recovery")
}
