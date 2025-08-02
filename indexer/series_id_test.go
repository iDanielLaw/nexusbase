package indexer

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestSeriesIDStore is a test helper to create a new, initialized SeriesIDStore.
func newTestSeriesIDStore(t *testing.T, dataDir string) *SeriesIDStore {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := NewSeriesIDStore(logger, nil)
	err := store.LoadFromFile(dataDir)
	require.NoError(t, err, "LoadFromFile on a new store should succeed")
	return store
}

func TestSeriesIDStore_Roundtrip(t *testing.T) {
	dataDir := t.TempDir()

	// --- Phase 1: Create, populate, and close the store ---
	store1 := newTestSeriesIDStore(t, dataDir)

	// Create some series
	id1, err1 := store1.GetOrCreateID("series_A")
	id2, err2 := store1.GetOrCreateID("series_B")
	id3, err3 := store1.GetOrCreateID("series_C")
	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NoError(t, err3)

	// Get an existing series to ensure it returns the same ID
	id1_again, err_again := store1.GetOrCreateID("series_A")
	require.NoError(t, err_again)

	assert.Equal(t, uint64(1), id1, "Expected first ID to be 1")
	assert.Equal(t, uint64(2), id2, "Expected second ID to be 2")
	assert.Equal(t, uint64(3), id3, "Expected third ID to be 3")
	assert.Equal(t, id1, id1_again, "Expected getting an existing key to return the same ID")

	// Close the store, which should persist everything to disk.
	err := store1.Close()
	require.NoError(t, err, "Closing the store should not fail")

	// --- Phase 2: Create a new store instance and load from the same file ---
	store2 := newTestSeriesIDStore(t, dataDir)
	defer store2.Close()

	// --- Phase 3: Verify the loaded data ---
	// Check that existing keys have the correct IDs
	loaded_id1, ok1 := store2.GetID("series_A")
	loaded_id2, ok2 := store2.GetID("series_B")
	loaded_id3, ok3 := store2.GetID("series_C")
	require.True(t, ok1 && ok2 && ok3, "All keys should be found in the loaded store")
	assert.Equal(t, id1, loaded_id1)
	assert.Equal(t, id2, loaded_id2)
	assert.Equal(t, id3, loaded_id3)

	// Check reverse mapping
	key1, ok_key1 := store2.GetKey(id1)
	assert.True(t, ok_key1)
	assert.Equal(t, "series_A", key1)

	// Check that creating a new key gets the next sequential ID
	id4, err4 := store2.GetOrCreateID("series_D")
	require.NoError(t, err4)
	assert.Equal(t, uint64(4), id4, "Expected next ID to be 4")
}

func TestSeriesIDStore_GetOrCreateID_Concurrent(t *testing.T) {
	dataDir := t.TempDir()
	store := newTestSeriesIDStore(t, dataDir)
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
	// Since there are 3 unique keys, the next ID to be generated should be 4.
	// The internal atomic counter `nextID` would be 3.
	assert.Equal(t, uint64(4), store.nextID.Load(), "Expected nextID counter to be 4")
}

func TestSeriesIDStore_LoadFromFile_CorruptedRecord(t *testing.T) {
	dataDir := t.TempDir()

	// --- Phase 1: Create a file with one valid record and one corrupted one ---
	store1 := newTestSeriesIDStore(t, dataDir)
	_, err := store1.GetOrCreateID("valid_key") // This creates the first valid record
	require.NoError(t, err)

	// Manually append a corrupted record (bad checksum)
	var dataBuf bytes.Buffer
	binary.Write(&dataBuf, binary.BigEndian, uint64(99))
	binary.Write(&dataBuf, binary.BigEndian, uint16(len("corrupted_key")))
	dataBuf.WriteString("corrupted_key")
	dataBytes := dataBuf.Bytes()
	recordLen := uint32(len(dataBytes))
	badChecksum := crc32.ChecksumIEEE(dataBytes) + 1 // Mismatch checksum

	var recordBuf bytes.Buffer
	binary.Write(&recordBuf, binary.BigEndian, recordLen)
	recordBuf.Write(dataBytes)
	binary.Write(&recordBuf, binary.BigEndian, badChecksum)

	_, err = store1.logFile.Write(recordBuf.Bytes())
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	// --- Phase 2: Attempt to load the corrupted file ---
	store2 := NewSeriesIDStore(slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	err = store2.LoadFromFile(dataDir)

	// --- Phase 3: Verification ---
	require.Error(t, err, "Expected an error when loading a corrupted file")
	assert.Contains(t, err.Error(), "checksum mismatch", "Error message should indicate a checksum mismatch")
}
