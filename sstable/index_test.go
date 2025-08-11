package sstable

import (
	"hash/crc32"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestIndexBuilder_Build(t *testing.T) {
	builder := &IndexBuilder{}

	// Add some entries
	entries := []BlockIndexEntry{
		{FirstKey: []byte("key01"), BlockOffset: 0, BlockLength: 1024},
		{FirstKey: []byte("key50"), BlockOffset: 1024, BlockLength: 2048},
		{FirstKey: []byte("key99"), BlockOffset: 3072, BlockLength: 512},
	}

	for _, entry := range entries {
		builder.Add(entry.FirstKey, entry.BlockOffset, entry.BlockLength)
	}

	// Build the index
	indexData, checksum, err := builder.Build()

	// Verification
	require.NoError(t, err, "Build should not return an error")
	require.NotEmpty(t, indexData, "Serialized index data should not be empty")

	// Verify checksum
	expectedChecksum := crc32.ChecksumIEEE(indexData)
	assert.Equal(t, expectedChecksum, checksum, "Checksum should match the data")

	// Verify content by manually deserializing (or using DeserializeIndex)
	deserializedIndex, err := DeserializeIndex(indexData, checksum, noop.NewTracerProvider().Tracer("test"), slog.Default())
	require.NoError(t, err, "DeserializeIndex should succeed with built data")
	require.NotNil(t, deserializedIndex)

	assert.Equal(t, len(entries), len(deserializedIndex.entries), "Deserialized index should have the same number of entries")

	for i, expected := range entries {
		actual := deserializedIndex.entries[i]
		assert.Equal(t, expected.FirstKey, actual.FirstKey, "FirstKey mismatch at index %d", i)
		assert.Equal(t, expected.BlockOffset, actual.BlockOffset, "BlockOffset mismatch at index %d", i)
		assert.Equal(t, expected.BlockLength, actual.BlockLength, "BlockLength mismatch at index %d", i)
	}
}

func TestDeserializeIndex(t *testing.T) {
	// 1. Setup: Create valid index data using the builder
	builder := &IndexBuilder{}
	builder.Add([]byte("apple"), 0, 100)
	builder.Add([]byte("banana"), 100, 150)
	validData, validChecksum, err := builder.Build()
	require.NoError(t, err)

	tracer := noop.NewTracerProvider().Tracer("test")
	logger := slog.Default()

	t.Run("ValidData", func(t *testing.T) {
		idx, err := DeserializeIndex(validData, validChecksum, tracer, logger)
		require.NoError(t, err, "Deserializing valid data should not produce an error")
		require.NotNil(t, idx)
		assert.Len(t, idx.entries, 2, "Should have 2 entries")
		assert.Equal(t, []byte("apple"), idx.entries[0].FirstKey)
		assert.Equal(t, int64(100), idx.entries[1].BlockOffset)
	})

	t.Run("CorruptedChecksum", func(t *testing.T) {
		_, err := DeserializeIndex(validData, validChecksum+1, tracer, logger)
		require.Error(t, err, "Should return an error for checksum mismatch")
		assert.ErrorIs(t, err, ErrCorrupted, "Error should be of type ErrCorrupted")
		assert.Contains(t, err.Error(), "index checksum mismatch")
	})

	t.Run("TruncatedData", func(t *testing.T) {
		if len(validData) < 5 {
			t.Skip("Valid data too short to truncate for test")
		}
		truncatedData := validData[:len(validData)-5]
		// The checksum will now be incorrect, but the primary error should be parsing
		_, err := DeserializeIndex(truncatedData, validChecksum, tracer, logger)
		require.Error(t, err, "Should return an error for truncated data")
		// The specific error might be about checksum or parsing, either is acceptable
		// as it indicates corruption.
	})

	t.Run("EmptyData", func(t *testing.T) {
		// Build an empty index
		emptyBuilder := &IndexBuilder{}
		emptyData, emptyChecksum, err := emptyBuilder.Build()
		require.NoError(t, err)
		assert.Empty(t, emptyData)
		assert.Equal(t, uint32(0), emptyChecksum)

		// Deserialize the empty index
		idx, err := DeserializeIndex(emptyData, emptyChecksum, tracer, logger)
		require.NoError(t, err, "Deserializing empty data should not produce an error")
		require.NotNil(t, idx)
		assert.Empty(t, idx.entries, "Deserialized index from empty data should have no entries")
	})

	t.Run("DataWithExtraBytes", func(t *testing.T) {
		// This test verifies that if the provided data slice is longer than the
		// actual serialized entries, the function detects this as corruption and fails,
		// as it will try to parse the extra bytes as a new entry.

		// 1. Create data with extra bytes at the end
		extraData := append(validData, []byte("some extra garbage bytes")...)

		// 2. Calculate the checksum for the corrupted data so we can pass the initial check
		corruptedChecksum := crc32.ChecksumIEEE(extraData)

		// 3. Attempt to deserialize. This should fail during parsing.
		_, err := DeserializeIndex(extraData, corruptedChecksum, tracer, logger)
		require.Error(t, err, "DeserializeIndex should fail when data contains extra bytes")
		assert.Contains(t, err.Error(), "exceeds data bounds", "Error message should indicate a parsing failure due to extra data")
	})
}
