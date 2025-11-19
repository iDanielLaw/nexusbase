package wal

import (
	"errors"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test that a single WAL record larger than MaxSegmentSize is rejected when
// attempting to write into an empty segment.
func TestWAL_OversizedSingleRecord_Rejected(t *testing.T) {
	tempDir := t.TempDir()
	opts := testWALOptions(t, tempDir)
	// Very small max segment size to force the oversized condition
	opts.MaxSegmentSize = 128

	wal, _, err := Open(opts)
	require.NoError(t, err)

	// Create a single oversized entry (key+value together exceed MaxSegmentSize)
	key := make([]byte, 200)
	value := make([]byte, 200)
	for i := range key {
		key[i] = 'k'
	}
	for i := range value {
		value[i] = 'v'
	}

	entry := core.WALEntry{
		EntryType: core.EntryTypePutEvent,
		Key:       key,
		Value:     value,
		SeqNum:    1,
	}

	// Append should return an error indicating the record is too large.
	err = wal.Append(entry)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrRecordTooLarge), "error should be core.ErrRecordTooLarge")

	// Close and verify nothing was recovered
	require.NoError(t, wal.Close())
	wal2, recovered, err := Open(opts)
	require.NoError(t, err)
	defer wal2.Close()
	assert.Len(t, recovered, 0, "No entries should be recovered since append failed")
}
