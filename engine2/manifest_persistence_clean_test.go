package engine2

import (
	"bytes"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/snapshot"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestBinary_RoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond)

	manifest := &core.SnapshotManifest{
		Type:                core.SnapshotTypeFull,
		ParentID:            "",
		CreatedAt:           now,
		LastWALSegmentIndex: 5,
		SequenceNumber:      12345,
		Levels: []core.SnapshotLevelManifest{
			{
				LevelNumber: 0,
				Tables: []core.SSTableMetadata{
					{ID: 1, FileName: "sst/1.sst", MinKey: []byte("a"), MaxKey: []byte("c")},
				},
			},
		},
		WALFile:            "wal",
		SSTableCompression: "snappy",
	}

	var buf bytes.Buffer
	err := snapshot.WriteManifestBinary(&buf, manifest)
	require.NoError(t, err)

	got, err := snapshot.ReadManifestBinary(&buf)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.True(t, manifest.CreatedAt.Equal(got.CreatedAt))
	manifest.CreatedAt = time.Time{}
	got.CreatedAt = time.Time{}
	assert.Equal(t, manifest, got)
}

func TestReadManifestBinary_ErrorCases(t *testing.T) {
	valid := &core.SnapshotManifest{Type: core.SnapshotTypeFull}
	var buf bytes.Buffer
	err := snapshot.WriteManifestBinary(&buf, valid)
	require.NoError(t, err)
	data := buf.Bytes()

	// Truncated header
	_, err = snapshot.ReadManifestBinary(bytes.NewReader(data[:1]))
	require.Error(t, err)
}
