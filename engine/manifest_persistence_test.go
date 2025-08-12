package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifestBinary_RoundTrip(t *testing.T) {
	// Use Truncate to avoid potential precision issues between systems when using UnixNano
	now := time.Now().UTC().Truncate(time.Microsecond)

	testCases := []struct {
		name     string
		manifest *core.SnapshotManifest
	}{
		{
			name: "Full Snapshot Manifest",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeFull,
				ParentID:            "", // ParentID is empty for full snapshots
				CreatedAt:           now,
				LastWALSegmentIndex: 5,
				SequenceNumber:      12345,
				Levels: []core.SnapshotLevelManifest{
					{
						LevelNumber: 0,
						Tables: []core.SSTableMetadata{
							{ID: 1, FileName: "sst/1.sst", MinKey: []byte("a"), MaxKey: []byte("c")},
							{ID: 2, FileName: "sst/2.sst", MinKey: []byte("d"), MaxKey: []byte("f")},
						},
					},
					{
						LevelNumber: 3,
						Tables: []core.SSTableMetadata{
							{ID: 10, FileName: "sst/10.sst", MinKey: []byte("g"), MaxKey: []byte("z")},
						},
					},
				},
				WALFile:             "wal",
				DeletedSeriesFile:   "deleted_series.json",
				RangeTombstonesFile: "range_tombstones.json",
				StringMappingFile:   "string_mapping.log",
				SeriesMappingFile:   "series_mapping.log",
				SSTableCompression:  "snappy",
			},
		},
		{
			name: "Incremental Snapshot Manifest",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeIncremental,
				ParentID:            "MANIFEST_parent_123.bin", // ParentID is set for incremental
				CreatedAt:           now.Add(time.Hour),
				LastWALSegmentIndex: 8,
				SequenceNumber:      54321,
				Levels: []core.SnapshotLevelManifest{
					{
						LevelNumber: 0,
						Tables: []core.SSTableMetadata{
							{ID: 15, FileName: "sst/15.sst", MinKey: []byte("h"), MaxKey: []byte("k")},
						},
					},
				},
				SSTableCompression: "zstd",
			},
		},
		{
			name: "Empty Manifest with Nil Slices",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeFull,
				CreatedAt:           now,
				LastWALSegmentIndex: 1,
				SequenceNumber:      1,
				Levels:              nil, // Test nil slice
			},
		},
		{
			name: "Manifest with Empty Levels",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeFull,
				CreatedAt:           now,
				LastWALSegmentIndex: 2,
				SequenceNumber:      2,
				Levels:              []core.SnapshotLevelManifest{}, // Test empty slice
			},
		},
		{
			name: "Manifest with Level with Empty Tables",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeFull,
				CreatedAt:           now,
				LastWALSegmentIndex: 3,
				SequenceNumber:      3,
				Levels: []core.SnapshotLevelManifest{
					{LevelNumber: 0, Tables: []core.SSTableMetadata{}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 1. Serialize to buffer
			var buf bytes.Buffer
			err := writeManifestBinary(&buf, tc.manifest)
			require.NoError(t, err)
			require.NotEmpty(t, buf.Bytes(), "Serialized buffer should not be empty")

			// 2. Deserialize from buffer
			deserializedManifest, err := readManifestBinary(&buf)
			require.NoError(t, err)
			require.NotNil(t, deserializedManifest)

			// 3. Compare
			// Special handling for nil vs empty slices for assert.Equal
			if tc.manifest.Levels == nil {
				tc.manifest.Levels = []core.SnapshotLevelManifest{}
			}
			if deserializedManifest.Levels == nil {
				deserializedManifest.Levels = []core.SnapshotLevelManifest{}
			}

			// Time comparison needs Equal() method
			assert.True(t, tc.manifest.CreatedAt.Equal(deserializedManifest.CreatedAt), "CreatedAt timestamp mismatch")
			// Set CreatedAt to zero for the final comparison to avoid issues with deep equality checks on time.Time
			tc.manifest.CreatedAt = time.Time{}
			deserializedManifest.CreatedAt = time.Time{}

			assert.Equal(t, tc.manifest, deserializedManifest, "Original and deserialized manifests should be identical")
		})
	}
}

func TestReadManifestBinary_ErrorCases(t *testing.T) {
	// Create a valid manifest to start with
	// Make it explicit to ensure the binary format is consistent for the test.
	validManifest := &core.SnapshotManifest{
		Type:                core.SnapshotTypeFull,
		ParentID:            "",
		CreatedAt:           time.Time{},
		LastWALSegmentIndex: 0,
		SequenceNumber:      1,
		Levels:              []core.SnapshotLevelManifest{}, // Use empty levels for simplicity
	}
	var validBuf bytes.Buffer
	err := writeManifestBinary(&validBuf, validManifest)
	require.NoError(t, err)
	validBytes := validBuf.Bytes()

	// Calculate offsets for creating truncated data slices.
	const (
		headerEnd    = 14                // Correct header size is 14 bytes.
		typeEnd      = headerEnd + 2 + 4 // "full" is 4 bytes + 2 for length
		parentIDEnd  = typeEnd + 2       // "" is 0 bytes + 2 for length
		createdAtEnd = parentIDEnd + 8
		lastWALEnd   = createdAtEnd + 8
		seqNumEnd    = lastWALEnd + 8
	)

	testCases := []struct {
		name       string
		data       []byte
		errContain string
	}{
		{"Empty data", []byte{}, "EOF"},
		{"Truncated header", validBytes[:headerEnd-1], "failed to read manifest header"}, // 13 bytes, should fail header read
		{"Invalid magic number", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "invalid binary manifest magic number"},
		{"Truncated in Type read", validBytes[:headerEnd+1], "failed to read snapshot type"},
		{"Truncated in ParentID read", validBytes[:typeEnd+1], "failed to read parent ID"},
		{"Truncated in CreatedAt read", validBytes[:parentIDEnd+1], "failed to read CreatedAt timestamp"},
		{"Truncated in LastWALSegmentIndex read", validBytes[:createdAtEnd+1], "failed to read last WAL segment index"},
		{"Truncated in SequenceNumber read", validBytes[:lastWALEnd+1], "failed to read sequence number"},
		{"Truncated in numLevels read", validBytes[:seqNumEnd+1], "failed to read number of levels"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := readManifestBinary(bytes.NewReader(tc.data))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContain)
		})
	}
}
