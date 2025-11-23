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
				ParentID:            "",
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
				DeletedSeriesFile:   "deleted_series.bin",
				RangeTombstonesFile: "range_tombstones.bin",
				StringMappingFile:   "string_mapping.log",
				SeriesMappingFile:   "series_mapping.log",
				SSTableCompression:  "snappy",
			},
		},
		{
			name: "Incremental Snapshot Manifest",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeIncremental,
				ParentID:            "MANIFEST_parent_123.bin",
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
				Levels:              nil,
			},
		},
		{
			name: "Manifest with Empty Levels",
			manifest: &core.SnapshotManifest{
				Type:                core.SnapshotTypeFull,
				CreatedAt:           now,
				LastWALSegmentIndex: 2,
				SequenceNumber:      2,
				Levels:              []core.SnapshotLevelManifest{},
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
			var buf bytes.Buffer
			err := snapshot.WriteManifestBinary(&buf, tc.manifest)
			require.NoError(t, err)
			require.NotEmpty(t, buf.Bytes(), "Serialized buffer should not be empty")

			deserializedManifest, err := snapshot.ReadManifestBinary(&buf)
			require.NoError(t, err)
			require.NotNil(t, deserializedManifest)

			if tc.manifest.Levels == nil {
				tc.manifest.Levels = []core.SnapshotLevelManifest{}
			}
			if deserializedManifest.Levels == nil {
				deserializedManifest.Levels = []core.SnapshotLevelManifest{}
			}

			assert.True(t, tc.manifest.CreatedAt.Equal(deserializedManifest.CreatedAt), "CreatedAt timestamp mismatch")
			tc.manifest.CreatedAt = time.Time{}
			deserializedManifest.CreatedAt = time.Time{}

			assert.Equal(t, tc.manifest, deserializedManifest, "Original and deserialized manifests should be identical")
		})
	}
}

func TestReadManifestBinary_ErrorCases(t *testing.T) {
	validManifest := &core.SnapshotManifest{
		Type:                core.SnapshotTypeFull,
		ParentID:            "",
		CreatedAt:           time.Time{},
		LastWALSegmentIndex: 0,
		SequenceNumber:      1,
		Levels:              []core.SnapshotLevelManifest{},
	}
	var validBuf bytes.Buffer
	err := snapshot.WriteManifestBinary(&validBuf, validManifest)
	require.NoError(t, err)
	validBytes := validBuf.Bytes()

	const (
		headerEnd    = 14
		versionLen   = 2
		typeEnd      = headerEnd + versionLen + 2 + 4
		parentIDEnd  = typeEnd + 2
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
		{"Truncated header", append([]byte{}, validBytes[:headerEnd-1]...), "failed to read manifest header"},
		{"Invalid magic number", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, "invalid binary manifest magic number"},
		{"Truncated in Type read", append([]byte{}, validBytes[:headerEnd+versionLen+1]...), "failed to read snapshot type"},
		{"Truncated in ParentID read", append([]byte{}, validBytes[:typeEnd+1]...), "failed to read parent ID"},
		{"Truncated in CreatedAt read", append([]byte{}, validBytes[:parentIDEnd+1]...), "failed to read CreatedAt timestamp"},
		{"Truncated in LastWALSegmentIndex read", append([]byte{}, validBytes[:createdAtEnd+1]...), "failed to read last WAL segment index"},
		{"Truncated in SequenceNumber read", append([]byte{}, validBytes[:lastWALEnd+1]...), "failed to read sequence number"},
		{"Truncated in numLevels read", append([]byte{}, validBytes[:seqNumEnd+1]...), "failed to read number of levels"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := snapshot.ReadManifestBinary(bytes.NewReader(tc.data))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContain)
		})
	}
}
