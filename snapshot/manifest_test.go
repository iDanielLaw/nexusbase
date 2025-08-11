package snapshot

import (
	"bytes"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManifest_RoundTrip_Full(t *testing.T) {
	// 1. Create a comprehensive manifest
	manifest := &core.SnapshotManifest{
		SequenceNumber: 12345,
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
	}

	// 2. Serialize to buffer
	var buf bytes.Buffer
	err := WriteManifestBinary(&buf, manifest)
	require.NoError(t, err)
	require.NotEmpty(t, buf.Bytes(), "Serialized buffer should not be empty")

	// 3. Deserialize from buffer
	deserializedManifest, err := ReadManifestBinary(&buf)
	require.NoError(t, err)
	require.NotNil(t, deserializedManifest)

	// 4. Compare
	assert.Equal(t, manifest, deserializedManifest, "Original and deserialized manifests should be identical")
}

func TestManifest_RoundTrip_EmptyAndNil(t *testing.T) {
	testCases := []struct {
		name     string
		manifest *core.SnapshotManifest
	}{
		{
			name: "Empty Manifest",
			manifest: &core.SnapshotManifest{
				SequenceNumber: 1,
				Levels:         []core.SnapshotLevelManifest{},
			},
		},
		{
			name: "Nil Levels Slice",
			manifest: &core.SnapshotManifest{
				SequenceNumber: 2,
				Levels:         nil,
			},
		},
		{
			name: "Level with empty tables slice",
			manifest: &core.SnapshotManifest{
				SequenceNumber: 3,
				Levels: []core.SnapshotLevelManifest{
					{LevelNumber: 0, Tables: []core.SSTableMetadata{}},
				},
			},
		},
		{
			name: "Table with nil keys",
			manifest: &core.SnapshotManifest{
				SequenceNumber: 4,
				Levels: []core.SnapshotLevelManifest{
					{
						LevelNumber: 0,
						Tables: []core.SSTableMetadata{
							{ID: 1, FileName: "1.sst", MinKey: nil, MaxKey: nil},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteManifestBinary(&buf, tc.manifest)
			require.NoError(t, err)

			deserialized, err := ReadManifestBinary(&buf)
			require.NoError(t, err)

			// Special handling for nil vs empty slices for DeepEqual
			if tc.manifest.Levels == nil {
				tc.manifest.Levels = []core.SnapshotLevelManifest{}
			}
			if deserialized.Levels == nil {
				deserialized.Levels = []core.SnapshotLevelManifest{}
			}

			assert.Equal(t, tc.manifest, deserialized)
		})
	}
}

func TestReadManifestBinary_ErrorCases(t *testing.T) {
	// Create a valid manifest to start with
	validManifest := &core.SnapshotManifest{
		SequenceNumber: 1,
		Levels: []core.SnapshotLevelManifest{
			{LevelNumber: 0, Tables: []core.SSTableMetadata{{ID: 1, FileName: "1.sst"}}},
		},
	}
	var validBuf bytes.Buffer
	err := WriteManifestBinary(&validBuf, validManifest)
	require.NoError(t, err)
	validBytes := validBuf.Bytes()

	testCases := []struct {
		name       string
		data       []byte
		errContain string
	}{
		{
			name:       "Empty data",
			data:       []byte{},
			errContain: "EOF",
		},
		{
			name:       "Truncated header",
			data:       validBytes[:5],
			errContain: "failed to read manifest header",
		},
		{
			name: "Invalid magic number",
			data: func() []byte {
				d := make([]byte, len(validBytes))
				copy(d, validBytes)
				d[0] = 0xDE
				d[1] = 0xAD
				return d
			}(),
			errContain: "invalid binary manifest magic number",
		},
		{
			name:       "Truncated after header",
			data:       validBytes[:14], // Header is 14 bytes
			errContain: "failed to read sequence number",
		},
		{
			name:       "Truncated in levels loop",
			data:       validBytes[:32], // Truncate in the middle of reading the table count for the first level
			errContain: "failed to read table count",
		},
		{
			name: "Truncated string length",
			data: func() []byte {
				// This is tricky to construct manually. We serialize a manifest
				// with a non-empty final string field, and then truncate the
				// last byte of that string's data.
				m := &core.SnapshotManifest{SequenceNumber: 1, SSTableCompression: "snappy"} //nolint:govet
				var b bytes.Buffer
				WriteManifestBinary(&b, m)
				// Truncate the last byte of the "snappy" string data
				return b.Bytes()[:b.Len()-1]
			}(),
			errContain: "failed to read string data",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadManifestBinary(bytes.NewReader(tc.data))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errContain)
		})
	}
}

func TestStringBytesWithLength_RoundTrip(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		testStrings := []string{"hello", "", "a string with spaces and symbols !@#"}
		for _, s := range testStrings {
			var buf bytes.Buffer
			err := writeStringWithLength(&buf, s)
			require.NoError(t, err)

			readStr, err := readStringWithLength(&buf)
			require.NoError(t, err)
			assert.Equal(t, s, readStr)
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		testBytes := [][]byte{[]byte("hello"), {}, nil, []byte{0, 1, 2, 3, 4, 5}}
		for _, b := range testBytes {
			var buf bytes.Buffer
			err := writeBytesWithLength(&buf, b)
			require.NoError(t, err)

			readB, err := readBytesWithLength(&buf)
			require.NoError(t, err)
			// For comparison, treat nil and empty slice as the same
			if b == nil {
				b = []byte{}
			}
			if readB == nil {
				readB = []byte{}
			}
			assert.Equal(t, b, readB)
		}
	})
}
