package engine

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"sort"
	"testing"

	"github.com/INLOpen/nexusbase/compressors"
	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/INLOpen/nexusbase/wal"
	"github.com/stretchr/testify/require"
)

// getBaseOptsForFlushTest returns a standard set of options for various tests.
// It creates a new temp directory for each call to ensure test isolation.
func getBaseOptsForFlushTest(t *testing.T) StorageEngineOptions {
	t.Helper()
	return StorageEngineOptions{
		DataDir:                      t.TempDir(),
		MemtableThreshold:            1024 * 1024,
		IndexMemtableThreshold:       1024 * 1024,
		BlockCacheCapacity:           100,
		MaxL0Files:                   4,
		TargetSSTableSize:            1024 * 1024,
		LevelsTargetSizeMultiplier:   2,
		MaxLevels:                    3,
		BloomFilterFalsePositiveRate: 0.01,
		SSTableDefaultBlockSize:      sstable.DefaultBlockSize,
		SSTableCompressor:            &compressors.NoCompressionCompressor{},
		WALSyncMode:                  wal.SyncDisabled, // Faster for tests
		CompactionIntervalSeconds:    3600,             // Disable auto-compaction for most tests
		Metrics:                      NewEngineMetrics(false, "test_"),
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Clock:                        &utils.SystemClock{},
		SelfMonitoringEnabled:        true,
		SelfMonitoringPrefix:         "__",
		SelfMonitoringIntervalMs:     1000,
	}
}

// --- General Test Helpers ---

// HelperDataPoint creates a core.DataPoint and fails the test on error.
func HelperDataPoint(t *testing.T, metric string, tags map[string]string, ts int64, fields map[string]any) core.DataPoint {
	t.Helper()
	dp, err := MakeDataPoint(metric, tags, ts, fields)
	if err != nil {
		t.Fatalf("Failed to create data point: %v", err)
	}
	return dp
}

// MakeDataPoint creates a core.DataPoint and returns an error if it fails.
func MakeDataPoint(metric string, tags map[string]string, ts int64, fields map[string]any) (core.DataPoint, error) {
	dp, err := core.NewSimpleDataPoint(metric, tags, ts, fields)
	if err != nil {
		return core.DataPoint{}, fmt.Errorf("Failed to create data point: %v", err)
	}
	return *dp, nil
}

// HelperFieldValueValidate retrieves a field from FieldValues and fails the test if not found.
func HelperFieldValueValidate(fields core.FieldValues, selected string) (*core.PointValue, error) {
	if len(fields) < 1 {
		return nil, fmt.Errorf("Fields has no values")
	}

	if val, ok := fields[selected]; !ok {
		return nil, fmt.Errorf("Field %s not found in fields", selected)
	} else {
		return &val, nil
	}
}

// HelperFieldValueValidateFloat64 retrieves a float64 field and fails the test if not found or wrong type.
func HelperFieldValueValidateFloat64(t *testing.T, fields core.FieldValues, selected string) float64 {
	t.Helper()
	val, err := HelperFieldValueValidate(fields, selected)
	if err != nil {
		t.Fatalf("Failed to validate field %s: %v", selected, err)
		return 0
	}
	if v, ok := val.ValueFloat64(); !ok {
		t.Fatalf("Value for field %s is not float64", selected)
		return 0
	} else {
		return v
	}
}

// HelperFieldValueValidateInt64 retrieves an int64 field and fails the test if not found or wrong type.
func HelperFieldValueValidateInt64(t *testing.T, fields core.FieldValues, selected string) int64 {
	t.Helper()
	val, err := HelperFieldValueValidate(fields, selected)
	if err != nil {
		t.Fatalf("Failed to validate field %s: %v", selected, err)
		return 0
	}
	if v, ok := val.ValueInt64(); !ok {
		t.Fatalf("Value for field %s is not int64", selected)
		return 0
	} else {
		return v
	}
}

// HelperFieldValueValidateString retrieves a string field and fails the test if not found or wrong type.
func HelperFieldValueValidateString(t *testing.T, fields core.FieldValues, selected string) string {
	t.Helper()
	val, err := HelperFieldValueValidate(fields, selected)
	if err != nil {
		t.Fatalf("Failed to validate field %s: %v", selected, err)
		return ""
	}
	if v, ok := val.ValueString(); !ok {
		t.Fatalf("Value for field %s is not string", selected)
		return ""
	} else {
		return v
	}
}

// HelperFieldValueValidateBool retrieves a bool field and fails the test if not found or wrong type.
func HelperFieldValueValidateBool(t *testing.T, fields core.FieldValues, selected string) bool {
	t.Helper()
	val, err := HelperFieldValueValidate(fields, selected)
	if err != nil {
		t.Fatalf("Failed to validate field %s: %v", selected, err)
		return false
	}
	if v, ok := val.ValueBool(); !ok {
		t.Fatalf("Value for field %s is not bool", selected)
		return false
	} else {
		return v
	}
}

// HelperFieldValueValidateNullable checks if a field is null or does not exist.
func HelperFieldValueValidateNullable(t *testing.T, fields core.FieldValues, selected string) bool {
	t.Helper()
	if val, ok := fields[selected]; !ok || val.IsNull() {
		return true
	} else {
		return false
	}
}

// compareFloatMaps is a helper to compare two maps of string to float64, with tolerance for NaN.
func compareFloatMaps(t *testing.T, actual, expected map[string]float64, windowIndex int) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("Window %d: map length mismatch:\nGot:    %v (%d entries)\nWanted: %v (%d entries)", windowIndex, actual, len(actual), expected, len(expected))
		return
	}

	for key, expectedVal := range expected {
		actualVal, ok := actual[key]
		if !ok {
			t.Errorf("Window %d: Expected key %q not found in result map", windowIndex, key)
			continue
		}
		// Check for NaN equality
		if math.IsNaN(expectedVal) {
			if !math.IsNaN(actualVal) {
				t.Errorf("Window %d: For key %q, expected NaN, got %f", windowIndex, key, actualVal)
			}
		} else if math.Abs(actualVal-expectedVal) > 1e-9 { // Check for regular float equality with tolerance
			t.Errorf("Window %d: For key %q, value mismatch: got %f, want %f", windowIndex, key, actualVal, expectedVal)
		}
	}
}

// --- SSTable Creation Helpers ---

type testEntry struct {
	metric string
	tags   map[string]string
	ts     int64
	value  string
	seqNum uint64 // Sequence number for the data point
}

type testEntryWithTombstone struct {
	metric    string
	tags      map[string]string
	ts        int64
	value     string
	seqNum    uint64
	entryType core.EntryType
}

// createTestSSTableForCleanup creates a simple SSTable for cleanup tests.
func createTestSSTableForCleanup(t *testing.T, dir string, id uint64) *sstable.SSTable {
	t.Helper()
	writerOpts := core.SSTableWriterOptions{
		BloomFilterFalsePositiveRate: 0.01,
		DataDir:                      dir,
		ID:                           id,
		Compressor:                   &compressors.NoCompressionCompressor{},
		Logger:                       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	writer, err := sstable.NewSSTableWriter(writerOpts)
	require.NoError(t, err)
	require.NoError(t, writer.Add([]byte(fmt.Sprintf("key-%d", id)), []byte("value"), core.EntryTypePutEvent, id))
	require.NoError(t, writer.Finish())

	loadOpts := sstable.LoadSSTableOptions{FilePath: writer.FilePath(), ID: id, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	tbl, err := sstable.LoadSSTable(loadOpts)
	require.NoError(t, err)
	return tbl
}

// makeTestEventValue creates an encoded FieldValues byte slice for flush tests.
func makeTestEventValue(t *testing.T, val string) []byte {
	t.Helper()
	fields, err := core.NewFieldValuesFromMap(map[string]interface{}{"value": val})
	require.NoError(t, err)
	encoded, err := fields.Encode()
	require.NoError(t, err)
	return encoded
}

// encodeTags is a helper to convert a map of string tags to their encoded ID pairs.
func encodeTags(eng StorageEngineInterface, tags map[string]string) []core.EncodedSeriesTagPair {
	if tags == nil {
		return nil
	}
	concreteEng, ok := eng.(*storageEngine)
	if !ok {
		// This is a test helper, so we can panic if the type is wrong.
		panic("encodeTags helper requires a concrete *storageEngine instance")
	}
	encoded := make([]core.EncodedSeriesTagPair, 0, len(tags))
	for k, v := range tags {
		keyID, _ := concreteEng.stringStore.GetOrCreateID(k)
		valID, _ := concreteEng.stringStore.GetOrCreateID(v)
		encoded = append(encoded, core.EncodedSeriesTagPair{KeyID: keyID, ValueID: valID})
	}
	sort.Slice(encoded, func(i, j int) bool { return encoded[i].KeyID < encoded[j].KeyID })
	return encoded
}
