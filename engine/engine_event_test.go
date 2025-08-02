package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	sys.SetDebugMode(true)
}

func TestStorageEngine_PutEvent(t *testing.T) {
	opts := getBaseOptsForFlushTest(t) // Re-use helper from flush test
	engine, err := NewStorageEngine(opts)
	require.NoError(t, err)
	err = engine.Start()
	require.NoError(t, err)
	defer engine.Close()

	concreteEngine := engine.(*storageEngine)

	event := HelperDataPoint(
		t,
		"user.login.attempt",
		map[string]string{"ip": "192.168.1.100", "region": "us-east"},
		1678886400000000000,
		map[string]interface{}{
			"success":       true,
			"username":      "testuser",
			"latency_ms":    float64(55.8),
			"attempt_count": int64(3),
		},
	)

	// Action
	err = engine.Put(context.Background(), event)
	require.NoError(t, err)

	// Verification
	// We can't query fields yet, but we can check the raw entry in the memtable.
	metricID, _ := concreteEngine.stringStore.GetOrCreateID(event.Metric)
	encodedTags := encodeTags(engine, event.Tags)
	tsdbKey := core.EncodeTSDBKey(metricID, encodedTags, event.Timestamp)

	// Check memtable
	valueBytes, entryType, found := concreteEngine.mutableMemtable.Get(tsdbKey)
	require.True(t, found, "event key should be found in memtable")
	require.Equal(t, core.EntryTypePutEvent, entryType, "entry type should be PutEvent")

	// Decode the value and check fields
	decodedFields, err := core.DecodeFields(bytes.NewBuffer(valueBytes))
	require.NoError(t, err, "failed to decode fields from memtable value")
	assert.Equal(t, event.Fields, decodedFields, "decoded fields should match original event fields")
}

func TestStorageEngine_PutEvent_EdgeCasesAndErrors(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name             string
		metric           string
		tags             map[string]string
		ts               int64
		fields           map[string]interface{}
		setupOpts        func(opts *StorageEngineOptions) // Optional function to modify engine options
		wantErr          bool
		wantErrType      interface{} // e.g., (*core.ValidationError)(nil)
		verify           func(t *testing.T, eng StorageEngineInterface)
		wantErrField     bool
		wantErrFieldType interface{}
	}{
		{
			name:        "error_invalid_metric_name",
			metric:      "", // Invalid empty metric
			tags:        map[string]string{"id": "A"},
			ts:          1,
			fields:      map[string]interface{}{"value": 1.0},
			wantErr:     true,
			wantErrType: (*core.ValidationError)(nil),
		},
		{
			name:        "error_invalid_tag_name",
			metric:      "valid.metric",
			tags:        map[string]string{"__reserved": "fail"}, // Invalid tag name
			ts:          1,
			fields:      map[string]interface{}{"value": 1.0},
			wantErr:     true,
			wantErrType: (*core.ValidationError)(nil),
		},
		{
			name:             "error_unsupported_field_type",
			metric:           "valid.metric",
			tags:             map[string]string{"id": "A"},
			ts:               1,
			fields:           map[string]interface{}{"bad_field": make(chan int)}, // Unsupported type
			wantErrField:     true,                                                // The error will be from core.EncodeFields
			wantErrFieldType: (*core.UnsupportedTypeError)(nil),
		},
		{
			name:    "success_no_tags",
			metric:  "metric.no_tags",
			tags:    nil,
			ts:      1,
			fields:  map[string]interface{}{"value": 1.0},
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineInterface) {
				// We can't query yet, but we can check the memtable
				concreteEngine := eng.(*storageEngine)
				metricID, _ := concreteEngine.stringStore.GetOrCreateID("metric.no_tags")
				tsdbKey := core.EncodeTSDBKey(metricID, nil, 1)
				_, _, found := concreteEngine.mutableMemtable.Get(tsdbKey)
				require.True(t, found, "event with no tags should be in memtable")
			},
		},
		{
			name:    "success_no_fields",
			metric:  "metric.no_fields",
			tags:    map[string]string{"id": "A"},
			ts:      1,
			fields:  nil,
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineInterface) {
				concreteEngine := eng.(*storageEngine)
				metricID, _ := concreteEngine.stringStore.GetOrCreateID("metric.no_fields")
				encodedTags := encodeTags(eng, map[string]string{"id": "A"})
				tsdbKey := core.EncodeTSDBKey(metricID, encodedTags, 1)
				valueBytes, _, found := concreteEngine.mutableMemtable.Get(tsdbKey)
				require.True(t, found, "event with no fields should be in memtable")

				decoded, err := core.DecodeFields(bytes.NewBuffer(valueBytes))
				require.NoError(t, err)
				assert.Empty(t, decoded, "decoded fields map should be empty")
			},
		},
		{
			name:   "success_triggers_flush",
			metric: "metric.flush.trigger",
			tags:   map[string]string{"id": "A"},
			ts:     1,
			fields: map[string]interface{}{"data": "some data that makes the entry large enough"},
			setupOpts: func(opts *StorageEngineOptions) {
				opts.MemtableThreshold = 10 // Very small threshold
			},
			wantErr: false,
			verify: func(t *testing.T, eng StorageEngineInterface) {
				concreteEngine := eng.(*storageEngine)
				// The Put call triggers an asynchronous flush. To fix the data race and create a
				// robust test, we must wait for the flush to complete and then verify the final state.
				// The most reliable outcome to wait for is the creation of the new L0 SSTable.
				require.Eventually(t, func() bool {
					// GetTablesForLevel is assumed to be thread-safe.
					l0Tables := concreteEngine.levelsManager.GetTablesForLevel(0)
					return len(l0Tables) == 1
				}, 2*time.Second, 10*time.Millisecond, "timed out waiting for flush to create an L0 sstable")

				// Now that the flush is complete, we can safely check the state of the memtables.
				concreteEngine.mu.RLock()
				defer concreteEngine.mu.RUnlock()
				assert.Empty(t, concreteEngine.immutableMemtables, "immutable memtables queue should be empty after flush completes")
				assert.NotNil(t, concreteEngine.mutableMemtable, "a new mutable memtable should still exist")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := getBaseOptsForFlushTest(t)
			if tc.setupOpts != nil {
				tc.setupOpts(&opts)
			}

			engine, err := NewStorageEngine(opts)
			require.NoError(t, err)
			err = engine.Start()
			require.NoError(t, err)
			defer engine.Close()
			concreteEngine := engine.(*storageEngine)

			err = core.ValidateMetricAndTags(concreteEngine.validator, tc.metric, tc.tags)
			if tc.wantErr {
				require.Error(t, err, "Expected an error but got nil")
				if tc.wantErrType != nil {
					assert.ErrorAs(t, err, &tc.wantErrType, "Error type mismatch")
				}
			}

			event, err2 := MakeDataPoint(tc.metric, tc.tags, tc.ts, tc.fields)
			if tc.wantErrField {
				require.Error(t, err2, "Expected an error but got nil")
				if tc.wantErrFieldType != nil {
					assert.ErrorAs(t, err2, &tc.wantErrFieldType, "Error type mismatch")
				}
			} else {
				require.NoError(t, err2, "Expected no error but got: %v", err2)
			}

			// If event creation succeeds, proceed to call Put.
			err = engine.Put(ctx, event)

			if tc.wantErr {
				require.Error(t, err, "Expected an error but got nil")
				if tc.wantErrType != nil {
					assert.ErrorAs(t, err, &tc.wantErrType, "Error type mismatch")
				}
			}

			if tc.wantErrField {
				require.Error(t, err, "Expected an error but got nil")
				if tc.wantErrType != nil {
					assert.ErrorAs(t, err, &tc.wantErrType, "Error type mismatch")
				}
			}

			if !tc.wantErr && !tc.wantErrField {
				require.NoError(t, err, "Expected no error but got: %v", err)
			}

			if tc.verify != nil {
				tc.verify(t, engine)
			}
		})
	}
}
