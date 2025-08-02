package engine

import (
	"encoding/json"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryResultItem_JSONMarshaling(t *testing.T) {
	t.Skip()
	// Helper to create a PointValue, panics on error for test simplicity
	mustNewPointValue := func(data any) core.PointValue {
		pv, err := core.NewPointValue(data)
		if err != nil {
			panic(err)
		}
		return pv
	}

	testCases := []struct {
		name           string
		item           core.QueryResultItem
		expectedJSON   string
		expectOmitKeys []string // Keys that should NOT be in the JSON
	}{
		{
			name: "Raw Event Data Point",
			item: core.QueryResultItem{
				Metric:    "http.requests",
				Tags:      map[string]string{"host": "server-1"},
				Timestamp: 1678886400000,
				IsEvent:   true,
				Fields: core.FieldValues{
					"status": mustNewPointValue(int64(200)),
					"method": mustNewPointValue("GET"),
				},
				IsAggregated: false, // Important for omitting aggregation fields
			},
			expectedJSON:   `{"metric":"http.requests","tags":{"host":"server-1"},"timestamp":1678886400000,"fields":{"method":"GET","status":200},"is_aggregated":false,"is_event":true}`,
			expectOmitKeys: []string{"window_start_time", "window_end_time", "aggregated_values"},
		},
		{
			name: "Aggregated Window",
			item: core.QueryResultItem{
				Metric: "http.requests",
				Tags:   map[string]string{"host": "server-1"},
				AggregatedValues: map[string]float64{
					"count": 100,
					"avg":   55.5,
				},
				IsAggregated:    true,
				WindowStartTime: 1678886400000,
				WindowEndTime:   1678886460000,
			},
			expectedJSON:   `{"metric":"http.requests","tags":{"host":"server-1"},"is_aggregated":true,"window_start_time":1678886400000,"window_end_time":1678886460000,"aggregated_values":{"avg":55.5,"count":100}}`,
			expectOmitKeys: []string{"timestamp", "fields", "is_event"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Marshal the item to JSON
			jsonData, err := json.Marshal(tc.item)
			require.NoError(t, err, "json.Marshal should not fail")

			// To make comparison reliable, unmarshal both expected and actual JSON into maps
			var actualMap, expectedMap map[string]interface{}
			require.NoError(t, json.Unmarshal(jsonData, &actualMap), "Unmarshaling actual JSON failed")
			require.NoError(t, json.Unmarshal([]byte(tc.expectedJSON), &expectedMap), "Unmarshaling expected JSON failed")

			// Compare the maps
			assert.Equal(t, expectedMap, actualMap, "The marshaled JSON does not match the expected structure")

			// Verify that omitempty fields are indeed omitted
			for _, key := range tc.expectOmitKeys {
				_, exists := actualMap[key]
				assert.False(t, exists, "Key '%s' should have been omitted from JSON", key)
			}
		})
	}
}
