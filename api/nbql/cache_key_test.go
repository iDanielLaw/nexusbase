package nbql

import (
	"testing"

	corenbql "github.com/INLOpen/nexuscore/nbql"
	"github.com/INLOpen/nexuscore/types"
	"github.com/stretchr/testify/assert"
)

func TestGenerateCacheKey(t *testing.T) {
	testCases := []struct {
		name     string
		stmt1    *corenbql.QueryStatement
		stmt2    *corenbql.QueryStatement // A statement that should produce the same key as stmt1
		diffStmt *corenbql.QueryStatement // A statement that should produce a different key
	}{
		{
			name: "Simple relative query",
			stmt1: &corenbql.QueryStatement{
				Metric:           "cpu.usage",
				IsRelative:       true,
				RelativeDuration: "1h",
				Tags:             map[string]string{"host": "server-a"},
			},
			stmt2: &corenbql.QueryStatement{ // Identical query
				Metric:           "cpu.usage",
				IsRelative:       true,
				RelativeDuration: "1h",
				Tags:             map[string]string{"host": "server-a"},
			},
			diffStmt: &corenbql.QueryStatement{ // Different duration
				Metric:           "cpu.usage",
				IsRelative:       true,
				RelativeDuration: "5m",
				Tags:             map[string]string{"host": "server-a"},
			},
		},
		{
			name: "Tags order should not matter",
			stmt1: &corenbql.QueryStatement{
				Metric:           "http.requests",
				IsRelative:       true,
				RelativeDuration: "30m",
				Tags:             map[string]string{"host": "A", "region": "us-east"},
			},
			stmt2: &corenbql.QueryStatement{ // Same tags, different order in map literal
				Metric:           "http.requests",
				IsRelative:       true,
				RelativeDuration: "30m",
				Tags:             map[string]string{"region": "us-east", "host": "A"},
			},
			diffStmt: &corenbql.QueryStatement{ // Different tag value
				Metric:           "http.requests",
				IsRelative:       true,
				RelativeDuration: "30m",
				Tags:             map[string]string{"host": "B", "region": "us-east"},
			},
		},
		{
			name: "Query with aggregation and downsampling",
			stmt1: &corenbql.QueryStatement{
				Metric:             "system.load",
				IsRelative:         true,
				RelativeDuration:   "6h",
				DownsampleInterval: "5m",
				EmitEmptyWindows:   true,
				AggregationSpecs: []corenbql.AggregationSpec{
					{Function: "avg", Field: "load1"},
					{Function: "max", Field: "load5", Alias: "max_load"},
				},
			},
			stmt2: &corenbql.QueryStatement{ // Identical
				Metric:             "system.load",
				IsRelative:         true,
				RelativeDuration:   "6h",
				DownsampleInterval: "5m",
				EmitEmptyWindows:   true,
				AggregationSpecs: []corenbql.AggregationSpec{
					{Function: "avg", Field: "load1"},
					{Function: "max", Field: "load5", Alias: "max_load"},
				},
			},
			diffStmt: &corenbql.QueryStatement{ // Different aggregation
				Metric:             "system.load",
				IsRelative:         true,
				RelativeDuration:   "6h",
				DownsampleInterval: "5m",
				EmitEmptyWindows:   true,
				AggregationSpecs: []corenbql.AggregationSpec{
					{Function: "avg", Field: "load1"},
					// Missing the max aggregation
				},
			},
		},
		{
			name: "Query with different sort order",
			stmt1: &corenbql.QueryStatement{
				Metric:    "logs.count",
				StartTime: 1000,
				EndTime:   2000,
				SortOrder: types.Ascending,
			},
			stmt2: &corenbql.QueryStatement{ // Identical
				Metric:    "logs.count",
				StartTime: 1000,
				EndTime:   2000,
				SortOrder: types.Ascending,
			},
			diffStmt: &corenbql.QueryStatement{ // Different sort order
				Metric:    "logs.count",
				StartTime: 1000,
				EndTime:   2000,
				SortOrder: types.Descending,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key1 := generateCacheKey(tc.stmt1)
			key2 := generateCacheKey(tc.stmt2)
			diffKey := generateCacheKey(tc.diffStmt)

			assert.NotEmpty(t, key1, "Generated key should not be empty")
			assert.Equal(t, key1, key2, "Identical statements should produce the same cache key")
			assert.NotEqual(t, key1, diffKey, "Different statements should produce different cache keys")
			t.Logf("Generated key for stmt1: %s", key1)
			t.Logf("Generated key for diffStmt: %s", diffKey)
		})
	}
}
