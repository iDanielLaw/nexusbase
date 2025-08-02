package nbql

import (
	"reflect"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/utils"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	run := func(t *testing.T, name string, input string, expectErr bool, expectedCmd Command) {
		mockNow := utils.NewMockClock(time.Date(2024, 7, 15, 12, 0, 0, 0, time.UTC))
		t.Run(name, func(t *testing.T) {
			stmt, err := ParseWithClock(input, mockNow)
			if expectErr {
				require.Error(t, err, "Expected a parsing error")
				return
			}

			require.NoError(t, err, "Did not expect a parsing error")
			require.NotNil(t, stmt, "Parsed statement should not be nil")

			// Use reflect.DeepEqual for a comprehensive comparison of the structs.
			// It's suitable here because we don't have complex types like functions or channels.
			require.True(t, reflect.DeepEqual(expectedCmd, stmt), "Parsed command does not match expected command.\nExpected: %#v\nGot:      %#v", expectedCmd, stmt)
		})
	}

	// --- PUSH Statement Tests ---
	t.Run("PUSH", func(t *testing.T) {
		run(t, "simple push with fields",
			`PUSH "http.requests" SET (method="GET",status=200,latency_ms=123.45,cached=true,user=NULL)`,
			false,
			&PushStatement{
				Metric:    "http.requests",
				Timestamp: 0, // Implicit NOW() is parsed as 0
				Tags:      nil,
				Fields: map[string]interface{}{
					"method":     "GET",
					"status":     int64(200),
					"latency_ms": 123.45,
					"cached":     true,
					"user":       nil,
				},
			},
		)
		run(t, "push with tags and time",
			`PUSH system.logs TIME 1678886400000 TAGGED (host="server-1", dc="us-east") SET (level="error", message="auth failed");`,
			false,
			&PushStatement{
				Metric:    "system.logs",
				Timestamp: 1678886400000,
				Tags: map[string]string{
					"host": "server-1",
					"dc":   "us-east",
				},
				Fields: map[string]interface{}{
					"level":   "error",
					"message": "auth failed",
				},
			},
		)
		run(t, "push with tags and dt",
			`PUSH system.logs TIME DT("2025-01-01 00:00:00+07:00") TAGGED (host="server-1", dc="us-east") SET (level="error", message="auth failed");`,
			false,
			&PushStatement{
				Metric:    "system.logs",
				Timestamp: 1735664400000000000,
				Tags: map[string]string{
					"host": "server-1",
					"dc":   "us-east",
				},
				Fields: map[string]interface{}{
					"level":   "error",
					"message": "auth failed",
				},
			},
		)
		run(t, "push with tags and now relative",
			`PUSH system.logs TIME NOW(-1h) TAGGED (host="server-1", dc="us-east") SET (level="error", message="auth failed");`,
			false,
			&PushStatement{
				Metric:    "system.logs",
				Timestamp: 1721041200000000000,
				Tags: map[string]string{
					"host": "server-1",
					"dc":   "us-east",
				},
				Fields: map[string]interface{}{
					"level":   "error",
					"message": "auth failed",
				},
			},
		)
		run(t, "push with keyword as metric name",
			`PUSH "FROM" SET (value=1);`,
			false,
			&PushStatement{
				Metric:    "FROM",
				Timestamp: 0,
				Tags:      nil,
				Fields: map[string]interface{}{
					"value": int64(1),
				},
			},
		)
		run(t, "malformed push missing SET", `PUSH http.requests (method="GET");`, true, nil)
		run(t, "malformed push missing fields", `PUSH http.requests SET;`, true, nil)
	})

	// --- QUERY Statement Tests ---
	t.Run("QUERY", func(t *testing.T) {
		run(t, "simple query",
			`QUERY cpu.usage FROM 1000 TO 2000;`,
			false,
			&QueryStatement{
				Metric:    "cpu.usage",
				StartTime: 1000,
				EndTime:   2000,
			},
		)
		run(t, "query with tags and limit",
			`QUERY "mem.free" FROM 1000 TO 2000 TAGGED (host="server-1") LIMIT 50;`,
			false,
			&QueryStatement{
				Metric:    "mem.free",
				StartTime: 1000,
				EndTime:   2000,
				Tags:      map[string]string{"host": "server-1"},
				Limit:     50,
			},
		)
		run(t, "query with aggregation",
			`QUERY http.requests FROM 1000 TO 2000 AGGREGATE (sum(bytes), count(*), avg(latency));`,
			false,
			&QueryStatement{
				Metric:    "http.requests",
				StartTime: 1000,
				EndTime:   2000,
				AggregationSpecs: []AggregationSpec{
					{Function: "sum", Field: "bytes"},
					{Function: "count", Field: "*"},
					{Function: "avg", Field: "latency"},
				},
			},
		)
		run(t, "query with aggregation and aliases",
			`QUERY http.requests FROM 1000 TO 2000 AGGREGATE (sum(bytes) as total_bytes, count(*) as num_reqs, avg(latency) as avg_latency);`,
			false,
			&QueryStatement{
				Metric:    "http.requests",
				StartTime: 1000,
				EndTime:   2000,
				AggregationSpecs: []AggregationSpec{
					{Function: "sum", Field: "bytes", Alias: "total_bytes"},
					{Function: "count", Field: "*", Alias: "num_reqs"},
					{Function: "avg", Field: "latency", Alias: "avg_latency"},
				},
			},
		)
		run(t, "query with downsampling",
			`QUERY system.load FROM 1000 TO 2000 AGGREGATE BY 5m (avg(value), max(value));`,
			false,
			&QueryStatement{
				Metric:             "system.load",
				StartTime:          1000,
				EndTime:            2000,
				DownsampleInterval: "5m",
				AggregationSpecs: []AggregationSpec{
					{Function: "avg", Field: "value"},
					{Function: "max", Field: "value"},
				},
			},
		)
		run(t, "query with NOW()",
			`QUERY logs FROM 1678886400000 TO NOW();`,
			false,
			&QueryStatement{
				Metric:    "logs",
				StartTime: 1678886400000,
				EndTime:   0, // NOW() is parsed as 0 by the visitor
			},
		)
		run(t, "query with DT() function",
			`QUERY logs FROM DT("2024-07-15 12:00:00+07:00") TO DT("2024-07-15T13:00:00Z");`,
			false,
			&QueryStatement{
				Metric: "logs",
				// time.Date(2024, 7, 15, 12, 0, 0, 0, time.FixedZone("ICT", 7*3600)).UnixNano()
				StartTime: 1721019600000000000,
				// time.Date(2024, 7, 15, 13, 0, 0, 0, time.UTC).UnixNano()
				EndTime: 1721048400000000000,
			},
		)
		run(t, "query with relative time",
			`QUERY cpu.usage FROM RELATIVE(1h)`,
			false,
			&QueryStatement{
				Metric:           "cpu.usage",
				IsRelative:       true,
				RelativeDuration: "1h",
				// StartTime and EndTime are now resolved in the engine, not the parser.
				// Their zero values are correct here.
			},
		)
		run(t, "query with order by",
			`QUERY cpu.usage FROM 1000 TO 2000 ORDER DESC LIMIT 10`,
			false,
			&QueryStatement{
				Metric:    "cpu.usage",
				StartTime: 1000,
				EndTime:   2000,
				SortOrder: core.Descending,
				Limit:     10,
			},
		)
		run(t, "query with single quotes",
			`QUERY 'cpu.usage' FROM 1 TO 2 TAGGED ('host'='server-1') AFTER 'cursor-xyz'`,
			false,
			&QueryStatement{
				Metric:    "cpu.usage",
				StartTime: 1,
				EndTime:   2,
				Tags: map[string]string{
					"host": "server-1",
				},
				AfterCursor: "cursor-xyz",
			},
		)
		run(t, "fail query missing time range",
			`QUERY cpu.usage;`,
			true, // Expect an error now
			nil,
		)
		run(t, "fail query with aggregate and order by",
			`QUERY cpu.usage FROM 1000 TO 2000 AGGREGATE BY 1m (avg(value)) ORDER DESC`,
			true, // Expect an error
			nil,
		)
		run(t, "malformed query missing TO", `QUERY cpu.usage FROM 1000;`, true, nil)
		run(t, "fail query with start time after end time",
			`QUERY cpu.usage FROM 2000 TO 1000`,
			true, // Expect an error
			nil,
		)
	})

	// --- REMOVE Statement Tests ---
	t.Run("REMOVE", func(t *testing.T) {
		run(t, "remove series",
			`REMOVE SERIES "cpu.usage" TAGGED (host="server-1");`,
			false,
			&RemoveStatement{
				Type:   RemoveTypeSeries,
				Metric: "cpu.usage",
				Tags:   map[string]string{"host": "server-1"},
			},
		)
		run(t, "remove at point in time",
			`REMOVE FROM "my.metric" TAGGED (app="backend") AT 12345;`,
			false,
			&RemoveStatement{
				Type:      RemoveTypePoint,
				Metric:    "my.metric",
				StartTime: 12345,
				Tags:      map[string]string{"app": "backend"},
			},
		)
		run(t, "remove by time range",
			`REMOVE FROM "my.metric" TAGGED (app="backend") FROM 1000 TO 2000;`,
			false,
			&RemoveStatement{
				Type:      RemoveTypeRange,
				Metric:    "my.metric",
				StartTime: 1000,
				EndTime:   2000,
				Tags:      map[string]string{"app": "backend"},
			},
		)
		run(t, "malformed remove series missing metric", `REMOVE SERIES TAGGED (host="server-1");`, true, nil)
		run(t, "malformed remove range missing tags", `REMOVE FROM "my.metric" FROM 1000 TO 2000;`, true, nil)
	})

	// --- SHOW Statement Tests ---
	t.Run("SHOW", func(t *testing.T) {
		run(t, "show metrics",
			`SHOW METRICS;`,
			false,
			&ShowStatement{Type: ShowMetrics},
		)
		run(t, "show tag keys",
			`SHOW TAG KEYS FROM "cpu.usage";`,
			false,
			&ShowStatement{
				Type:   ShowTagKeys,
				Metric: "cpu.usage",
			},
		)
		run(t, "show tag values",
			`SHOW TAG VALUES FROM system.logs WITH KEY = "host";`,
			false,
			&ShowStatement{
				Type:   ShowTagValues,
				Metric: "system.logs",
				TagKey: "host",
			},
		)
		run(t, "show tag values without from",
			`SHOW TAG VALUES WITH KEY = "region";`,
			false,
			&ShowStatement{
				Type:   ShowTagValues,
				Metric: "", // Metric is optional
				TagKey: "region",
			},
		)
		run(t, "malformed show tag keys", `SHOW TAG KEYS;`, true, nil)
		run(t, "malformed show tag values", `SHOW TAG VALUES WITH host;`, true, nil)
	})

	// --- FLUSH Statement Tests ---
	t.Run("FLUSH", func(t *testing.T) {
		run(t, "flush all", `FLUSH ALL;`, false, &FlushStatement{Type: FlushAll})
		run(t, "flush memtable", `FLUSH MEMTABLE;`, false, &FlushStatement{Type: FlushMemtable})
		run(t, "flush disk", `FLUSH DISK;`, false, &FlushStatement{Type: FlushDisk})
		run(t, "flush default", `FLUSH;`, false, &FlushStatement{Type: FlushAll}) // Default is ALL
	})
}
