package nbql

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/INLOpen/nexusbase/core"
)

// generateCacheKey creates a canonical and unique string key for a given QueryStatement.
// This key is used for caching query results, especially for relative time queries.
// A canonical key ensures that queries that are semantically identical but written
// differently (e.g., different tag order) produce the same key.
func generateCacheKey(stmt *QueryStatement) string {
	// Using bytes.Buffer is more efficient for building strings piece by piece than concatenation.
	var key bytes.Buffer

	// 1. Metric Name: The base of the query.
	key.WriteString(stmt.Metric)
	key.WriteByte('|')

	// 2. Time Range: Differentiate between cacheable relative queries and unique absolute queries.
	if stmt.IsRelative {
		key.WriteString("REL:")
		key.WriteString(stmt.RelativeDuration)
	} else {
		key.WriteString("ABS:")
		key.WriteString(strconv.FormatInt(stmt.StartTime, 10))
		key.WriteByte('-')
		key.WriteString(strconv.FormatInt(stmt.EndTime, 10))
	}
	key.WriteByte('|')

	// 3. Tags: Sort tag keys to ensure canonical representation.
	// This makes `host=A,region=B` and `region=B,host=A` produce the same key part.
	if len(stmt.Tags) > 0 {
		tagKeys := make([]string, 0, len(stmt.Tags))
		for k := range stmt.Tags {
			tagKeys = append(tagKeys, k)
		}
		sort.Strings(tagKeys)

		for i, k := range tagKeys {
			if i > 0 {
				key.WriteByte(',')
			}
			key.WriteString(k)
			key.WriteByte('=')
			key.WriteString(stmt.Tags[k])
		}
	}
	key.WriteByte('|')

	// 4. Aggregation and Downsampling: The order of specs matters.
	if len(stmt.AggregationSpecs) > 0 {
		if stmt.DownsampleInterval != "" {
			key.WriteString("DS:")
			key.WriteString(stmt.DownsampleInterval)
			if stmt.EmitEmptyWindows {
				key.WriteString(",EMPTY")
			}
			key.WriteByte(':')
		}

		for i, spec := range stmt.AggregationSpecs {
			if i > 0 {
				key.WriteByte(',')
			}
			key.WriteString(spec.Function)
			key.WriteByte('(')
			key.WriteString(spec.Field)
			key.WriteByte(')')
			if spec.Alias != "" {
				key.WriteString(" as ")
				key.WriteString(spec.Alias)
			}
		}
	}
	key.WriteByte('|')

	// 5. Sort Order: ASC vs DESC produces different results.
	if stmt.SortOrder == core.Descending {
		key.WriteString("DESC")
	} else {
		key.WriteString("ASC") // Default
	}

	// Note: Limit and AfterCursor are for pagination and are intentionally excluded
	// from the cache key. The full result set is cached, and pagination is applied
	// to the cached result.

	return key.String()
}
