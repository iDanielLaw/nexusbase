package iterator

import (
	"strconv"
)

// parsePercentile extracts the percentile value (e.g., 95.5) from a function name like "p95.5".
func parsePercentile(funcName string) (float64, bool) {
	if len(funcName) > 1 && (funcName[0] == 'p' || funcName[0] == 'P') {
		p, err := strconv.ParseFloat(funcName[1:], 64)
		if err == nil && p >= 0 && p <= 100 {
			return p, true
		}
	}
	return 0, false
}
