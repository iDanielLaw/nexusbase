package listeners

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/INLOpen/nexusbase/hooks"
)

// Thresholds defines the min/max acceptable values for a metric's field.
type Thresholds struct {
	Min float64
	Max float64
}

// OutlierRule defines the configuration for detecting outliers for a specific metric and field.
type OutlierRule struct {
	MetricName string
	FieldName  string
	Thresholds Thresholds
}

// OutlierDetectionListener checks incoming data points for values that fall outside configured thresholds.
type OutlierDetectionListener struct {
	logger *slog.Logger
	rules  map[string]map[string]Thresholds // map[metricName]map[fieldName]Thresholds
}

// NewOutlierDetectionListener creates a new listener for detecting outliers.
// Rules are provided to define what constitutes an outlier for specific metrics/fields.
func NewOutlierDetectionListener(logger *slog.Logger, rules []OutlierRule) *OutlierDetectionListener {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	ruleMap := make(map[string]map[string]Thresholds)
	for _, rule := range rules {
		if _, ok := ruleMap[rule.MetricName]; !ok {
			ruleMap[rule.MetricName] = make(map[string]Thresholds)
		}
		ruleMap[rule.MetricName][rule.FieldName] = rule.Thresholds
	}

	return &OutlierDetectionListener{
		logger: logger.With("component", "OutlierDetectionListener"),
		rules:  ruleMap,
	}
}

// OnEvent handles PrePutBatch events to check for outliers before data is written.
func (l *OutlierDetectionListener) OnEvent(ctx context.Context, event hooks.HookEvent) error {
	// This listener works on PrePutBatch for efficiency.
	if event.Type() != hooks.EventPrePutBatch {
		return nil
	}

	payload, ok := event.Payload().(hooks.PrePutBatchPayload)
	if !ok {
		l.logger.Error("Received PrePutBatch event with incorrect payload type", "payload_type", fmt.Sprintf("%T", event.Payload()))
		return nil
	}

	// The payload contains a pointer to the slice of points.
	// We iterate through them without modifying them.
	for _, point := range *payload.Points {
		metricRules, hasRulesForMetric := l.rules[point.Metric]
		if !hasRulesForMetric {
			continue // No rules for this metric, skip.
		}

		for fieldName, thresholds := range metricRules {
			if fieldValue, ok := point.Fields[fieldName]; ok {
				var numericValue float64
				isNumeric := false

				// Attempt to get the value as a float64 for comparison, checking both float and int types.
				if fVal, ok := fieldValue.ValueFloat64(); ok {
					numericValue = fVal
					isNumeric = true
				} else if iVal, ok := fieldValue.ValueInt64(); ok {
					numericValue = float64(iVal)
					isNumeric = true
				}

				if isNumeric {
					if numericValue < thresholds.Min || numericValue > thresholds.Max {
						l.logger.Warn("Outlier detected",
							"metric", point.Metric,
							"tags", fmt.Sprintf("%v", point.Tags),
							"field", fieldName,
							"value", numericValue,
							"min_threshold", thresholds.Min,
							"max_threshold", thresholds.Max,
						)
					}
				}
			}
		}
	}

	// This is a detection hook, not a validation hook, so we don't cancel the operation.
	return nil
}

// Priority defines the execution order.
func (l *OutlierDetectionListener) Priority() int { return 100 }

// IsAsync indicates this listener can run in the background.
// Since it's a Pre-hook, it will run synchronously regardless, but it's good practice to define it.
func (l *OutlierDetectionListener) IsAsync() bool { return false }
