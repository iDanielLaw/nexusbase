package core

// DataPoint represents a single structured data point or event.
// It is the canonical representation of data within the system,
// consolidating the previously separate DataPoint and PushStatement concepts.

type DataPoint struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    FieldValues
}

// NewDataPoint creates a new DataPoint, ensuring the Fields map is initialized.
func NewDataPoint(metric string, tags map[string]string, timestamp int64) (*DataPoint, error) {
	// Validation is now expected to be done by the caller (e.g., the engine)
	// using a Validator instance.
	return &DataPoint{
		Metric:    metric,
		Tags:      tags,
		Timestamp: timestamp,
	}, nil
}

// AddField adds or updates a field in the event's Fields map.
// It initializes the map if it's nil.
func (dp *DataPoint) AddField(key string, value PointValue) {
	if dp.Fields == nil {
		dp.Fields = make(FieldValues)
	}
	dp.Fields[key] = value
}

// GetField retrieves a field's value and its existence.
func (dp *DataPoint) GetField(key string) (PointValue, bool) {
	if dp.Fields == nil {
		return PointValue{}, false
	}
	val, ok := dp.Fields[key]
	return val, ok
}

func ValidateMetricAndTags(validator *Validator, metric string, tags map[string]string) error {
	if err := validator.ValidateMetricName(metric); err != nil {
		return err
	}
	for k, v := range tags {
		if err := validator.ValidateLabelName(k); err != nil {
			return err
		}
		if err := ValidateLabelValue(v); err != nil {
			return err
		}
	}
	return nil
}
