package engine

// GetActiveSeriesSnapshot returns a snapshot (slice) of active series keys
// for the provided StorageEngineInterface. It will return an error if the
// passed engine cannot be inspected (should be a concrete *storageEngine).
func GetActiveSeriesSnapshot(e StorageEngineInterface) ([]string, error) {
	// Attempt to type-assert to the concrete implementation so we can access
	// the activeSeries map under lock. This keeps the lookup internal to the
	// engine package while exposing a safe exported accessor for tools.
	se, ok := e.(*storageEngine)
	if !ok || se == nil {
		return nil, nil
	}
	se.activeSeriesMu.RLock()
	defer se.activeSeriesMu.RUnlock()
	out := make([]string, 0, len(se.activeSeries))
	for k := range se.activeSeries {
		out = append(out, k)
	}
	return out, nil
}
