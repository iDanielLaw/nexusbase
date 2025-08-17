package engine

// addActiveSeries tracks a new active time series.
// It now also persists the new series key to the series.log file if it's a new series.
func (e *storageEngine) addActiveSeries(seriesKey string) {
	if seriesKey == "" {
		return
	}
	e.activeSeriesMu.Lock()
	// Check if the series already exists in the map.
	if _, exists := e.activeSeries[seriesKey]; !exists {
		// If it doesn't exist, it's a new series.
		// 1. Add it to the in-memory map.
		e.activeSeries[seriesKey] = struct{}{}
		// 2. Persist it to the series log file.
		e.persistNewSeriesKey_locked([]byte(seriesKey))
		// 3. Increment the metric for new series creation.
		if e.metrics != nil && e.metrics.SeriesCreatedTotal != nil {
			e.metrics.SeriesCreatedTotal.Add(1)
		}
	}
	e.activeSeriesMu.Unlock()
}

// persistNewSeriesKey_locked writes a new series key to the series.log file.
// It assumes the caller (addActiveSeries) holds the activeSeriesMu lock.
// It acquires its own lock for file I/O.
func (e *storageEngine) persistNewSeriesKey_locked(seriesKey []byte) {
	e.seriesLogMu.Lock()
	defer e.seriesLogMu.Unlock()

	if e.seriesLogFile != nil {
		// Append the key followed by a newline character.
		if _, err := e.seriesLogFile.Write(append(seriesKey, '\n')); err != nil {
			e.logger.Error("Failed to write new series key to series.log", "error", err)
		}
	}
}

// removeActiveSeries stops tracking an active time series.
func (e *storageEngine) removeActiveSeries(seriesKey string) {
	if seriesKey == "" {
		return
	}
	e.activeSeriesMu.Lock()
	delete(e.activeSeries, seriesKey)
	e.activeSeriesMu.Unlock()
}
