package engine

import (
	"os"
	"path/filepath"
)

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
		e.logger.Info("addActiveSeries: adding new series", "series_key", seriesKey)
		e.activeSeries[seriesKey] = struct{}{}
		// 2. Persist it to the series log file.
		e.persistNewSeriesKey_locked([]byte(seriesKey))
		// 3. Also append to a debug file in the data dir for offline inspection.
		if e.opts.DataDir != "" {
			func() {
				path := filepath.Join(e.opts.DataDir, "debug_series_writes.log")
				f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err == nil {
					_, _ = f.Write(append([]byte(seriesKey), '\n'))
					_ = f.Close()
				}
			}()
		}
		// Also write to a global temp file to persist across test cleanup
		func() {
			path := filepath.Join(os.TempDir(), "nexus_debug_series_writes.log")
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err == nil {
				_, _ = f.Write(append([]byte(seriesKey), '\n'))
				_ = f.Close()
			}
		}()
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
		// Ensure the write is flushed to disk so a simulated crash does not
		// leave a partially-written series key in the log. This avoids
		// malformed entries during recovery which can create spurious active
		// series. Sync is cheap for tests and correctness; in hot paths we may
		// revisit this for performance.
		if err := e.seriesLogFile.Sync(); err != nil {
			e.logger.Warn("Failed to sync series.log after write", "error", err)
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
