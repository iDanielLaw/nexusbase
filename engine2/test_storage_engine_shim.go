package engine2

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/hooks"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/sstable"
	"github.com/INLOpen/nexuscore/utils/clock"
)

// storageEngineShim is a minimal in-memory implementation of the
// StorageEngine-like API surface used by the legacy tests while they
// are being ported. It is intentionally simple and not meant for
// production use; it provides determinism for unit tests.
type storageEngineShim struct {
	mu          sync.RWMutex
	data        map[string]map[int64]core.FieldValues // seriesKey -> ts -> fields
	hookManager hooks.HookManager
	dataDir     string
	nextID      uint64
	stringStore indexer.StringStoreInterface
	clk         clock.Clock
	wal         *testWAL
	// Tombstone state for tests
	deletedSeries     map[string]uint64
	deletedSeriesMu   sync.RWMutex
	rangeTombstones   map[string][]core.RangeTombstone
	rangeTombstonesMu sync.RWMutex
}

func newStorageEngineShim(baseDir string) *storageEngineShim {
	s := &storageEngineShim{
		data:            make(map[string]map[int64]core.FieldValues),
		dataDir:         filepath.Join(baseDir, "shim"),
		nextID:          1000,
		clk:             clock.SystemClockDefault,
		wal:             &testWAL{},
		deletedSeries:   make(map[string]uint64),
		rangeTombstones: make(map[string][]core.RangeTombstone),
	}
	s.hookManager = hooks.NewHookManager(nil)
	s.stringStore = NewStringStore()
	return s
}

func (s *storageEngineShim) Start() error                      { return nil }
func (s *storageEngineShim) Close() error                      { return nil }
func (s *storageEngineShim) GetHookManager() hooks.HookManager { return s.hookManager }
func (s *storageEngineShim) GetNextSSTableID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return s.nextID
}
func (s *storageEngineShim) GetWALPath() string                           { return filepath.Join(s.dataDir, "wal") }
func (s *storageEngineShim) GetStringStore() indexer.StringStoreInterface { return s.stringStore }
func (s *storageEngineShim) Metrics() (*EngineMetrics, error) {
	// Provide a real EngineMetrics instance so tests that inspect metrics can run.
	return NewEngineMetrics(false, "shim_"), nil
}
func (s *storageEngineShim) GetPubSub() (PubSubInterface, error) {
	return nil, fmt.Errorf("not implemented")
}

// Put stores a single datapoint.
func (s *storageEngineShim) Put(ctx context.Context, point core.DataPoint) error {
	// fire pre-put hooks
	if hm := s.GetHookManager(); hm != nil {
		ev := hooks.NewPrePutDataPointEvent(hooks.PrePutDataPointPayload{Metric: &point.Metric, Tags: &point.Tags, Timestamp: &point.Timestamp, Fields: &point.Fields})
		if err := hm.Trigger(ctx, ev); err != nil {
			return err
		}
	}

	key := string(core.EncodeSeriesKeyWithString(point.Metric, point.Tags))
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[int64]core.FieldValues)
	}
	s.data[key][point.Timestamp] = point.Fields

	if hm := s.GetHookManager(); hm != nil {
		payload := hooks.NewPostPutDataPointEvent(hooks.PostPutDataPointPayload{Metric: point.Metric, Tags: point.Tags, Timestamp: point.Timestamp, Fields: point.Fields})
		_ = hm.Trigger(ctx, payload)
	}
	return nil
}

func (s *storageEngineShim) PutBatch(ctx context.Context, points []core.DataPoint) error {
	// Run PrePutBatch hook which may modify the slice or cancel the operation.
	if hm := s.GetHookManager(); hm != nil {
		ev := hooks.NewPrePutBatchEvent(hooks.PrePutBatchPayload{Points: &points})
		if err := hm.Trigger(ctx, ev); err != nil {
			return err
		}
	}

	// Prepare WAL entries (simple emulation) and attempt to append atomically.
	// If WAL append fails, the batch should not be applied.
	walEntries := make([]core.WALEntry, 0, len(points))
	for _, p := range points {
		key := core.EncodeTSDBKeyWithString(p.Metric, p.Tags, p.Timestamp)
		buf, _ := p.Fields.Encode()
		walEntries = append(walEntries, core.WALEntry{EntryType: core.EntryTypePutEvent, Key: key, Value: buf})
	}
	if w := s.wal; w != nil {
		if err := w.AppendBatch(walEntries); err != nil {
			return err
		}
	}

	// If WAL append succeeded, write to in-memory store
	for _, p := range points {
		if err := s.Put(ctx, p); err != nil {
			return err
		}
	}

	// Fire PostPutBatch hook asynchronously (mimic real engine behavior)
	if hm := s.GetHookManager(); hm != nil {
		payload := hooks.PostPutBatchPayload{Points: points, Error: nil}
		go func() { _ = hm.Trigger(ctx, hooks.NewPostPutBatchEvent(payload)) }()
	}
	return nil
}

func (s *storageEngineShim) Get(ctx context.Context, metric string, tags map[string]string, timestamp int64) (core.FieldValues, error) {
	// Allow pre-get hooks to modify metric/tags/timestamp before lookup
	if hm := s.GetHookManager(); hm != nil {
		p := hooks.NewPreGetPointEvent(hooks.PreGetPointPayload{Metric: &metric, Tags: &tags, Timestamp: &timestamp})
		if err := hm.Trigger(ctx, p); err != nil {
			return nil, err
		}
	}

	key := string(core.EncodeSeriesKeyWithString(metric, tags))
	s.mu.RLock()
	series, ok := s.data[key]
	s.mu.RUnlock()
	if ok {
		if fv, ok2 := series[timestamp]; ok2 {
			// post-get
			if hm := s.GetHookManager(); hm != nil {
				payload := hooks.NewPostGetPointEvent(hooks.PostGetPointPayload{Metric: metric, Tags: tags, Timestamp: timestamp, Result: &fv})
				_ = hm.Trigger(ctx, payload)
			}
			return fv, nil
		}
	}
	return nil, sstable.ErrNotFound
}

func (s *storageEngineShim) Delete(ctx context.Context, metric string, tags map[string]string, timestamp int64) error {
	// Allow pre-delete hooks to modify metric/tags/timestamp or cancel
	if hm := s.GetHookManager(); hm != nil {
		p := hooks.NewPreDeletePointEvent(hooks.PreDeletePointPayload{Metric: &metric, Tags: &tags, Timestamp: &timestamp})
		if err := hm.Trigger(ctx, p); err != nil {
			return err
		}
	}

	key := string(core.EncodeSeriesKeyWithString(metric, tags))
	s.mu.Lock()
	if series, ok := s.data[key]; ok {
		delete(series, timestamp)
	}
	s.mu.Unlock()
	if hm := s.GetHookManager(); hm != nil {
		payload := hooks.NewPostDeletePointEvent(hooks.PostDeletePointPayload{Metric: metric, Tags: tags, Timestamp: timestamp})
		_ = hm.Trigger(ctx, payload)
	}
	return nil
}

func (s *storageEngineShim) DeleteSeries(ctx context.Context, metric string, tags map[string]string) error {
	// Pre-delete-series hook: allow modification or cancellation
	if hm := s.GetHookManager(); hm != nil {
		p := hooks.NewPreDeleteSeriesEvent(hooks.PreDeleteSeriesPayload{Metric: &metric, Tags: &tags})
		if err := hm.Trigger(ctx, p); err != nil {
			return err
		}
	}

	key := string(core.EncodeSeriesKeyWithString(metric, tags))
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	// Post-delete-series hook
	if hm := s.GetHookManager(); hm != nil {
		payload := hooks.PostDeleteSeriesPayload{Metric: metric, Tags: tags, SeriesKey: key}
		_ = hm.Trigger(ctx, hooks.NewPostDeleteSeriesEvent(payload))
	}
	return nil
}

func (s *storageEngineShim) DeletesByTimeRange(ctx context.Context, metric string, tags map[string]string, startTime, endTime int64) error {
	// Pre-delete-range hook
	if hm := s.GetHookManager(); hm != nil {
		p := hooks.NewPreDeleteRangeEvent(hooks.PreDeleteRangePayload{Metric: &metric, Tags: &tags, StartTime: &startTime, EndTime: &endTime})
		if err := hm.Trigger(ctx, p); err != nil {
			return err
		}
	}

	key := string(core.EncodeSeriesKeyWithString(metric, tags))
	s.mu.Lock()
	if series, ok := s.data[key]; ok {
		for ts := range series {
			if ts >= startTime && ts <= endTime {
				delete(series, ts)
			}
		}
	}
	s.mu.Unlock()

	// Post-delete-range hook (include SeriesKey and times)
	if hm := s.GetHookManager(); hm != nil {
		payload := hooks.PostDeleteRangePayload{Metric: metric, Tags: tags, SeriesKey: key, StartTime: startTime, EndTime: endTime}
		_ = hm.Trigger(ctx, hooks.NewPostDeleteRangeEvent(payload))
	}
	return nil
}

// simple in-memory iterator
type memIterator struct {
	items []*core.QueryResultItem
	i     int
}

func (it *memIterator) Next() bool {
	if it.i >= len(it.items) {
		return false
	}
	it.i++
	return true
}
func (it *memIterator) At() (*core.QueryResultItem, error) {
	if it.i == 0 || it.i > len(it.items) {
		return nil, fmt.Errorf("out of range")
	}
	return it.items[it.i-1], nil
}
func (it *memIterator) AtValue() (core.QueryResultItem, error) {
	v, err := it.At()
	if err != nil {
		return core.QueryResultItem{}, err
	}
	return *v, nil
}
func (it *memIterator) UnderlyingAt() (*core.IteratorNode, error) { return nil, nil }
func (it *memIterator) Close() error                              { return nil }
func (it *memIterator) Error() error                              { return nil }
func (it *memIterator) Put(item *core.QueryResultItem)            {}

func (s *storageEngineShim) Query(ctx context.Context, params core.QueryParams) (core.QueryResultIteratorInterface, error) {
	// naive implementation: collect matching points and sort by timestamp
	var collected []*core.QueryResultItem
	s.mu.RLock()
	for key, series := range s.data {
		if params.Metric != "" {
			// Allow metric + subset-of-tags matching: when tests query by metric and a subset
			// of tags (e.g. metric + {region: east}), we should match series that include
			// those tag key/values. The legacy engine supports this; make the shim liberal
			// and match any series whose encoded key contains the metric prefix and
			// all provided tag key/value substrings.
			if !strings.HasPrefix(key, params.Metric) {
				continue
			}
			if params.Tags != nil {
				match := true
				for k, v := range params.Tags {
					if !(contains(key, k) && contains(key, v)) {
						match = false
						break
					}
				}
				if !match {
					continue
				}
			}
		}
		for ts, fv := range series {
			if params.StartTime != 0 && ts < params.StartTime {
				continue
			}
			if params.EndTime != 0 && ts >= params.EndTime {
				continue
			}
			collected = append(collected, &core.QueryResultItem{Timestamp: ts, Fields: fv, Tags: params.Tags})
		}
	}
	s.mu.RUnlock()
	sort.Slice(collected, func(i, j int) bool { return collected[i].Timestamp < collected[j].Timestamp })
	return &memIterator{items: collected, i: 0}, nil
}

func (s *storageEngineShim) GetSeriesByTags(metric string, tags map[string]string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var keys []string
	for key := range s.data {
		if metric != "" {
			// If no tags specified, match any series for the metric by prefix.
			if tags == nil {
				if strings.HasPrefix(key, metric) {
					keys = append(keys, key)
				}
				continue
			}
			// Metric + tags: match series that contain all provided tag key/values
			if !strings.HasPrefix(key, metric) {
				continue
			}
			match := true
			for k, v := range tags {
				if !(contains(key, k) && contains(key, v)) {
					match = false
					break
				}
			}
			if match {
				keys = append(keys, key)
			}
			continue
		}
		if tags == nil {
			keys = append(keys, key)
			continue
		}
		match := true
		for k, v := range tags {
			if !(contains(key, k) && contains(key, v)) {
				match = false
				break
			}
		}
		if match {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	return strings.Contains(s, sub)
}

func (s *storageEngineShim) ForceFlush(ctx context.Context, wait bool) error         { return nil }
func (s *storageEngineShim) TriggerCompaction()                                      {}
func (s *storageEngineShim) CreateIncrementalSnapshot(snapshotsBaseDir string) error { return nil }
func (s *storageEngineShim) VerifyDataConsistency() []error                          { return nil }
func (s *storageEngineShim) CreateSnapshot(ctx context.Context) (string, error)      { return "", nil }
func (s *storageEngineShim) RestoreFromSnapshot(ctx context.Context, path string, overwrite bool) error {
	return nil
}
func (s *storageEngineShim) ApplyReplicatedEntry(ctx context.Context, entry *core.WALEntry) error {
	return nil
}
func (s *storageEngineShim) GetLatestAppliedSeqNum() uint64               { return 0 }
func (s *storageEngineShim) ReplaceWithSnapshot(snapshotDir string) error { return nil }
func (s *storageEngineShim) CleanupEngine()                               {}
func (s *storageEngineShim) GetSnapshotsBaseDir() string {
	return filepath.Join(s.dataDir, "snapshots")
}
func (s *storageEngineShim) GetDLQDir() string         { return filepath.Join(s.dataDir, "dlq") }
func (s *storageEngineShim) GetDataDir() string        { return s.dataDir }
func (s *storageEngineShim) GetSequenceNumber() uint64 { return 0 }

// testWAL implements the small subset of WAL methods used by tests: the
// ability to inject a testing-only append error.
type testWAL struct {
	mu       sync.Mutex
	injected error
}

func (w *testWAL) SetTestingOnlyInjectAppendError(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.injected = err
}

func (w *testWAL) AppendBatch(entries []core.WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.injected != nil {
		return w.injected
	}
	return nil
}

func (w *testWAL) Append(entry core.WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.injected != nil {
		return w.injected
	}
	return nil
}

func (s *storageEngineShim) GetWAL() interface{}   { return s.wal }
func (s *storageEngineShim) GetClock() clock.Clock { return s.clk }

// Tombstone helpers for tests
func (s *storageEngineShim) SetDeletedSeries(seriesKey []byte, seq uint64) {
	s.deletedSeriesMu.Lock()
	defer s.deletedSeriesMu.Unlock()
	s.deletedSeries[string(seriesKey)] = seq
}

func (s *storageEngineShim) IsSeriesDeleted(seriesKey []byte, dataPointSeq uint64) bool {
	s.deletedSeriesMu.RLock()
	defer s.deletedSeriesMu.RUnlock()
	dseq, ok := s.deletedSeries[string(seriesKey)]
	if !ok {
		return false
	}
	if dataPointSeq == 0 {
		return true
	}
	return dataPointSeq <= dseq
}

func (s *storageEngineShim) SetRangeTombstones(seriesKey []byte, tombs []core.RangeTombstone) {
	s.rangeTombstonesMu.Lock()
	defer s.rangeTombstonesMu.Unlock()
	s.rangeTombstones[string(seriesKey)] = tombs
}

func (s *storageEngineShim) IsCoveredByRangeTombstone(seriesKey []byte, ts int64, dataPointSeq uint64) bool {
	s.rangeTombstonesMu.RLock()
	tombs, ok := s.rangeTombstones[string(seriesKey)]
	s.rangeTombstonesMu.RUnlock()
	if !ok || len(tombs) == 0 {
		return false
	}
	// Find the tombstone with the highest SeqNum that covers ts
	var maxSeq uint64
	found := false
	for _, rt := range tombs {
		if ts >= rt.MinTimestamp && ts <= rt.MaxTimestamp {
			if !found || rt.SeqNum > maxSeq {
				maxSeq = rt.SeqNum
				found = true
			}
		}
	}
	if !found {
		return false
	}
	if dataPointSeq == 0 {
		return true
	}
	return dataPointSeq <= maxSeq
}
