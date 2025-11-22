package indexer

import (
	"log/slog"
	"os"
	"testing"

	"sync"

	"github.com/INLOpen/nexusbase/core"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

// TestEndToEnd_Indexer is a small, runnable end-to-end example that exercises
// the common path: StringStore -> TagIndexManager.Add -> memtable -> flush -> SSTable -> Query
func TestEndToEnd_Indexer(t *testing.T) {
	tempDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{DataDir: tempDir, MemtableThreshold: 10}

	nextId := func() uint64 { return 1 }

	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}

	// Ensure stores have initial files/headers
	deps.StringStore.LoadFromFile(tempDir)
	deps.SeriesIDStore.LoadFromFile(tempDir)

	tim, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("NewTagIndexManager failed: %v", err)
	}
	defer tim.Stop()

	// Create strings and add a series
	// Prepare commonly used strings
	deps.StringStore.GetOrCreateID("host")
	deps.StringStore.GetOrCreateID("serverX")
	deps.StringStore.GetOrCreateID("serverY")
	deps.StringStore.GetOrCreateID("region")
	deps.StringStore.GetOrCreateID("eu")
	deps.StringStore.GetOrCreateID("us")
	deps.StringStore.GetOrCreateID("env")
	deps.StringStore.GetOrCreateID("prod")
	deps.StringStore.GetOrCreateID("staging")

	// Create three series with different tag combinations:
	// s1: host=serverX, region=eu, env=prod
	// s2: host=serverY, region=eu, env=staging
	// s3: host=serverX, region=us, env=prod
	seriesKey1 := string(core.EncodeSeriesKeyWithString("metric.example", map[string]string{"id": "1"}))
	seriesKey2 := string(core.EncodeSeriesKeyWithString("metric.example", map[string]string{"id": "2"}))
	seriesKey3 := string(core.EncodeSeriesKeyWithString("metric.example", map[string]string{"id": "3"}))

	sid1, err := deps.SeriesIDStore.GetOrCreateID(seriesKey1)
	if err != nil {
		t.Fatalf("SeriesIDStore.GetOrCreateID failed for s1: %v", err)
	}
	sid2, err := deps.SeriesIDStore.GetOrCreateID(seriesKey2)
	if err != nil {
		t.Fatalf("SeriesIDStore.GetOrCreateID failed for s2: %v", err)
	}
	sid3, err := deps.SeriesIDStore.GetOrCreateID(seriesKey3)
	if err != nil {
		t.Fatalf("SeriesIDStore.GetOrCreateID failed for s3: %v", err)
	}

	if err := tim.Add(sid1, map[string]string{"host": "serverX", "region": "eu", "env": "prod"}); err != nil {
		t.Fatalf("tim.Add s1 failed: %v", err)
	}
	if err := tim.Add(sid2, map[string]string{"host": "serverY", "region": "eu", "env": "staging"}); err != nil {
		t.Fatalf("tim.Add s2 failed: %v", err)
	}
	if err := tim.Add(sid3, map[string]string{"host": "serverX", "region": "us", "env": "prod"}); err != nil {
		t.Fatalf("tim.Add s3 failed: %v", err)
	}

	// Force a flush of immutable memtables for deterministic test behavior
	tim.processImmutableIndexMemtables()

	// --- Single-tag queries (basic) ---
	bmHost, err := tim.Query(map[string]string{"host": "serverX"})
	if err != nil {
		t.Fatalf("tim.Query host failed: %v", err)
	}
	expectedHost := roaring64.BitmapOf(sid1, sid3)
	if !bmHost.Equals(expectedHost) {
		t.Fatalf("unexpected bitmap for host=serverX: got %v want %v", bmHost, expectedHost)
	}

	bmRegion, err := tim.Query(map[string]string{"region": "eu"})
	if err != nil {
		t.Fatalf("tim.Query region failed: %v", err)
	}
	expectedRegion := roaring64.BitmapOf(sid1, sid2)
	if !bmRegion.Equals(expectedRegion) {
		t.Fatalf("unexpected bitmap for region=eu: got %v want %v", bmRegion, expectedRegion)
	}

	// --- AND semantics: multiple tags passed to Query are intersected ---
	bmAnd, err := tim.Query(map[string]string{"host": "serverX", "region": "eu"})
	if err != nil {
		t.Fatalf("tim.Query AND failed: %v", err)
	}
	expectedAnd := roaring64.BitmapOf(sid1)
	if !bmAnd.Equals(expectedAnd) {
		t.Fatalf("unexpected AND result for host=serverX & region=eu: got %v want %v", bmAnd, expectedAnd)
	}

	// AND with three tags
	bmAnd3, err := tim.Query(map[string]string{"host": "serverX", "region": "eu", "env": "prod"})
	if err != nil {
		t.Fatalf("tim.Query 3-way AND failed: %v", err)
	}
	if !bmAnd3.Equals(expectedAnd) {
		t.Fatalf("unexpected 3-way AND result: got %v want %v", bmAnd3, expectedAnd)
	}

	// --- OR semantics: simulate by ORing results of separate queries ---
	// host=serverX OR region=eu should yield sid1, sid2, sid3
	orBitmap := bmHost.Clone()
	orBitmap.Or(bmRegion)
	expectedOr := roaring64.BitmapOf(sid1, sid2, sid3)
	if !orBitmap.Equals(expectedOr) {
		t.Fatalf("unexpected OR result for host=serverX OR region=eu: got %v want %v", orBitmap, expectedOr)
	}

	// env=prod AND host=serverX should return sid1 and sid3
	bmEnvHost, err := tim.Query(map[string]string{"env": "prod", "host": "serverX"})
	if err != nil {
		t.Fatalf("tim.Query env+host AND failed: %v", err)
	}
	expectedEnvHost := roaring64.BitmapOf(sid1, sid3)
	if !bmEnvHost.Equals(expectedEnvHost) {
		t.Fatalf("unexpected env+host result: got %v want %v", bmEnvHost, expectedEnvHost)
	}
}
