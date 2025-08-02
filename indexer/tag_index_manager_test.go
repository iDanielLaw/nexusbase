package indexer

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/RoaringBitmap/roaring/roaring64"
	"go.opentelemetry.io/otel/trace"
)

func nextIDBuilder() core.SSTableNextIDFactory {
	var id atomic.Uint64
	return func() uint64 {
		return id.Add(1)
	}
}

func TestTagIndexManager_Persistence(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:           tempDir,
		MemtableThreshold: 100, // High threshold to prevent auto-flush
	}

	nextId := nextIDBuilder()

	// Setup dependencies
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.StringStore.LoadFromFile(tempDir)
	deps.SeriesIDStore.LoadFromFile(tempDir)

	// --- Phase 1: Create, populate, and close the index manager ---

	tim1, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager (1): %v", err)
	}

	// Helper to add data and ensure strings are in the store first,
	// simulating the behavior of the main StorageEngine.
	addData := func(seriesID uint64, tags map[string]string) {
		for k, v := range tags {
			deps.StringStore.GetOrCreateID(k)
			deps.StringStore.GetOrCreateID(v)
		}
		tim1.Add(seriesID, tags)
	}

	// Add some data
	addData(1, map[string]string{"host": "serverA"})
	addData(2, map[string]string{"host": "serverB"})
	addData(3, map[string]string{"region": "us-east"})
	addData(1, map[string]string{"region": "us-east"}) // Series 1 also has this tag

	// Stop will trigger a final flush
	tim1.Stop()

	// --- Phase 2: Reopen and verify the state ---

	tim2, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager (2): %v", err)
	}
	defer tim2.Stop()

	// Verify that the index was loaded from the manifest
	if tim2.levelsManager.GetTotalTableCount() == 0 {
		t.Fatal("Expected index SSTables to be loaded from manifest, but none were found.")
	}

	// Query for the data
	// host=serverA should have series 1
	bmA, err := tim2.Query(map[string]string{"host": "serverA"})
	if err != nil {
		t.Fatalf("Query for host=serverA failed: %v", err)
	}
	expectedA := roaring64.BitmapOf(1)
	if !bmA.Equals(expectedA) {
		t.Errorf("Bitmap for host=serverA mismatch. Got %v, want %v", bmA, expectedA)
	}

	// region=us-east should have series 1 and 3
	bmRegion, err := tim2.Query(map[string]string{"region": "us-east"})
	if err != nil {
		t.Fatalf("Query for region=us-east failed: %v", err)
	}
	expectedRegion := roaring64.BitmapOf(1, 3)
	if !bmRegion.Equals(expectedRegion) {
		t.Errorf("Bitmap for region=us-east mismatch. Got %v, want %v", bmRegion, expectedRegion)
	}

}

func TestTagIndexManager_AddEncoded(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:           tempDir,
		MemtableThreshold: 100, // High threshold to prevent auto-flush
	}

	nextId := nextIDBuilder()

	// Setup dependencies
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.StringStore.LoadFromFile(tempDir)
	deps.SeriesIDStore.LoadFromFile(tempDir)

	tim, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager: %v", err)
	}
	defer tim.Stop()

	// Manually encode some tags
	hostID, _ := deps.StringStore.GetOrCreateID("host")
	serverAID, _ := deps.StringStore.GetOrCreateID("serverA")
	regionID, _ := deps.StringStore.GetOrCreateID("region")
	usEastID, _ := deps.StringStore.GetOrCreateID("us-east")

	encodedTags1 := []core.EncodedSeriesTagPair{
		{KeyID: hostID, ValueID: serverAID},
		{KeyID: regionID, ValueID: usEastID},
	}

	// Add the encoded data
	seriesID1 := uint64(1)
	if err := tim.AddEncoded(seriesID1, encodedTags1); err != nil {
		t.Fatalf("AddEncoded failed: %v", err)
	}

	// Verify by querying
	bm, err := tim.Query(map[string]string{"host": "serverA", "region": "us-east"})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	expectedBitmap := roaring64.BitmapOf(seriesID1)
	if !bm.Equals(expectedBitmap) {
		t.Errorf("Bitmap mismatch after AddEncoded. Got %v, want %v", bm, expectedBitmap)
	}
}

func mustGetStringID(stringStore StringStoreInterface, s string) uint64 {
	id, ok := stringStore.GetID(s)
	if !ok {
		// In tests, it's often easier to create it if it doesn't exist
		newID, err := stringStore.GetOrCreateID(s)
		if err != nil {
			panic(fmt.Sprintf("failed to get or create string '%s': %v", s, err))
		}
		return newID
	}
	return id
}

func TestTagIndexManager_Compaction_L1ToL2(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	nextId := nextIDBuilder()

	// --- Setup a mock engine ---
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.SeriesIDStore.LoadFromFile(tempDir)
	deps.StringStore.LoadFromFile(tempDir)

	// --- Setup TagIndexManager ---
	opts := TagIndexManagerOptions{
		DataDir:                    tempDir,
		MemtableThreshold:          1000, // High threshold
		MaxL0Files:                 10,
		CompactionIntervalSeconds:  3600,
		LevelsTargetSizeMultiplier: 2, // Important for LN compaction check
	}
	tim, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager: %v", err)
	}
	defer tim.Stop()

	// Pre-populate the seriesIDStore for the series we are going to use in the test.
	// This simulates the main engine having already assigned IDs to these series.
	deps.SeriesIDStore.GetOrCreateID("dummy_series_for_id_1") // Will be ID 1
	deps.SeriesIDStore.GetOrCreateID("dummy_series_for_id_2") // Will be ID 2
	deps.SeriesIDStore.GetOrCreateID("dummy_series_for_id_3") // Will be ID 3

	// Manually set a small base target size to trigger compaction easily
	concretePrivateLvMgr := tim.GetLevelsManager().(internal.PrivateLevelManager)
	concretePrivateLvMgr.SetBaseTargetSize(10) // Very small target size

	// --- Phase 1: Create SSTables and place them in L1 and L2 ---
	// L1 table to be compacted (its size will be > 10 bytes)
	memL1 := NewIndexMemtable()
	memL1.Add(mustGetStringID(deps.StringStore, "host"), mustGetStringID(deps.StringStore, "A"), 1)
	memL1.Add(mustGetStringID(deps.StringStore, "host"), mustGetStringID(deps.StringStore, "B"), 2)
	sstL1, err := tim.flushIndexMemtableToSSTable(memL1)
	if err != nil {
		t.Fatalf("Failed to flush L1 table: %v", err)
	}
	tim.levelsManager.AddTableToLevel(1, sstL1)

	// L2 table that overlaps with sstL1
	memL2_overlap := NewIndexMemtable()
	memL2_overlap.Add(mustGetStringID(deps.StringStore, "host"), mustGetStringID(deps.StringStore, "B"), 3)
	sstL2_overlap, err := tim.flushIndexMemtableToSSTable(memL2_overlap)
	if err != nil {
		t.Fatalf("Failed to flush L2 overlap table: %v", err)
	}
	tim.levelsManager.AddTableToLevel(2, sstL2_overlap)

	// --- Phase 2: Trigger and verify compaction ---
	if !tim.levelsManager.NeedsLevelNCompaction(1, opts.LevelsTargetSizeMultiplier) {
		t.Fatalf("Expected L1 to need compaction, but it doesn't. Size: %d, Target: %d", tim.levelsManager.GetTotalSizeForLevel(1), concretePrivateLvMgr.GetBaseTargetSize())
	}
	if err := tim.compactIndexLevelNToLevelNPlus1(1); err != nil {
		t.Fatalf("compactIndexLevelNToLevelNPlus1 failed: %v", err)
	}

	// --- Phase 3: Verification ---
	if count := len(tim.levelsManager.GetTablesForLevel(1)); count != 0 {
		t.Errorf("Expected L1 to be empty, got %d tables", count)
	}

	// Verify content by querying
	bmB, _ := tim.Query(map[string]string{"host": "B"})
	if !bmB.Equals(roaring64.BitmapOf(2, 3)) {
		t.Errorf("Bitmap for host=B mismatch. Got %v, want %v", bmB, roaring64.BitmapOf(2, 3))
	}
}

func TestTagIndexManager_Compaction_WithDeletions(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")
	nextId := nextIDBuilder()

	// --- Setup a mock engine with pre-populated stores ---
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.SeriesIDStore.LoadFromFile(tempDir)
	deps.StringStore.LoadFromFile(tempDir)

	// Helper to get string ID, panics on failure for test simplicity
	mustGetStringID := func(s string) uint64 {
		id, ok := deps.StringStore.GetID(s)
		if !ok {
			newID, _ := deps.StringStore.GetOrCreateID(s)
			return newID
		}
		return id
	}

	// Pre-populate series and string IDs
	seriesA_ID, _ := deps.SeriesIDStore.GetOrCreateID("seriesA") // ID 1
	seriesB_ID, _ := deps.SeriesIDStore.GetOrCreateID("seriesB") // ID 2
	seriesC_ID, _ := deps.SeriesIDStore.GetOrCreateID("seriesC") // ID 3
	deps.StringStore.GetOrCreateID("region")
	deps.StringStore.GetOrCreateID("us")
	deps.StringStore.GetOrCreateID("eu")

	// Mark series B as deleted
	deps.DeletedSeries["seriesB"] = 100

	// --- Setup TagIndexManager ---
	opts := TagIndexManagerOptions{
		DataDir:           tempDir,
		MemtableThreshold: 100, // High threshold
	}
	tim, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager: %v", err)
	}
	defer tim.Stop()

	// --- Create L0 files manually ---
	// File 1
	mem1 := NewIndexMemtable()
	mem1.Add(mustGetStringID("region"), mustGetStringID("us"), seriesA_ID)
	mem1.Add(mustGetStringID("region"), mustGetStringID("us"), seriesB_ID) // Add deleted series
	mem1.Add(mustGetStringID("region"), mustGetStringID("us"), seriesC_ID)
	sst1, err := tim.flushIndexMemtableToSSTable(mem1)
	if err != nil || sst1 == nil {
		t.Fatalf("Failed to flush mem1: %v", err)
	}
	tim.levelsManager.AddL0Table(sst1)

	// --- Trigger Compaction ---
	if err := tim.compactIndexL0ToL1(); err != nil {
		t.Fatalf("compactIndexL0ToL1 failed: %v", err)
	}

	// --- Verification ---
	l1Tables := tim.levelsManager.GetTablesForLevel(1)
	if len(l1Tables) != 1 {
		t.Fatalf("Expected 1 table in L1, got %d", len(l1Tables))
	}
	l1Table := l1Tables[0]

	// Check the bitmap for "region=us" in the new L1 table
	regionUsKey := encodeTagIndexKey(mustGetStringID("region"), mustGetStringID("us"))
	valueBytes, _, err := l1Table.Get(regionUsKey)
	if err != nil {
		t.Fatalf("Failed to get key for region=us from L1 table: %v", err)
	}

	resultBitmap := roaring64.New()
	resultBitmap.ReadFrom(bytes.NewReader(valueBytes))

	expectedBitmap := roaring64.BitmapOf(seriesA_ID, seriesC_ID) // Should only contain A
	if !resultBitmap.Equals(expectedBitmap) {
		t.Errorf("Bitmap for region=us mismatch. Got %v, want %v", resultBitmap, expectedBitmap)
	}
}

func TestTagIndexManager_Compaction(t *testing.T) {
	tempDir := t.TempDir()
	logger := newTestLogger()
	tracer := trace.NewNoopTracerProvider().Tracer("test")

	opts := TagIndexManagerOptions{
		DataDir:                   tempDir,
		MemtableThreshold:         2,    // Flush after 2 bitmaps are created
		MaxL0Files:                2,    // Compact after 2 L0 files are created
		CompactionIntervalSeconds: 3600, // Disable auto-compaction for test predictability
	}

	// Setup a mock engine because compaction logic needs it to check for deleted series
	// and to map series IDs back to keys.
	nextId := nextIDBuilder()
	deps := &TagIndexDependencies{
		StringStore:     NewStringStore(logger, nil),
		SeriesIDStore:   NewSeriesIDStore(logger, nil),
		DeletedSeries:   make(map[string]uint64),
		DeletedSeriesMu: &sync.RWMutex{},
		SSTNextID:       nextId,
	}
	deps.SeriesIDStore.LoadFromFile(tempDir)
	deps.StringStore.LoadFromFile(tempDir)

	tim, err := NewTagIndexManager(opts, deps, logger, tracer)
	if err != nil {
		t.Fatalf("Failed to create TagIndexManager: %v", err)
	}
	defer tim.Stop()

	// Pre-populate the seriesIDStore for the series we are going to use in the test.
	// This simulates the main engine having already assigned IDs to these series.
	series1Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "1"}))
	series2Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "2"}))
	series3Key := string(core.EncodeSeriesKeyWithString("metric.compaction", map[string]string{"id": "3"}))
	deps.SeriesIDStore.GetOrCreateID(series1Key) // Will be ID 1
	deps.SeriesIDStore.GetOrCreateID(series2Key) // Will be ID 2
	deps.SeriesIDStore.GetOrCreateID(series3Key) // Will be ID 3

	// Helper to add data and ensure strings are in the store first
	addData := func(seriesID uint64, tags map[string]string) {
		for k, v := range tags {
			deps.StringStore.GetOrCreateID(k)
			deps.StringStore.GetOrCreateID(v)
		}
		tim.Add(seriesID, tags)
	}

	// --- Phase 1: Create 2 L0 files ---
	// File 1
	addData(1, map[string]string{"host": "serverA"})   // Bitmap 1
	addData(2, map[string]string{"region": "us-east"}) // Bitmap 2, triggers flush
	tim.processImmutableIndexMemtables()               // Manually trigger flush for test predictability

	// File 2
	addData(1, map[string]string{"region": "us-east"}) // Bitmap 3 (host=serverA already exists)
	addData(3, map[string]string{"host": "serverB"})   // Bitmap 4, triggers flush
	tim.processImmutableIndexMemtables()               // Manually trigger flush

	if tim.levelsManager.GetTotalTableCount() != 2 {
		t.Fatalf("Expected 2 L0 tables before compaction, got %d", tim.levelsManager.GetTotalTableCount())
	}

	// --- Phase 2: Trigger and verify compaction ---
	// Manually call the compaction logic for a synchronous test
	if err := tim.compactIndexL0ToL1(); err != nil {
		t.Fatalf("compactIndexL0ToL1 failed: %v", err)
	}

	// --- Phase 3: Verification ---
	if count := len(tim.levelsManager.GetTablesForLevel(0)); count != 0 {
		t.Errorf("Expected L0 to be empty after compaction, got %d tables", count)
	}
	l1Tables := tim.levelsManager.GetTablesForLevel(1)
	if len(l1Tables) != 1 {
		t.Fatalf("Expected 1 table in L1 after compaction, got %d", len(l1Tables))
	}

	// Verify content of the new L1 table
	// Query for region=us-east, which should have series 1 and 2
	bmRegion, err := tim.Query(map[string]string{"region": "us-east"})
	if err != nil {
		t.Fatalf("Query for region=us-east failed after compaction: %v", err)
	}
	expectedRegion := roaring64.BitmapOf(1, 2)
	if !bmRegion.Equals(expectedRegion) {
		t.Errorf("Bitmap for region=us-east mismatch after compaction. Got %v, want %v", bmRegion, expectedRegion)
	}
}

// newTestLogger is a helper to create a logger for tests.
func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
