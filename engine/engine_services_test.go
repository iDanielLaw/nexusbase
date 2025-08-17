package engine

import (
	"expvar"
	"sync"
	"testing"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/indexer"
	"github.com/INLOpen/nexusbase/internal"
	"github.com/INLOpen/nexusbase/levels"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/trace"
)

// --- Mocks for testing ServiceManager ---

type mockCompactor struct {
	mock.Mock
	startCalled   bool
	stopCalled    bool
	triggerCalled bool
	mu            sync.Mutex
}

var _ CompactionManagerInterface = (*mockCompactor)(nil)

func (m *mockCompactor) Start(wg *sync.WaitGroup) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
}

func (m *mockCompactor) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
}

func (m *mockCompactor) Trigger() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.triggerCalled = true
}

func (m *mockCompactor) SetMetricsCounters(
	compactionCount *expvar.Int,
	compactionLatencyHist *expvar.Map,
	dataReadBytes *expvar.Int,
	dataWrittenBytes *expvar.Int,
	tablesMerged *expvar.Int,
) {
	m.Called(compactionCount, compactionLatencyHist, dataReadBytes, dataWrittenBytes, tablesMerged)
}

type mockTagIndexManager struct {
	startCalled bool
	stopCalled  bool
	mu          sync.Mutex
}

var _ indexer.TagIndexManagerInterface = (*mockTagIndexManager)(nil)

func (m *mockTagIndexManager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startCalled = true
}

func (m *mockTagIndexManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
}

// Implement other methods of TagIndexManagerInterface to satisfy the interface
func (m *mockTagIndexManager) Add(seriesID uint64, tags map[string]string) error { return nil }
func (m *mockTagIndexManager) AddEncoded(seriesID uint64, encodedTags []core.EncodedSeriesTagPair) error {
	return nil
}
func (m *mockTagIndexManager) RemoveSeries(seriesID uint64) {}
func (m *mockTagIndexManager) Query(tags map[string]string) (*roaring64.Bitmap, error) {
	return nil, nil
}
func (m *mockTagIndexManager) CreateSnapshot(snapshotDir string) error      { return nil }
func (m *mockTagIndexManager) RestoreFromSnapshot(snapshotDir string) error { return nil }
func (m *mockTagIndexManager) LoadFromFile(dataDir string) error            { return nil }
func (m *mockTagIndexManager) GetShutdownChain() chan struct{}              { return nil }
func (m *mockTagIndexManager) GetWaitGroup() *sync.WaitGroup                { return nil }
func (m *mockTagIndexManager) GetLevelsManager() levels.Manager             { return nil }

// Ensure mockTagIndexManager implements the internal interface as well
var _ internal.PrivateTagIndexManager = (*mockTagIndexManager)(nil)

// setupServiceManagerTest creates a minimal engine and a service manager for testing.
func setupServiceManagerTest(t *testing.T, opts StorageEngineOptions) (*storageEngine, *ServiceManager) {
	t.Helper()

	// Create a minimal engine with only the components needed by ServiceManager
	lm, _ := levels.NewLevelsManager(opts.MaxLevels, opts.MaxL0Files, opts.TargetSSTableSize, trace.NewNoopTracerProvider().Tracer("test"), opts.CompactionFallbackStrategy)

	eng := &storageEngine{
		opts:          opts,
		shutdownChan:  make(chan struct{}),
		flushChan:     make(chan struct{}, 1),
		logger:        opts.Logger,
		clock:         opts.Clock,
		levelsManager: lm,
	}

	// The service manager now has a metrics collection loop that calls back into the engine.
	// Ensure the metrics struct is initialized to prevent nil pointer dereferences.
	if opts.Metrics == nil {
		opts.Metrics = NewEngineMetrics(false, "servicemanager_test_")
	}
	eng.metrics = opts.Metrics

	// Set default implementations for function fields, which we will override in tests.
	eng.processImmutableMemtablesFunc = eng.processImmutableMemtables
	eng.triggerPeriodicFlushFunc = eng.triggerPeriodicFlush
	eng.syncMetadataFunc = eng.syncMetadata

	sm := NewServiceManager(eng)
	return eng, sm
}

func TestServiceManager_StartAndStop(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	eng, sm := setupServiceManagerTest(t, opts)

	// Create and assign mocks
	mockComp := &mockCompactor{}
	mockTim := &mockTagIndexManager{}
	eng.compactor = mockComp
	eng.tagIndexManager = mockTim

	// Action
	sm.Start()

	// Give goroutines a moment to start up
	time.Sleep(20 * time.Millisecond)

	// Verification (Start)
	mockComp.mu.Lock()
	assert.True(t, mockComp.startCalled, "Compactor.Start should have been called")
	mockComp.mu.Unlock()

	mockTim.mu.Lock()
	assert.True(t, mockTim.startCalled, "TagIndexManager.Start should have been called")
	mockTim.mu.Unlock()

	// Action (Stop)
	sm.Stop()

	// Verification (Stop)
	mockComp.mu.Lock()
	assert.True(t, mockComp.stopCalled, "Compactor.Stop should have been called")
	mockComp.mu.Unlock()

	mockTim.mu.Lock()
	assert.True(t, mockTim.stopCalled, "TagIndexManager.Stop should have been called")
	mockTim.mu.Unlock()

	// Verify shutdown channel is closed
	select {
	case <-eng.shutdownChan:
		// expected
	default:
		t.Error("shutdownChan was not closed")
	}
}

func TestServiceManager_FlushLoop(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.MemtableFlushIntervalMs = 50 // 50ms interval for testing

	eng, sm := setupServiceManagerTest(t, opts)

	// Add detectors to the engine struct to see if methods are called
	processImmCalled := make(chan bool, 1)
	triggerPeriodicCalled := make(chan bool, 1)

	// Override the real methods with our detectors
	eng.processImmutableMemtablesFunc = func(writeCheckpoint bool) {
		processImmCalled <- true
	}
	eng.triggerPeriodicFlushFunc = func() {
		triggerPeriodicCalled <- true
	}

	// Start the service manager, which starts the flush loop
	sm.Start()
	defer sm.Stop()

	t.Run("ManualFlushSignal", func(t *testing.T) {
		eng.flushChan <- struct{}{}
		select {
		case <-processImmCalled:
			// success
		case <-time.After(100 * time.Millisecond):
			t.Fatal("processImmutableMemtables was not called after signaling flushChan")
		}
	})

	t.Run("PeriodicFlushSignal", func(t *testing.T) {
		select {
		case <-triggerPeriodicCalled:
			// success
		case <-time.After(150 * time.Millisecond): // Wait for longer than the 50ms interval
			t.Fatal("triggerPeriodicFlush was not called after time interval")
		}
	})
}

func TestServiceManager_MetadataSyncLoop(t *testing.T) {
	opts := getBaseOptsForFlushTest(t)
	opts.MetadataSyncIntervalSeconds = 1 // 1 second interval for testing

	eng, sm := setupServiceManagerTest(t, opts)

	// Add a detector
	syncMetaCalled := make(chan bool, 1)
	eng.syncMetadataFunc = func() {
		syncMetaCalled <- true
	}

	sm.Start()
	defer sm.Stop()

	select {
	case <-syncMetaCalled:
		// success
	case <-time.After(1500 * time.Millisecond): // Wait for longer than the 1s interval
		t.Fatal("syncMetadata was not called after time interval")
	}
}
