package hooks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/INLOpen/nexusbase/core"
	"github.com/INLOpen/nexusbase/sstable"
)

// EventType defines the type of a hook event.
type EventType string

// --- Event Type Constants ---
const (
	// Data Lifecycle Events
	EventPrePutDataPoint  EventType = "PrePutDataPoint"
	EventPostPutDataPoint EventType = "PostPutDataPoint"
	EventPrePutBatch      EventType = "PrePutBatch"
	EventPostPutBatch     EventType = "PostPutBatch"
	EventPreGetPoint      EventType = "PreGetPoint"
	EventPostGetPoint     EventType = "PostGetPoint"
	EventPreDeletePoint   EventType = "PreDeletePoint"
	EventPostDeletePoint  EventType = "PostDeletePoint"
	EventPreDeleteSeries  EventType = "PreDeleteSeries"
	EventPostDeleteSeries EventType = "PostDeleteSeries"
	EventPreDeleteRange   EventType = "PreDeleteRange"
	EventPostDeleteRange  EventType = "PostDeleteRange"

	// Engine Lifecycle Events
	EventPreFlushMemtable  EventType = "PreFlushMemtable"
	EventPostFlushMemtable EventType = "PostFlushMemtable"
	EventPostCompaction    EventType = "PostCompaction"

	// Admin Lifecycle Events
	EventPreCompaction      EventType = "PreCompaction"
	EventPreCreateSnapshot  EventType = "PreCreateSnapshot"
	EventPostCreateSnapshot EventType = "PostCreateSnapshot"

	// Engine Internal Events
	EventPostSSTableCreate EventType = "PostSSTableCreate"
	EventPreSSTableDelete  EventType = "PreSSTableDelete"
	EventPostManifestWrite EventType = "PostManifestWrite"
	EventPreWALAppend      EventType = "PreWALAppend"
	EventPostWALAppend     EventType = "PostWALAppend"
	EventPostWALRotate     EventType = "PostWALRotate"
	EventPostWALRecovery   EventType = "PostWALRecovery"
	// Cache Events
	EventOnCacheHit      EventType = "OnCacheHit"
	EventOnCacheMiss     EventType = "OnCacheMiss"
	EventOnCacheEviction EventType = "OnCacheEviction"

	// Metadata & Indexing Events
	EventOnStringCreate EventType = "OnStringCreate"
	EventOnSeriesCreate EventType = "OnSeriesCreate"

	// --- New Event Types ---
	// Engine Lifecycle
	EventPreStartEngine  EventType = "PreStartEngine"
	EventPostStartEngine EventType = "PostStartEngine"
	EventPreCloseEngine  EventType = "PreCloseEngine"
	EventPostCloseEngine EventType = "PostCloseEngine"
	// Query Lifecycle
	EventPreQuery  EventType = "PreQuery"
	EventPostQuery EventType = "PostQuery"
)

// --- HookManager Interface and Implementation ---

// HookManager defines the interface for managing and triggering hooks.
type HookManager interface {
	// Register adds a listener for a specific event type.
	Register(eventType EventType, listener HookListener)
	// Trigger fires all registered listeners for a given event.
	// It handles synchronous vs. asynchronous execution based on the event type and listener preference.
	Trigger(ctx context.Context, event HookEvent) error
	// Stop waits for all asynchronous listeners to complete. Useful for graceful shutdown.
	Stop()
}

// HookEvent is the interface that all event objects must implement.
type HookEvent interface {
	// Type returns the type of the event.
	Type() EventType
	// Payload returns the data associated with the event.
	Payload() interface{}
}

// BaseEvent provides a base implementation for HookEvent.
type BaseEvent struct {
	eventType EventType
	payload   interface{}
}

func (e *BaseEvent) Type() EventType      { return e.eventType }
func (e *BaseEvent) Payload() interface{} { return e.payload }

// PrePutDataPointPayload contains the data for a PrePutDataPoint event.
// Fields are pointers to allow listeners to modify the data point before it's written.
type PrePutDataPointPayload struct {
	Metric    *string
	Tags      *map[string]string
	Timestamp *int64
	Fields    *core.FieldValues
}

// NewPrePutDataPointEvent creates a new event for before a data point is put.
func NewPrePutDataPointEvent(payload PrePutDataPointPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPrePutDataPoint,
		payload:   payload,
	}
}

// PostPutDataPointPayload contains the data for a PostPutDataPoint event.
type PostPutDataPointPayload struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Fields    core.FieldValues
}

// NewPostPutDataPointEvent creates a new event for after a data point is put.
func NewPostPutDataPointEvent(payload PostPutDataPointPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostPutDataPoint,
		payload:   payload,
	}
}

// PrePutBatchPayload contains the data for a PrePutBatch event.
// The Points field is a pointer to a slice of DataPoints, allowing listeners
// to modify the entire batch (e.g., add, remove, or change points) before it's processed.
type PrePutBatchPayload struct {
	Points *[]core.DataPoint
}

// NewPrePutBatchEvent creates a new event for before a batch of data points is put.
func NewPrePutBatchEvent(payload PrePutBatchPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPrePutBatch,
		payload:   payload,
	}
}

// PostPutBatchPayload contains the data for a PostPutBatch event.
type PostPutBatchPayload struct {
	Points []core.DataPoint
	Error  error // The final error state of the PutBatch operation.
}

// NewPostPutBatchEvent creates a new event for after a batch of data points is put.
func NewPostPutBatchEvent(payload PostPutBatchPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostPutBatch,
		payload:   payload,
	}
}

// PreGetPointPayload contains the data for a PreGetPoint event.
// Fields are pointers to allow listeners to modify the query parameters.
type PreGetPointPayload struct {
	Metric    *string
	Tags      *map[string]string
	Timestamp *int64
}

// NewPreGetPointEvent creates a new event for before a data point is retrieved.
func NewPreGetPointEvent(payload PreGetPointPayload) HookEvent {
	return &BaseEvent{eventType: EventPreGetPoint, payload: payload}
}

// PostGetPointPayload contains the data for a PostGetPoint event.
// The Result field is a pointer to allow listeners to modify the returned data.
type PostGetPointPayload struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Result    *core.FieldValues // Pointer to the result map
	Error     error
}

// NewPostGetPointEvent creates a new event for after a data point is retrieved.
func NewPostGetPointEvent(payload PostGetPointPayload) HookEvent {
	return &BaseEvent{eventType: EventPostGetPoint, payload: payload}
}

// PreDeletePointPayload contains the data for a PreDeletePoint event.
// Fields are pointers to allow listeners to modify the deletion parameters.
type PreDeletePointPayload struct {
	Metric    *string
	Tags      *map[string]string
	Timestamp *int64
}

// NewPreDeletePointEvent creates a new event for before a data point is deleted.
func NewPreDeletePointEvent(payload PreDeletePointPayload) HookEvent {
	return &BaseEvent{eventType: EventPreDeletePoint, payload: payload}
}

// PostDeletePointPayload contains the data for a PostDeletePoint event.
type PostDeletePointPayload struct {
	Metric    string
	Tags      map[string]string
	Timestamp int64
	Error     error
}

// NewPostDeletePointEvent creates a new event for after a data point is deleted.
func NewPostDeletePointEvent(payload PostDeletePointPayload) HookEvent {
	return &BaseEvent{eventType: EventPostDeletePoint, payload: payload}
}

// PreFlushMemtablePayload contains data for a PreFlushMemtable event.
type PreFlushMemtablePayload struct {
	// Currently no data, but can be extended.
}

// NewPreFlushMemtableEvent creates a new event for before a memtable is flushed.
func NewPreFlushMemtableEvent(payload PreFlushMemtablePayload) HookEvent {
	return &BaseEvent{
		eventType: EventPreFlushMemtable,
		payload:   payload,
	}
}

// NewPreDeleteSeriesEvent creates a new event for before a series is deleted.
func NewPreDeleteSeriesEvent(payload PreDeleteSeriesPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPreDeleteSeries,
		payload:   payload,
	}
}

// NewPostDeleteSeriesEvent creates a new event for after a series is deleted.
func NewPostDeleteSeriesEvent(payload PostDeleteSeriesPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostDeleteSeries,
		payload:   payload,
	}
}

// PostFlushMemtablePayload contains the data for a PostFlushMemtable event.
type PostFlushMemtablePayload struct {
	SSTable *sstable.SSTable
}

// NewPostFlushMemtableEvent creates a new event for after a memtable is flushed.
func NewPostFlushMemtableEvent(payload PostFlushMemtablePayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostFlushMemtable,
		payload:   payload,
	}
}

// PreCompactionPayload contains data for a PreCompaction event.
type PreCompactionPayload struct {
	SourceLevel int
	TargetLevel int
}

// NewPreCompactionEvent creates a new event for before a compaction starts.
func NewPreCompactionEvent(payload PreCompactionPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPreCompaction,
		payload:   payload,
	}
}

// CompactedTableInfo holds immutable data about an SSTable for use in hooks.
type CompactedTableInfo struct {
	ID   uint64
	Size int64
	Path string
}

// PostCompactionPayload contains data about a completed compaction.
type PostCompactionPayload struct {
	SourceLevel int
	TargetLevel int
	NewTables   []CompactedTableInfo
	OldTables   []CompactedTableInfo
}

// NewPostCompactionEvent creates a new event for after a compaction finishes.
func NewPostCompactionEvent(payload PostCompactionPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostCompaction,
		payload:   payload,
	}
}

// --- HookListener Interface ---

// HookListener defines the interface for components that want to listen to events.
type HookListener interface {
	// OnEvent is called by the HookManager when a registered event is triggered.
	// Returning an error from a "Pre" hook (e.g., PrePutDataPoint) can cancel the operation.
	// Errors from "Post" hooks are typically logged without affecting the main operation.
	OnEvent(ctx context.Context, event HookEvent) error

	// Priority returns the listener's priority. Lower numbers are executed first.
	Priority() int

	// IsAsync indicates if the listener should be called asynchronously for Post-events.
	IsAsync() bool
}

// PreCreateSnapshotPayload contains data for a PreCreateSnapshot event.
type PreCreateSnapshotPayload struct {
	SnapshotDir string
}

// NewPreCreateSnapshotEvent creates a new event for before a snapshot is created.
func NewPreCreateSnapshotEvent(payload PreCreateSnapshotPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPreCreateSnapshot,
		payload:   payload,
	}
}

// PostCreateSnapshotPayload contains data for a PostCreateSnapshot event.
type PostCreateSnapshotPayload struct {
	SnapshotDir  string
	ManifestPath string
}

// NewPostCreateSnapshotEvent creates a new event for after a snapshot is created.
func NewPostCreateSnapshotEvent(payload PostCreateSnapshotPayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostCreateSnapshot,
		payload:   payload,
	}
}

// SSTablePayload contains information about an SSTable for create/delete events.
type SSTablePayload struct {
	ID    uint64
	Level int
	Path  string
	Size  int64
}

// NewPostSSTableCreateEvent creates an event for after a new SSTable has been created and loaded.
func NewPostSSTableCreateEvent(payload SSTablePayload) HookEvent {
	return &BaseEvent{eventType: EventPostSSTableCreate, payload: payload}
}

// NewPreSSTableDeleteEvent creates an event for before an SSTable file is deleted from disk.
func NewPreSSTableDeleteEvent(payload SSTablePayload) HookEvent {
	return &BaseEvent{eventType: EventPreSSTableDelete, payload: payload}
}

// ManifestWritePayload contains information about a manifest write.
type ManifestWritePayload struct {
	Path string
}

// NewPostManifestWriteEvent creates an event for after a manifest file has been written.
func NewPostManifestWriteEvent(payload ManifestWritePayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostManifestWrite,
		payload:   payload,
	}
}

// WALAppendPayload contains the data for a Pre WALAppend event.
// For Pre-hooks, Entries is a pointer to allow modification.
type WALAppendPayload struct {
	Entries *[]core.WALEntry // Pointer for Pre-hook modification
}

// PostWALAppendPayload contains data after a WAL append operation.
type PostWALAppendPayload struct {
	Entries []core.WALEntry
	Error   error
}

// NewPreWALAppendEvent creates an event for before a batch of entries is appended to the WAL.
func NewPreWALAppendEvent(payload WALAppendPayload) HookEvent {
	return &BaseEvent{eventType: EventPreWALAppend, payload: payload}
}

// NewPostWALAppendEvent creates an event for after a batch of entries is appended to the WAL.
func NewPostWALAppendEvent(payload PostWALAppendPayload) HookEvent {
	return &BaseEvent{eventType: EventPostWALAppend, payload: payload}
}

// PostWALRotatePayload contains information about a WAL rotation.
type PostWALRotatePayload struct {
	OldSegmentIndex uint64
	NewSegmentIndex uint64
	NewSegmentPath  string
}

// NewPostWALRotateEvent creates an event for after the WAL has been rotated to a new segment.
func NewPostWALRotateEvent(payload PostWALRotatePayload) HookEvent {
	return &BaseEvent{eventType: EventPostWALRotate, payload: payload}
}

// PostWALRecoveryPayload contains information about a completed WAL recovery.
type PostWALRecoveryPayload struct {
	RecoveredEntriesCount int
	Duration              time.Duration
}

// NewPostWALRecoveryEvent creates an event for after WAL recovery is complete.
func NewPostWALRecoveryEvent(payload PostWALRecoveryPayload) HookEvent {
	return &BaseEvent{eventType: EventPostWALRecovery, payload: payload}
}

// CachePayload contains information for cache-related events.
type CachePayload struct {
	Key string
}

// NewOnCacheHitEvent creates an event for a cache hit.
func NewOnCacheHitEvent(payload CachePayload) HookEvent {
	return &BaseEvent{eventType: EventOnCacheHit, payload: payload}
}

// NewOnCacheMissEvent creates an event for a cache miss.
func NewOnCacheMissEvent(payload CachePayload) HookEvent {
	return &BaseEvent{eventType: EventOnCacheMiss, payload: payload}
}

// NewOnCacheEvictionEvent creates an event for a cache eviction.
func NewOnCacheEvictionEvent(payload CachePayload) HookEvent {
	return &BaseEvent{eventType: EventOnCacheEviction, payload: payload}
}

// --- Metadata & Indexing Payloads ---

// StringCreatePayload contains information about a newly created string-to-ID mapping.
type StringCreatePayload struct {
	Str string
	ID  uint64
}

// NewOnStringCreateEvent creates an event for when a new string is added to the dictionary.
func NewOnStringCreateEvent(payload StringCreatePayload) HookEvent {
	return &BaseEvent{eventType: EventOnStringCreate, payload: payload}
}

// SeriesCreatePayload contains information about a newly created series-to-ID mapping.
type SeriesCreatePayload struct {
	SeriesKey string
	SeriesID  uint64
}

// NewOnSeriesCreateEvent creates an event for when a new time series is first seen.
func NewOnSeriesCreateEvent(payload SeriesCreatePayload) HookEvent {
	return &BaseEvent{eventType: EventOnSeriesCreate, payload: payload}
}

// --- New Payloads and Event Creators ---

// EngineLifecyclePayload is used for engine start/close events.
// It's currently empty but can be extended.
type EngineLifecyclePayload struct{}

// NewPreStartEngineEvent creates an event for before the engine starts.
func NewPreStartEngineEvent() HookEvent {
	return &BaseEvent{eventType: EventPreStartEngine, payload: EngineLifecyclePayload{}}
}

// NewPostStartEngineEvent creates an event for after the engine has started.
func NewPostStartEngineEvent() HookEvent {
	return &BaseEvent{eventType: EventPostStartEngine, payload: EngineLifecyclePayload{}}
}

// NewPreCloseEngineEvent creates an event for before the engine closes.
func NewPreCloseEngineEvent() HookEvent {
	return &BaseEvent{eventType: EventPreCloseEngine, payload: EngineLifecyclePayload{}}
}

// NewPostCloseEngineEvent creates an event for after the engine has closed.
func NewPostCloseEngineEvent() HookEvent {
	return &BaseEvent{eventType: EventPostCloseEngine, payload: EngineLifecyclePayload{}}
}

// PreQueryPayload contains the query parameters before execution.
// The Params field is a pointer to allow listeners to modify the query.
type PreQueryPayload struct {
	Params *core.QueryParams
}

// NewPreQueryEvent creates an event for before a query is executed.
func NewPreQueryEvent(payload PreQueryPayload) HookEvent {
	return &BaseEvent{eventType: EventPreQuery, payload: payload}
}

// PostQueryPayload contains information after a query has executed.
type PostQueryPayload struct {
	Params   core.QueryParams
	Duration time.Duration
	Error    error
}

// NewPostQueryEvent creates an event for after a query has executed.
func NewPostQueryEvent(payload PostQueryPayload) HookEvent {
	return &BaseEvent{eventType: EventPostQuery, payload: payload}
}

// PreDeleteSeriesPayload contains the data for a PreDeleteSeries event.
// Fields are pointers to allow listeners to modify the data before deletion.
type PreDeleteSeriesPayload struct {
	Metric *string
	Tags   *map[string]string
}

// PostDeleteSeriesPayload contains the data for a PostDeleteSeries event.
type PostDeleteSeriesPayload struct {
	Metric    string
	Tags      map[string]string
	SeriesKey string // The internal, binary series key
}

// PreDeleteRangePayload contains the data for a PreDeleteRange event.
type PreDeleteRangePayload struct {
	Metric    *string
	Tags      *map[string]string
	StartTime *int64
	EndTime   *int64
}

// PostDeleteRangePayload contains the data for a PostDeleteRange event.
type PostDeleteRangePayload struct {
	Metric    string
	Tags      map[string]string
	SeriesKey string
	StartTime int64
	EndTime   int64
}

// listenerWithPriority wraps a listener with its priority for heap management.
type listenerWithPriority struct {
	listener HookListener
	priority int
}

// DefaultHookManager is a concrete implementation of HookManager.
type DefaultHookManager struct {
	// The map stores slices of listeners, kept sorted by priority.
	listeners map[EventType][]*listenerWithPriority
	mu        sync.RWMutex
	wg        sync.WaitGroup // For tracking async listeners
	logger    *slog.Logger
}

// NewHookManager creates a new DefaultHookManager.
func NewHookManager(logger *slog.Logger) HookManager {
	if logger == nil {
		// Default to a discard logger to prevent nil panics if no logger is provided.
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &DefaultHookManager{
		listeners: make(map[EventType][]*listenerWithPriority),
		logger:    logger,
	}
}

// Register adds a listener for a specific event type, maintaining priority order.
func (m *DefaultHookManager) Register(eventType EventType, listener HookListener) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item := &listenerWithPriority{
		listener: listener,
		priority: listener.Priority(),
	}

	// Get the existing slice of listeners for this event type.
	l := m.listeners[eventType]

	// Find the correct insertion index to maintain sorted order.
	// sort.Search finds the first index i where l[i].priority >= item.priority.
	idx := sort.Search(len(l), func(i int) bool {
		return l[i].priority >= item.priority
	})

	// Optimized insertion to reduce re-allocations.
	// Append a zero value to the slice, which might grow the slice once.
	l = append(l, nil)
	// Shift elements to make space for the new item.
	copy(l[idx+1:], l[idx:])
	// Insert the new item at the correct position.
	l[idx] = item // Insert the new item

	m.listeners[eventType] = l
}

// Trigger fires all registered listeners for a given event in priority order.
func (m *DefaultHookManager) Trigger(ctx context.Context, event HookEvent) error {
	m.mu.RLock()
	listeners, ok := m.listeners[event.Type()]
	m.mu.RUnlock()

	if !ok || len(listeners) == 0 {
		return nil
	}

	isPreHook := strings.HasPrefix(string(event.Type()), "Pre")

	for _, item := range listeners {
		isListenerAsync := item.listener.IsAsync()

		// Pre-hooks MUST be synchronous to allow for cancellation.
		// Post-hooks can be sync or async based on the listener's preference.
		if isPreHook || !isListenerAsync {
			// --- Synchronous Execution ---
			if isPreHook && isListenerAsync {
				m.logger.Warn("Listener for Pre-hook requested async execution, but Pre-hooks are always synchronous.", "event", event.Type(), "priority", item.priority)
			}

			if err := item.listener.OnEvent(ctx, event); err != nil {
				if isPreHook {
					// For Pre-hooks, the error is critical and cancels the operation.
					return fmt.Errorf("pre-hook for event %s (priority %d) failed: %w", event.Type(), item.priority, err)
				}
				// For synchronous Post-hooks, we just log the error and continue.
				m.logger.Error("Error from synchronous post-hook listener", "event", event.Type(), "priority", item.priority, "error", err)
			}
		} else {
			// --- Asynchronous Execution --- (Only for Post-hooks that return IsAsync() == true)
			m.wg.Add(1)
			// Pass item as an argument to the closure to capture its current value.
			go func(currentItem *listenerWithPriority) {
				defer m.wg.Done()
				if err := currentItem.listener.OnEvent(ctx, event); err != nil {
					m.logger.Error("Error from asynchronous post-hook listener", "event", event.Type(), "priority", currentItem.priority, "error", err)
				}
			}(item)
		}
	}
	return nil
}

// Stop waits for all asynchronous listeners to complete.
func (m *DefaultHookManager) Stop() {
	m.wg.Wait()
}

// NewPreDeleteRangeEvent creates a new event for before a data range is deleted.
func NewPreDeleteRangeEvent(payload PreDeleteRangePayload) HookEvent {
	return &BaseEvent{
		eventType: EventPreDeleteRange,
		payload:   payload,
	}
}

// NewPostDeleteRangeEvent creates a new event for after a data range is deleted.
func NewPostDeleteRangeEvent(payload PostDeleteRangePayload) HookEvent {
	return &BaseEvent{
		eventType: EventPostDeleteRange,
		payload:   payload,
	}
}
