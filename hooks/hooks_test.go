package hooks

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockListener is a mock implementation of HookListener for testing.
type mockListener struct {
	priority int
	// A channel to signal when OnEvent is called, for async tests.
	callSignal chan string
	// A slice to record the order of calls, for sync tests.
	callOrder *[]string
	// The name of this listener, to be recorded in callOrder.
	name string
	// An error to return from OnEvent, for error handling tests.
	returnErr error
	// Whether the listener should run asynchronously.
	isAsync bool
	// A function to be executed inside OnEvent, for payload modification tests.
	onEventFunc func(event HookEvent)
	// A delay to simulate work.
	workDelay time.Duration
}

func (m *mockListener) OnEvent(ctx context.Context, event HookEvent) error {
	if m.workDelay > 0 {
		time.Sleep(m.workDelay)
	}
	if m.onEventFunc != nil {
		m.onEventFunc(event)
	}
	if m.callOrder != nil {
		*m.callOrder = append(*m.callOrder, m.name)
	}
	if m.callSignal != nil {
		m.callSignal <- m.name
	}
	return m.returnErr
}

func (m *mockListener) Priority() int {
	return m.priority
}

func (m *mockListener) IsAsync() bool {
	return m.isAsync
}

// TestNewHookManager ensures the manager is initialized correctly.
func TestNewHookManager(t *testing.T) {
	manager := NewHookManager(nil)
	if manager == nil {
		t.Fatal("NewHookManager returned nil")
	}
	defaultManager, ok := manager.(*DefaultHookManager)
	if !ok {
		t.Fatalf("NewHookManager did not return a *DefaultHookManager")
	}
	if defaultManager.listeners == nil {
		t.Error("Expected listeners map to be initialized, but it was nil")
	}
	if defaultManager.logger == nil {
		t.Error("Expected logger to be initialized, but it was nil")
	}
}

func TestDefaultHookManager_Register(t *testing.T) {
	t.Run("should register listeners in priority order", func(t *testing.T) {
		manager := NewHookManager(nil).(*DefaultHookManager)

		listener1 := &mockListener{name: "listener1", priority: 10}
		listener2 := &mockListener{name: "listener2", priority: 1}
		listener3 := &mockListener{name: "listener3", priority: 5}

		eventType := EventPrePutDataPoint

		manager.Register(eventType, listener1)
		manager.Register(eventType, listener2)
		manager.Register(eventType, listener3)

		listeners := manager.listeners[eventType]
		if len(listeners) != 3 {
			t.Fatalf("Expected 3 listeners to be registered, got %d", len(listeners))
		}

		// Check order
		if listeners[0].listener.(*mockListener).name != "listener2" {
			t.Errorf("Expected listener with priority 1 to be first, got %s", listeners[0].listener.(*mockListener).name)
		}
		if listeners[1].listener.(*mockListener).name != "listener3" {
			t.Errorf("Expected listener with priority 5 to be second, got %s", listeners[1].listener.(*mockListener).name)
		}
		if listeners[2].listener.(*mockListener).name != "listener1" {
			t.Errorf("Expected listener with priority 10 to be third, got %s", listeners[2].listener.(*mockListener).name)
		}
	})
}

func TestDefaultHookManager_Trigger(t *testing.T) {
	t.Run("PreHook", func(t *testing.T) {
		t.Run("should execute in priority order synchronously", func(t *testing.T) {
			manager := NewHookManager(nil)
			callOrder := make([]string, 0)

			listener1 := &mockListener{name: "listener1", priority: 10, callOrder: &callOrder}
			listener2 := &mockListener{name: "listener2", priority: 1, callOrder: &callOrder}
			listener3 := &mockListener{name: "listener3", priority: 5, callOrder: &callOrder}

			eventType := EventPrePutDataPoint
			manager.Register(eventType, listener1)
			manager.Register(eventType, listener2)
			manager.Register(eventType, listener3)

			event := NewPrePutDataPointEvent(PrePutDataPointPayload{})
			err := manager.Trigger(context.Background(), event)

			if err != nil {
				t.Fatalf("Trigger returned an unexpected error: %v", err)
			}

			expectedOrder := []string{"listener2", "listener3", "listener1"}
			if len(callOrder) != len(expectedOrder) {
				t.Fatalf("Expected %d listeners to be called, but %d were", len(expectedOrder), len(callOrder))
			}

			for i, name := range expectedOrder {
				if callOrder[i] != name {
					t.Errorf("Call order mismatch at index %d. Got %s, want %s", i, callOrder[i], name)
				}
			}
		})

		t.Run("should stop execution and return error on failure", func(t *testing.T) {
			manager := NewHookManager(nil)
			callOrder := make([]string, 0)
			simulatedErr := errors.New("simulated error")

			listener1 := &mockListener{name: "listener1_p10", priority: 10, callOrder: &callOrder}
			listener2 := &mockListener{name: "listener2_p1", priority: 1, callOrder: &callOrder}
			listener3_err := &mockListener{name: "listener3_p5_err", priority: 5, callOrder: &callOrder, returnErr: simulatedErr}

			eventType := EventPrePutDataPoint
			manager.Register(eventType, listener1)
			manager.Register(eventType, listener2)
			manager.Register(eventType, listener3_err)

			event := NewPrePutDataPointEvent(PrePutDataPointPayload{})
			err := manager.Trigger(context.Background(), event)

			if err == nil {
				t.Fatal("Trigger was expected to return an error, but got nil")
			}
			if !errors.Is(err, simulatedErr) {
				t.Fatalf("Trigger returned wrong error. Got %v, want %v", err, simulatedErr)
			}

			expectedOrder := []string{"listener2_p1", "listener3_p5_err"}
			if len(callOrder) != len(expectedOrder) {
				t.Fatalf("Expected %d listeners to be called, but %d were. Called: %v", len(expectedOrder), len(callOrder), callOrder)
			}
		})

		t.Run("should allow payload modification", func(t *testing.T) {
			manager := NewHookManager(nil)
			originalMetric := "cpu_usage"
			newMetric := "cpu_usage_modified"

			modifierListener := &mockListener{
				name:     "modifier",
				priority: 1,
				onEventFunc: func(event HookEvent) {
					if p, ok := event.Payload().(PrePutDataPointPayload); ok {
						*p.Metric = newMetric
					}
				},
			}
			manager.Register(EventPrePutDataPoint, modifierListener)

			payload := PrePutDataPointPayload{Metric: &originalMetric}
			event := NewPrePutDataPointEvent(payload)
			err := manager.Trigger(context.Background(), event)

			if err != nil {
				t.Fatalf("Trigger returned an unexpected error: %v", err)
			}

			if *payload.Metric != newMetric {
				t.Errorf("Expected payload metric to be modified. Got %s, want %s", *payload.Metric, newMetric)
			}
		})

		t.Run("should ignore async flag and run synchronously", func(t *testing.T) {
			manager := NewHookManager(nil)
			callOrder := make([]string, 0)
			listener := &mockListener{name: "pre_hook_async_request", priority: 1, isAsync: true, callOrder: &callOrder}
			manager.Register(EventPrePutDataPoint, listener)

			event := NewPrePutDataPointEvent(PrePutDataPointPayload{})
			err := manager.Trigger(context.Background(), event)
			if err != nil {
				t.Fatalf("Trigger returned an unexpected error: %v", err)
			}

			if len(callOrder) != 1 || callOrder[0] != "pre_hook_async_request" {
				t.Errorf("Expected pre-hook to run synchronously despite async flag. Call order: %v", callOrder)
			}
		})
	})

	t.Run("PostHook", func(t *testing.T) {
		t.Run("should execute async and sync listeners correctly", func(t *testing.T) {
			manager := NewHookManager(nil)
			signalChan := make(chan string, 1)
			callOrder := make([]string, 0)

			listenerAsync := &mockListener{name: "post_listener_async", priority: 10, isAsync: true, callSignal: signalChan}
			listenerSync := &mockListener{name: "post_listener_sync", priority: 1, isAsync: false, callOrder: &callOrder}
			manager.Register(EventPostPutDataPoint, listenerAsync)
			manager.Register(EventPostPutDataPoint, listenerSync)

			event := NewPostPutDataPointEvent(PostPutDataPointPayload{})
			err := manager.Trigger(context.Background(), event)

			if err != nil {
				t.Fatalf("Trigger returned an unexpected error for post-hook: %v", err)
			}

			if len(callOrder) != 1 || callOrder[0] != "post_listener_sync" {
				t.Errorf("Expected synchronous listener to be called immediately. Got call order: %v", callOrder)
			}

			select {
			case name := <-signalChan:
				if name != "post_listener_async" {
					t.Errorf("Received signal from wrong listener. Got %s", name)
				}
			case <-time.After(100 * time.Millisecond):
				t.Fatal("Timed out waiting for async listener to be called")
			}
			manager.Stop()
		})

		t.Run("should not return error from sync listener and continue execution", func(t *testing.T) {
			manager := NewHookManager(nil)
			callOrder := make([]string, 0)
			simulatedErr := errors.New("post hook error")

			listener1 := &mockListener{name: "listener1_p1_err", priority: 1, callOrder: &callOrder, returnErr: simulatedErr, isAsync: false}
			listener2 := &mockListener{name: "listener2_p5", priority: 5, callOrder: &callOrder, isAsync: false}
			manager.Register(EventPostPutDataPoint, listener1)
			manager.Register(EventPostPutDataPoint, listener2)

			event := NewPostPutDataPointEvent(PostPutDataPointPayload{})
			err := manager.Trigger(context.Background(), event)

			if err != nil {
				t.Fatalf("Trigger should not return error for post-hook failures, but got: %v", err)
			}

			expectedOrder := []string{"listener1_p1_err", "listener2_p5"}
			if len(callOrder) != len(expectedOrder) {
				t.Fatalf("Expected all listeners to be called. Got %d, want %d. Called: %v", len(callOrder), len(expectedOrder), callOrder)
			}
		})
	})

	t.Run("General", func(t *testing.T) {
		t.Run("should do nothing for event with no listeners", func(t *testing.T) {
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			manager := NewHookManager(logger)
			event := NewPrePutDataPointEvent(PrePutDataPointPayload{})

			err := manager.Trigger(context.Background(), event)
			if err != nil {
				t.Fatalf("Trigger returned an unexpected error when no listeners are registered: %v", err)
			}
		})
	})
}

func TestDefaultHookManager_Stop(t *testing.T) {
	t.Run("should wait for async listeners to complete", func(t *testing.T) {
		manager := NewHookManager(nil)
		var listenerCompleted atomic.Bool
		delay := 50 * time.Millisecond

		listener := &mockListener{
			name:      "slow_async_listener",
			priority:  1,
			isAsync:   true,
			workDelay: delay,
			onEventFunc: func(event HookEvent) {
				listenerCompleted.Store(true)
			},
		}
		manager.Register(EventPostPutDataPoint, listener)

		event := NewPostPutDataPointEvent(PostPutDataPointPayload{})
		_ = manager.Trigger(context.Background(), event)

		stopDone := make(chan struct{})
		startTime := time.Now()
		go func() {
			manager.Stop()
			close(stopDone)
		}()

		select {
		case <-stopDone:
			duration := time.Since(startTime)
			if duration < delay {
				t.Errorf("Stop() returned too quickly. Expected to block for at least %v, but took %v", delay, duration)
			}
		case <-time.After(delay * 2):
			t.Fatal("Timed out waiting for Stop() to return")
		}

		if !listenerCompleted.Load() {
			t.Error("Listener did not complete its work before Stop() returned")
		}
	})
}

// waitTimeout is a helper function to wait for a WaitGroup with a timeout.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration, t *testing.T) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		// WaitGroup finished.
	case <-time.After(timeout):
		t.Fatal("Timed out waiting for listeners to be called")
	}
}

// --- Benchmarks ---

func BenchmarkTrigger_PreHook_1_Listener(b *testing.B) {
	manager := NewHookManager(nil)
	listener := &mockListener{name: "l1", priority: 1}
	manager.Register(EventPrePutDataPoint, listener)
	event := NewPrePutDataPointEvent(PrePutDataPointPayload{})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = manager.Trigger(ctx, event)
	}
}

func BenchmarkTrigger_PreHook_10_Listeners(b *testing.B) {
	manager := NewHookManager(nil)
	for i := 0; i < 10; i++ {
		listener := &mockListener{name: "l", priority: i}
		manager.Register(EventPrePutDataPoint, listener)
	}
	event := NewPrePutDataPointEvent(PrePutDataPointPayload{})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = manager.Trigger(ctx, event)
	}
}

func BenchmarkTrigger_PostHook_10_Listeners(b *testing.B) {
	manager := NewHookManager(nil)
	// For post-hooks, the actual listener work is async.
	// This benchmark measures the overhead of the Trigger function itself,
	// which should be low as it only launches goroutines.
	for i := 0; i < 10; i++ {
		listener := &mockListener{name: "l", priority: i, isAsync: true}
		manager.Register(EventPostPutDataPoint, listener)
	}
	event := NewPostPutDataPointEvent(PostPutDataPointPayload{})
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = manager.Trigger(ctx, event)
	}
}

func BenchmarkRegister(b *testing.B) {
	eventType := EventPrePutDataPoint

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// This benchmarks the registration process, which involves sorting.
		manager := NewHookManager(nil)
		for j := 0; j < 100; j++ {
			// Create a new listener each time to avoid pointer reuse issues,
			// though for this benchmark, it's not strictly necessary.
			manager.Register(eventType, &mockListener{name: "l", priority: j})
		}
	}
}
