package core

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	// This constant must match the one in pool.go for the test to be accurate.
	const testInitialPoolSize = 16384

	// Test Get and Put
	t.Run("Get and Put", func(t *testing.T) {
		pool := NewBufferPool(0) // Creates a pre-warmed pool
		require.Equal(t, testInitialPoolSize, len(pool.items), "Pool should be pre-warmed with the correct number of items")

		// Get a buffer from the pool
		buf := pool.Get()
		require.NotNil(t, buf, "Get() should not return a nil buffer")
		require.Equal(t, testInitialPoolSize-1, len(pool.items), "Pool size should decrease by 1 after Get")

		// Write some data to it
		testString := "hello world"
		buf.WriteString(testString)
		assert.Equal(t, testString, buf.String(), "Buffer content should match what was written")

		// Put the buffer back
		pool.Put(buf)
		require.Equal(t, testInitialPoolSize, len(pool.items), "Pool size should return to initial size after Put")

		// Get another buffer, it should be the same one but reset
		buf2 := pool.Get()
		assert.Equal(t, 0, buf2.Len(), "Reused buffer should be reset (length 0)")
	})

	t.Run("Get more than pool size", func(t *testing.T) {
		pool := NewBufferPool(0)
		// Get all pre-warmed buffers
		for i := 0; i < testInitialPoolSize; i++ {
			pool.Get()
		}
		require.Equal(t, 0, len(pool.items), "Pool should be empty after getting all pre-warmed buffers")

		// Get one more, which should trigger a new allocation via newFunc
		newBuf := pool.Get()
		require.NotNil(t, newBuf)
		require.Equal(t, 0, len(pool.items), "Pool should still be empty after creating a new buffer on-demand")

		// Put the newly created buffer back into the pool
		pool.Put(newBuf)
		require.Equal(t, 1, len(pool.items), "Pool should now contain the newly created buffer, increasing its size")
	})

	// Test with initial capacity
	t.Run("With Initial Capacity", func(t *testing.T) {
		initialCap := 128
		pool := NewBufferPool(initialCap)
		buf := pool.Get()
		require.NotNil(t, buf)

		// Check that the new buffer has the specified initial capacity and is empty.
		assert.Equal(t, 0, buf.Len(), "Expected new buffer to have length 0")
		assert.GreaterOrEqual(t, buf.Cap(), initialCap, "Expected new buffer to have at least the specified capacity")
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		pool := NewBufferPool(128)
		var wg sync.WaitGroup
		numGoroutines := 200
		numOpsPerGoroutine := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numOpsPerGoroutine; j++ {
					buf := pool.Get()
					buf.WriteString("test")
					pool.Put(buf)
				}
			}()
		}
		wg.Wait()
		// The final size of the pool can be > initialPoolSize if contention was high,
		// but it should not be excessively large. This just tests for race conditions.
		assert.GreaterOrEqual(t, len(pool.items), testInitialPoolSize, "Pool size should be at least the initial size after concurrent use")
	})
}

// Benchmark for getting and putting a buffer from the pool
func BenchmarkBufferPool_GetPut(b *testing.B) {
	pool := NewBufferPool(0) // Uses default capacity
	data := []byte("some data to write")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf.Write(data)
			pool.Put(buf)
		}
	})
}

// Renamed benchmark to be more specific
func BenchmarkMutexBufferPool_GetPut_WithCapacity(b *testing.B) {
	pool := NewBufferPool(128)
	data := []byte("some data to write")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Get()
			buf.Write(data)
			pool.Put(buf)
		}
	})
}
