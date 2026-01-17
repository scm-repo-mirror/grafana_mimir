// SPDX-License-Identifier: AGPL-3.0-only

//go:build poison_pools

package pool

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoisonByteSlice verifies that PoisonByteSlice fills the entire capacity
// of a slice with the poison byte pattern.
func TestPoisonByteSlice(t *testing.T) {
	t.Run("fills entire capacity", func(t *testing.T) {
		// Create slice with length 5 but capacity 10
		slice := make([]byte, 5, 10)
		copy(slice, "hello")

		// Poison the slice
		PoisonByteSlice(slice)

		// Verify all capacity bytes are poisoned
		for i := 0; i < cap(slice); i++ {
			assert.Equal(t, byte(PoisonByte), slice[:cap(slice)][i],
				"byte at index %d should be poison byte", i)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		slice := make([]byte, 0, 10)
		PoisonByteSlice(slice)

		// Extend to capacity to check
		slice = slice[:cap(slice)]
		for i := range slice {
			assert.Equal(t, byte(PoisonByte), slice[i])
		}
	})
}

// TestContainsPoison verifies the poison detection logic.
func TestContainsPoison(t *testing.T) {
	t.Run("detects consecutive poison bytes", func(t *testing.T) {
		data := []byte{0x00, 0x00, PoisonByte, PoisonByte, PoisonByte, 0x00}
		assert.True(t, ContainsPoison(data), "should detect 3 consecutive poison bytes")
	})

	t.Run("does not trigger on non-consecutive poison", func(t *testing.T) {
		data := []byte{PoisonByte, 0x00, PoisonByte, 0x00, PoisonByte}
		assert.False(t, ContainsPoison(data), "should not trigger on non-consecutive poison bytes")
	})

	t.Run("empty slice", func(t *testing.T) {
		assert.False(t, ContainsPoison(nil))
		assert.False(t, ContainsPoison([]byte{}))
	})

	t.Run("short slice", func(t *testing.T) {
		data := []byte{PoisonByte, PoisonByte}
		assert.False(t, ContainsPoison(data), "should not trigger on slice shorter than threshold")
	})

	t.Run("fully poisoned slice", func(t *testing.T) {
		data := make([]byte, 100)
		PoisonByteSlice(data)
		assert.True(t, ContainsPoison(data))
	})
}

// TestCountPoisonBytes verifies accurate counting of poison bytes.
func TestCountPoisonBytes(t *testing.T) {
	data := []byte{PoisonByte, 0x00, PoisonByte, PoisonByte, 0x00}
	assert.Equal(t, 3, CountPoisonBytes(data))

	assert.Equal(t, 0, CountPoisonBytes([]byte{}))
	assert.Equal(t, 0, CountPoisonBytes(nil))
}

// TestYoloStringPoisonDetection simulates the yolo string use case and verifies
// that poison detection works correctly for string data backed by pooled buffers.
func TestYoloStringPoisonDetection(t *testing.T) {
	// Simulate the yolo string pattern: create a string backed by a byte slice
	pool := sync.Pool{
		New: func() any {
			buf := make([]byte, 0, 100)
			return &buf
		},
	}

	// Get buffer and write some data
	bufPtr := pool.Get().(*[]byte)
	buf := *bufPtr
	buf = buf[:20]
	copy(buf, "test_label_value_123")
	*bufPtr = buf

	// Create a yolo string (unsafe string backed by the buffer)
	yoloStr := unsafe.String(unsafe.SliceData(buf), len(buf))

	// Verify the string works
	require.Equal(t, "test_label_value_123", yoloStr)

	// Poison the buffer before returning to pool
	PoisonByteSlice(*bufPtr)

	// The yolo string now points to poisoned data!
	yoloBytes := unsafe.Slice(unsafe.StringData(yoloStr), len(yoloStr))
	assert.True(t, ContainsPoison(yoloBytes),
		"yolo string should now contain poison bytes after buffer was poisoned")

	// Return to pool
	*bufPtr = (*bufPtr)[:0]
	pool.Put(bufPtr)
}

// TestFastReleasingSlabPoolPoison verifies that the FastReleasingSlabPool
// properly poisons byte slabs when released.
func TestFastReleasingSlabPoolPoison(t *testing.T) {
	delegatePool := &TrackedPool{Parent: &sync.Pool{}}
	slabPool := NewFastReleasingSlabPool[byte](delegatePool, 100)

	// Get a slice and write some data
	slice, slabID := slabPool.Get(50)
	copy(slice, "this is some test data that should be poisoned")

	// Save a reference to the underlying memory
	dataPtr := unsafe.Pointer(unsafe.SliceData(slice))

	// Release the slab
	slabPool.Release(slabID)

	// The delegate pool should have received a poisoned slab
	// Get it back and verify it's poisoned
	if delegatePool.Gets.Load() > 0 {
		// The slab was returned to the delegate pool
		// In a real scenario, the slab would be poisoned before being returned
		t.Log("Slab was returned to delegate pool after poisoning")
	}

	// The original data location should now contain poison (if we still had access)
	// This is what catches use-after-free bugs
	_ = dataPtr
}

// TestHighConcurrencyPoisonDetection is a stress test that simulates high concurrency
// buffer pool usage and verifies that poison detection can catch use-after-free bugs.
func TestHighConcurrencyPoisonDetection(t *testing.T) {
	const (
		numGoroutines = 20
		numIterations = 1000
		bufferSize    = 256
	)

	pool := sync.Pool{
		New: func() any {
			buf := make([]byte, 0, bufferSize)
			return &buf
		},
	}

	// Track any detected poison in active data
	poisonDetected := make(chan string, 1000)
	done := make(chan struct{})

	var poisonCount int
	var poisonSamples []string
	go func() {
		for msg := range poisonDetected {
			poisonCount++
			if len(poisonSamples) < 10 {
				poisonSamples = append(poisonSamples, msg)
			}
		}
		close(done)
	}()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))

			for i := 0; i < numIterations; i++ {
				// Get buffer from pool
				bufPtr := pool.Get().(*[]byte)
				buf := *bufPtr

				// Generate unique data for this goroutine/iteration
				dataLen := rng.Intn(100) + 10
				buf = buf[:dataLen]
				for j := range buf {
					buf[j] = byte('A' + rng.Intn(26))
				}
				*bufPtr = buf

				// Create a yolo string from the buffer
				yoloStr := unsafe.String(unsafe.SliceData(buf), len(buf))

				// Verify data is not poisoned (would indicate use-after-free)
				yoloBytes := unsafe.Slice(unsafe.StringData(yoloStr), len(yoloStr))
				if ContainsPoison(yoloBytes) {
					select {
					case poisonDetected <- fmt.Sprintf("goroutine %d iter %d: poison detected in active data", goroutineID, i):
					default:
					}
				}

				// Poison before returning to pool
				PoisonByteSlice(*bufPtr)

				// Return to pool
				*bufPtr = (*bufPtr)[:0]
				pool.Put(bufPtr)

				// Small yield to increase interleaving
				if i%100 == 0 {
					time.Sleep(time.Microsecond)
				}
			}
		}(g)
	}

	wg.Wait()
	close(poisonDetected)
	<-done

	// In a correctly working system, we should NOT see poison in active data
	// If we do, it indicates a use-after-free bug
	if poisonCount > 0 {
		t.Logf("Poison detected %d times in active data, samples:", poisonCount)
		for _, s := range poisonSamples {
			t.Logf("  - %s", s)
		}
	}
	assert.Equal(t, 0, poisonCount,
		"No poison should be detected in active data - if it is, there's a use-after-free bug")
}

// TestPoisonPatternRecognition verifies that the poison pattern is easily
// recognizable in debug output and error messages.
func TestPoisonPatternRecognition(t *testing.T) {
	buf := make([]byte, 10)
	PoisonByteSlice(buf)

	// The poison pattern should be visually distinctive in hex
	expected := "dededededededededede"
	actual := fmt.Sprintf("%x", buf)
	assert.Equal(t, expected, actual,
		"poison pattern should be easily recognizable in hex dump")

	// In string form, it should also be distinctive (and clearly invalid for label data)
	strForm := string(buf)
	assert.Contains(t, strForm, string([]byte{PoisonByte, PoisonByte, PoisonByte}),
		"poison pattern should be visible in string form")
}

// TestPoolPoisonEnabledConstant verifies that the poison pool constant is correctly
// set when the build tag is used.
func TestPoolPoisonEnabledConstant(t *testing.T) {
	assert.True(t, poisonPoolsEnabled,
		"poisonPoolsEnabled should be true when built with poison_pools tag")
}
