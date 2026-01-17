package pool

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestScratchBuilderOverwriteCorruption demonstrates how using ScratchBuilder.Overwrite
// without copying can cause label corruption. This is the root cause of the issue:
// Overwrite reuses an internal buffer, so Labels created via Overwrite share memory.
func TestScratchBuilderOverwriteCorruption(t *testing.T) {
	builder := labels.NewScratchBuilder(16)

	// Create first labels using Overwrite
	var labels1 labels.Labels
	builder.Reset()
	builder.Add("__name__", "metric_one")
	builder.Add("job", "test1")
	builder.Add("instance", "localhost:9090")
	builder.Overwrite(&labels1)

	// Verify first labels are correct
	require.Equal(t, "metric_one", labels1.Get("__name__"))
	require.Equal(t, "test1", labels1.Get("job"))

	// Store a reference to labels1 (simulating storing in a pool buffer)
	storedLabels := labels1

	// Create second labels using the SAME builder with Overwrite
	var labels2 labels.Labels
	builder.Reset()
	builder.Add("__name__", "metric_two")
	builder.Add("job", "test2")
	builder.Add("instance", "localhost:9091")
	builder.Overwrite(&labels2)

	// Verify second labels are correct
	require.Equal(t, "metric_two", labels2.Get("__name__"))
	require.Equal(t, "test2", labels2.Get("job"))

	// BUG DEMONSTRATION: storedLabels now contains corrupted data!
	// Because Overwrite reuses the internal buffer, labels1's data was overwritten.
	// This is the exact pattern that can cause WAL corruption.
	assert.NotEqual(t, "metric_one", storedLabels.Get("__name__"),
		"BUG DEMO: Without copying, storedLabels got corrupted when builder was reused")

	// The stored labels now incorrectly show the second metric's data
	assert.Equal(t, "metric_two", storedLabels.Get("__name__"),
		"BUG DEMO: storedLabels now shows metric_two instead of metric_one")
}

// TestScratchBuilderOverwriteWithCopyIsSafe demonstrates that copying labels
// after Overwrite prevents the corruption.
func TestScratchBuilderOverwriteWithCopyIsSafe(t *testing.T) {
	builder := labels.NewScratchBuilder(16)

	// Create first labels using Overwrite
	var labels1 labels.Labels
	builder.Reset()
	builder.Add("__name__", "metric_one")
	builder.Add("job", "test1")
	builder.Overwrite(&labels1)

	// Copy the labels before storing (THE FIX)
	storedLabels := labels1.Copy()

	// Create second labels using the SAME builder
	var labels2 labels.Labels
	builder.Reset()
	builder.Add("__name__", "metric_two")
	builder.Add("job", "test2")
	builder.Overwrite(&labels2)

	// FIX VERIFIED: storedLabels retains original data because we copied
	assert.Equal(t, "metric_one", storedLabels.Get("__name__"),
		"FIX VERIFIED: Copied labels retain original value")
	assert.Equal(t, "test1", storedLabels.Get("job"),
		"FIX VERIFIED: Copied labels retain original value")
}

// TestRefSeriesPoolStaleDataWithoutZeroing demonstrates how not zeroing labels
// when returning buffers to the pool can lead to stale data being accessible.
func TestRefSeriesPoolStaleDataWithoutZeroing(t *testing.T) {
	pool := sync.Pool{
		New: func() any {
			return make([]record.RefSeries, 0, 512)
		},
	}

	// Simulate the BAD (old) implementation without zeroing
	putWithoutZeroing := func(b []record.RefSeries) {
		pool.Put(b[:0]) // Only resets length, capacity slots retain data
	}

	// Simulate the GOOD (fixed) implementation with zeroing
	putWithZeroing := func(b []record.RefSeries) {
		for i := range b {
			b[i].Labels = labels.EmptyLabels()
		}
		pool.Put(b[:0])
	}

	t.Run("without zeroing capacity slots retain stale labels", func(t *testing.T) {
		// Reset pool
		pool = sync.Pool{
			New: func() any {
				return make([]record.RefSeries, 0, 512)
			},
		}

		// Get buffer and populate with series
		buf := pool.Get().([]record.RefSeries)
		series1Labels := labels.FromStrings("__name__", "stale_metric", "job", "old_job")
		buf = append(buf, record.RefSeries{Ref: 1, Labels: series1Labels})
		buf = append(buf, record.RefSeries{Ref: 2, Labels: series1Labels})

		originalLen := len(buf)

		// Return without zeroing
		putWithoutZeroing(buf)

		// Get buffer again
		buf2 := pool.Get().([]record.RefSeries)
		require.Equal(t, 0, len(buf2), "length should be 0")

		// Access capacity slots - they still have old data
		if cap(buf2) >= originalLen {
			buf2 = buf2[:originalLen]
			for i := 0; i < originalLen; i++ {
				assert.False(t, buf2[i].Labels.IsEmpty(),
					"BUG: capacity slot %d still has stale labels: %v", i, buf2[i].Labels)
			}
		}
	})

	t.Run("with zeroing capacity slots are cleared", func(t *testing.T) {
		// Reset pool
		pool = sync.Pool{
			New: func() any {
				return make([]record.RefSeries, 0, 512)
			},
		}

		// Get buffer and populate with series
		buf := pool.Get().([]record.RefSeries)
		series1Labels := labels.FromStrings("__name__", "test_metric", "job", "test_job")
		buf = append(buf, record.RefSeries{Ref: 1, Labels: series1Labels})
		buf = append(buf, record.RefSeries{Ref: 2, Labels: series1Labels})

		originalLen := len(buf)

		// Return WITH zeroing (the fix)
		putWithZeroing(buf)

		// Get buffer again
		buf2 := pool.Get().([]record.RefSeries)
		require.Equal(t, 0, len(buf2))

		// Access capacity slots - they should be cleared
		if cap(buf2) >= originalLen {
			buf2 = buf2[:originalLen]
			for i := 0; i < originalLen; i++ {
				assert.True(t, buf2[i].Labels.IsEmpty(),
					"FIX VERIFIED: capacity slot %d is cleared", i)
			}
		}
	})
}

// TestConcurrentAppendWithSharedBuilder demonstrates a potential race condition
// when a ScratchBuilder is shared across concurrent operations without proper copying.
func TestConcurrentAppendWithSharedBuilder(t *testing.T) {
	const (
		numGoroutines = 10
		numSeries     = 100
	)

	// This test demonstrates the UNSAFE pattern (sharing a builder without copying)
	t.Run("unsafe shared builder causes corruption", func(t *testing.T) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		builder := labels.NewScratchBuilder(16)
		allLabels := make([]labels.Labels, 0, numGoroutines*numSeries)
		corrupted := 0
		panicCount := 0

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numSeries; i++ {
					metricName := fmt.Sprintf("metric_%d_%d", goroutineID, i)

					mu.Lock()
					var lbls labels.Labels
					builder.Reset()
					builder.Add("__name__", metricName)
					builder.Add("goroutine", fmt.Sprintf("%d", goroutineID))
					builder.Add("series", fmt.Sprintf("%d", i))
					builder.Overwrite(&lbls)

					// Store WITHOUT copying (the bug)
					allLabels = append(allLabels, lbls)
					mu.Unlock()

					// Small yield to increase chance of interleaving
					time.Sleep(time.Microsecond)
				}
			}(g)
		}

		wg.Wait()

		// Check for corruption - use recover to catch panics from corrupted label data
		for idx, lbls := range allLabels {
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount++
						if panicCount <= 3 {
							t.Logf("BUG DEMO: Panic at index %d due to corrupted label data: %v", idx, r)
						}
					}
				}()

				name := lbls.Get("__name__")
				goroutineStr := lbls.Get("goroutine")
				seriesStr := lbls.Get("series")

				expected := fmt.Sprintf("metric_%s_%s", goroutineStr, seriesStr)
				if name != expected {
					corrupted++
					if corrupted <= 5 { // Only log first few
						t.Logf("Corruption at index %d: expected %s, got %s", idx, expected, name)
					}
				}
			}()
		}

		// We expect corruption or panics because of the shared builder
		totalIssues := corrupted + panicCount
		assert.Greater(t, totalIssues, 0,
			"Expected corruption with shared builder pattern (found %d corrupted, %d panics out of %d)",
			corrupted, panicCount, len(allLabels))
	})

	// This test demonstrates the SAFE pattern (copying after Overwrite)
	t.Run("copying after overwrite prevents corruption", func(t *testing.T) {
		var wg sync.WaitGroup
		var mu sync.Mutex
		builder := labels.NewScratchBuilder(16)
		allLabels := make([]labels.Labels, 0, numGoroutines*numSeries)

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < numSeries; i++ {
					metricName := fmt.Sprintf("metric_%d_%d", goroutineID, i)

					mu.Lock()
					var lbls labels.Labels
					builder.Reset()
					builder.Add("__name__", metricName)
					builder.Add("goroutine", fmt.Sprintf("%d", goroutineID))
					builder.Add("series", fmt.Sprintf("%d", i))
					builder.Overwrite(&lbls)

					// Store WITH copying (the fix)
					allLabels = append(allLabels, lbls.Copy())
					mu.Unlock()

					time.Sleep(time.Microsecond)
				}
			}(g)
		}

		wg.Wait()

		// Check for corruption
		corrupted := 0
		for idx, lbls := range allLabels {
			name := lbls.Get("__name__")
			goroutineStr := lbls.Get("goroutine")
			seriesStr := lbls.Get("series")

			expected := fmt.Sprintf("metric_%s_%s", goroutineStr, seriesStr)
			if name != expected {
				corrupted++
				t.Errorf("Unexpected corruption at index %d: expected %s, got %s", idx, expected, name)
			}
		}

		assert.Equal(t, 0, corrupted,
			"FIX VERIFIED: No corruption with copying (found %d/%d corrupted)",
			corrupted, len(allLabels))
	})
}

// TestLabelFusionCorruptionPattern attempts to reproduce the specific "label fusion"
// corruption pattern observed in production WAL files, where labels from multiple
// series appear concatenated in a single Labels.data string.
func TestLabelFusionCorruptionPattern(t *testing.T) {
	// This test simulates the scenario where:
	// 1. Multiple series are being created rapidly
	// 2. A shared buffer/builder is reused
	// 3. Labels are stored without proper copying
	// 4. The result is labels that contain data from multiple series

	const numIterations = 1000

	t.Run("simulate label fusion through buffer reuse", func(t *testing.T) {
		builder := labels.NewScratchBuilder(64)
		refSeriesBuffer := make([]record.RefSeries, 0, 512)

		// Metrics that will be "fused" together (similar to production observation)
		metrics := []struct {
			name string
			job  string
		}{
			{"kafka_writer_write_time_seconds_bucket", "kafka"},
			{"cortex_distributor_sample_delay_seconds_count", "distributor"},
			{"prometheus_http_requests_total", "prometheus"},
			{"go_goroutines", "runtime"},
		}

		fusionDetected := 0
		panicOccurred := false

		for iter := 0; iter < numIterations && !panicOccurred; iter++ {
			// Clear buffer (simulating pool get with b[:0])
			refSeriesBuffer = refSeriesBuffer[:0]

			// Append multiple series using shared builder (simulating rapid ingestion)
			for i, m := range metrics {
				var lbls labels.Labels
				builder.Reset()
				builder.Add("__name__", m.name)
				builder.Add("job", m.job)
				builder.Add("instance", fmt.Sprintf("host%d:9090", i))
				builder.Overwrite(&lbls)

				// Store WITHOUT copying (the bug pattern)
				refSeriesBuffer = append(refSeriesBuffer, record.RefSeries{
					Ref:    chunks.HeadSeriesRef(iter*len(metrics) + i),
					Labels: lbls, // Not copied!
				})
			}

			// Check if any series has labels from another series
			// Use recover to catch panics from corrupted label data
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Logf("BUG DEMO: Panic occurred due to corrupted label data at iter %d: %v", iter, r)
						panicOccurred = true
						fusionDetected++
					}
				}()

				for i, rs := range refSeriesBuffer {
					name := rs.Labels.Get("__name__")
					job := rs.Labels.Get("job")

					// Check for label fusion (labels from wrong metric)
					expectedName := metrics[i].name
					expectedJob := metrics[i].job

					if name != expectedName || job != expectedJob {
						fusionDetected++
						if fusionDetected <= 3 {
							t.Logf("Label fusion detected at iter %d, series %d: got __name__=%s job=%s, expected __name__=%s job=%s",
								iter, i, name, job, expectedName, expectedJob)
						}
					}

					// Check for duplicate label names (the corruption we observed)
					if dupName, hasDup := rs.Labels.HasAnyDuplicateLabelNames(); hasDup {
						t.Logf("Duplicate label name detected: %s in labels: %s", dupName, rs.Labels.String())
						fusionDetected++
					}
				}
			}()
		}

		// With the buggy pattern, we expect label fusion or even panics due to corruption
		assert.Greater(t, fusionDetected, 0,
			"Expected label fusion or corruption with shared builder pattern")
	})

	t.Run("copying prevents label fusion", func(t *testing.T) {
		builder := labels.NewScratchBuilder(64)
		refSeriesBuffer := make([]record.RefSeries, 0, 512)

		metrics := []struct {
			name string
			job  string
		}{
			{"kafka_writer_write_time_seconds_bucket", "kafka"},
			{"cortex_distributor_sample_delay_seconds_count", "distributor"},
			{"prometheus_http_requests_total", "prometheus"},
			{"go_goroutines", "runtime"},
		}

		fusionDetected := 0

		for iter := 0; iter < numIterations; iter++ {
			refSeriesBuffer = refSeriesBuffer[:0]

			for i, m := range metrics {
				var lbls labels.Labels
				builder.Reset()
				builder.Add("__name__", m.name)
				builder.Add("job", m.job)
				builder.Add("instance", fmt.Sprintf("host%d:9090", i))
				builder.Overwrite(&lbls)

				// Store WITH copying (the fix)
				refSeriesBuffer = append(refSeriesBuffer, record.RefSeries{
					Ref:    chunks.HeadSeriesRef(iter*len(metrics) + i),
					Labels: lbls.Copy(),
				})
			}

			for i, rs := range refSeriesBuffer {
				name := rs.Labels.Get("__name__")
				job := rs.Labels.Get("job")

				expectedName := metrics[i].name
				expectedJob := metrics[i].job

				if name != expectedName || job != expectedJob {
					fusionDetected++
					t.Errorf("Unexpected fusion at iter %d, series %d: got __name__=%s, expected %s",
						iter, i, name, expectedName)
				}

				if dupName, hasDup := rs.Labels.HasAnyDuplicateLabelNames(); hasDup {
					t.Errorf("Unexpected duplicate label: %s", dupName)
					fusionDetected++
				}
			}
		}

		assert.Equal(t, 0, fusionDetected,
			"FIX VERIFIED: No label fusion when copying")
	})
}

// TestHighVolumePoolChurn stress tests the pool with high volume to try to
// trigger any edge cases in buffer reuse.
func TestHighVolumePoolChurn(t *testing.T) {
	const (
		numGoroutines  = 20
		numOperations  = 10000
		bufferCapacity = 64
	)

	pool := sync.Pool{
		New: func() any {
			return make([]record.RefSeries, 0, bufferCapacity)
		},
	}

	// Function to put buffer back without zeroing (the bug)
	putWithoutZeroing := func(b []record.RefSeries) {
		pool.Put(b[:0])
	}

	// Function to put buffer back with zeroing (the fix)
	putWithZeroing := func(b []record.RefSeries) {
		for i := range b {
			b[i].Labels = labels.EmptyLabels()
		}
		pool.Put(b[:0])
	}

	runTest := func(t *testing.T, putFunc func([]record.RefSeries), expectCorruption bool) {
		var wg sync.WaitGroup
		corruptions := make(chan string, 1000)
		done := make(chan struct{})

		// Collector goroutine
		var corruptionCount int
		var corruptionSamples []string
		go func() {
			for c := range corruptions {
				corruptionCount++
				if len(corruptionSamples) < 5 {
					corruptionSamples = append(corruptionSamples, c)
				}
			}
			close(done)
		}()

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(goroutineID)))
				builder := labels.NewScratchBuilder(16)

				for op := 0; op < numOperations; op++ {
					buf := pool.Get().([]record.RefSeries)

					// Generate random number of series
					numSeries := rng.Intn(10) + 1
					expectedLabels := make([]string, numSeries)

					for i := 0; i < numSeries; i++ {
						metricName := fmt.Sprintf("metric_g%d_op%d_s%d", goroutineID, op, i)
						expectedLabels[i] = metricName

						var lbls labels.Labels
						builder.Reset()
						builder.Add("__name__", metricName)
						builder.Add("goroutine", fmt.Sprintf("%d", goroutineID))
						builder.Overwrite(&lbls)

						buf = append(buf, record.RefSeries{
							Ref:    chunks.HeadSeriesRef(op*100 + i),
							Labels: lbls.Copy(), // We copy here to isolate the pool issue
						})
					}

					// Verify all labels are correct before returning to pool
					for i, rs := range buf {
						actualName := rs.Labels.Get("__name__")
						if actualName != expectedLabels[i] {
							corruptions <- fmt.Sprintf("g%d op%d s%d: expected %s, got %s",
								goroutineID, op, i, expectedLabels[i], actualName)
						}
					}

					putFunc(buf)
				}
			}(g)
		}

		wg.Wait()
		close(corruptions)
		<-done

		if expectCorruption {
			// For the pool test with proper copying, we shouldn't see corruption
			// The corruption from not zeroing manifests when accessing capacity slots
			t.Logf("Corruption count: %d", corruptionCount)
		} else {
			assert.Equal(t, 0, corruptionCount,
				"Expected no corruption, but found %d cases", corruptionCount)
		}
	}

	t.Run("with zeroing - no corruption expected", func(t *testing.T) {
		runTest(t, putWithZeroing, false)
	})

	t.Run("without zeroing - checking for issues", func(t *testing.T) {
		// Note: Direct corruption might not manifest with proper copying
		// The issue is more about memory retention and potential GC issues
		runTest(t, putWithoutZeroing, false)
	})
}

// TestMemoryRetentionWithoutZeroing demonstrates that without zeroing,
// the Labels in capacity slots retain references to label string data,
// potentially preventing garbage collection of that data.
func TestMemoryRetentionWithoutZeroing(t *testing.T) {
	pool := sync.Pool{
		New: func() any {
			return make([]record.RefSeries, 0, 100)
		},
	}

	// Create a large label set
	largeLabelValue := strings.Repeat("x", 10000) // 10KB string

	// Get buffer and add series with large labels
	buf := pool.Get().([]record.RefSeries)
	largeLabels := labels.FromStrings(
		"__name__", "large_metric",
		"large_value", largeLabelValue,
	)
	buf = append(buf, record.RefSeries{Ref: 1, Labels: largeLabels})

	// Without zeroing, return to pool
	pool.Put(buf[:0])

	// Get buffer again
	buf2 := pool.Get().([]record.RefSeries)

	// The capacity slot still holds the large labels reference
	if cap(buf2) >= 1 {
		buf2 = buf2[:1]
		// This demonstrates that the large string is still referenced
		val := buf2[0].Labels.Get("large_value")
		assert.Equal(t, largeLabelValue, val,
			"Without zeroing, large label data is still accessible in capacity slots")

		// In production, this means:
		// 1. Memory is retained longer than necessary
		// 2. If the original label data came from a reusable buffer (yoloString),
		//    accessing this stale data could return corrupted values
	}
}
