package util

import (
	"bytes"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestNewWorkerPool_CreateMultiplesPoolsWithSameRegistry(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	wp1 := NewWorkerPool("test1", 100, reg)
	defer wp1.Stop()
	wp2 := NewWorkerPool("test2", 100, reg)
	defer wp2.Stop()
}

func TestWorkerPool_TestMetric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	workerPool := NewWorkerPool("test1", 1, reg)
	defer workerPool.Stop()

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
				# HELP cortex_worker_pool_fallback_total The total number additional go routines that needed to be created to run jobs.
				# TYPE cortex_worker_pool_fallback_total counter
				cortex_worker_pool_fallback_total{name="test1"} 0
`), "cortex_worker_pool_fallback_total"))

	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Block the first job
	workerPool.Submit(func() {
		wg.Wait()
	})

	// create an extra job to increment the metric
	workerPool.Submit(func() {})
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
				# HELP cortex_worker_pool_fallback_total The total number additional go routines that needed to be created to run jobs.
				# TYPE cortex_worker_pool_fallback_total counter
				cortex_worker_pool_fallback_total{name="test1"} 1
`), "cortex_worker_pool_fallback_total"))

	wg.Done()
}

func TestWorkerPool_ShouldFallbackWhenAllWorkersAreBusy(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	numberOfWorkers := 10
	workerPool := NewWorkerPool("test1", numberOfWorkers, reg)
	defer workerPool.Stop()

	m := sync.Mutex{}
	blockerWg := sync.WaitGroup{}
	blockerWg.Add(numberOfWorkers)

	// Lets lock all submited jobs
	m.Lock()

	for i := 0; i < numberOfWorkers; i++ {
		workerPool.Submit(func() {
			defer blockerWg.Done()
			m.Lock()
			m.Unlock() //nolint:staticcheck
		})
	}

	// At this point all workers should be busy. lets try to create a new job
	wg := sync.WaitGroup{}
	wg.Add(1)
	workerPool.Submit(func() {
		defer wg.Done()
	})

	// Make sure the last job ran to the end
	wg.Wait()

	// Lets release the jobs
	m.Unlock()

	blockerWg.Wait()

}
