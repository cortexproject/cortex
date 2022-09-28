package limiter

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	firstRequest  = "firstRequest"
	secondRequest = "secondRequest"
)

func TestHeapMemLimiter_LoadBytes_ShouldReturnNoErrorWhenHeapAllocIsNotExceedingLimit(t *testing.T) {
	fakeMemStats := &runtime.MemStats{
		HeapAlloc: 0,
	}

	l := &HeapMemLimiter{
		heapLimitInBytes: 100,
		memStats:         fakeMemStats,
	}
	l.AddRequest(firstRequest)
	l.AddRequest(secondRequest)
	assert.NoError(t, l.LoadBytes(100, firstRequest))
	assert.NoError(t, l.LoadBytes(100, secondRequest))
}

func TestHeapMemLimiter_LoadBytes_ShouldNeverReturnErrorForFirstRequest(t *testing.T) {
	fakeMemStats := &runtime.MemStats{
		HeapAlloc: 1000000,
	}

	l := &HeapMemLimiter{
		heapLimitInBytes: 100,
		memStats:         fakeMemStats,
	}
	l.AddRequest(firstRequest)
	l.AddRequest(secondRequest)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			assert.NoError(t, l.LoadBytes(100, firstRequest))
			wg.Done()
		}()
		go func() {
			assert.Error(t, fmt.Errorf(ErrMaxMemoryLimitHit), l.LoadBytes(100, secondRequest))
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestHeapMemLimiter_AddorRemoveRequests_ShouldWorkCorrectlyWhenDoneConcurrently(t *testing.T) {
	l := &HeapMemLimiter{
		heapLimitInBytes: 0,
	}
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			l.AddRequest(fmt.Sprintf("request-%d", i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		r := fmt.Sprintf("request-%d", i)
		assert.Truef(t, contains(l.queue, r), "Queue doesn't contain all requests. queue: %v, request: %s", l.queue, r)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			l.RemoveRequest(fmt.Sprintf("request-%d", i))
			wg.Done()
		}(i)
	}
	wg.Wait()

	assert.Zerof(t, len(l.queue), "Expected queue to be empty: %v", l.queue)
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
