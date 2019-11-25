package util

import (
	"sync"
	"time"
)

// WaitGroupWithTimeout calls Wait() on a sync.WaitGroup and return once the
// Wait() completed or the timeout expires, whatever occurs first. Returns true
// if the Wait() completed before the timeout expired, false otherwise.
func WaitGroupWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}
