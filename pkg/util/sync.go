package util

import (
	"context"
	"sync"
)

// WaitGroup calls Wait() on a sync.WaitGroup and return once the Wait() completed
// or the context is cancelled or times out, whatever occurs first. Returns true if
// the Wait() completed before the timeout expired, false otherwise.
func WaitGroup(ctx context.Context, wg *sync.WaitGroup) bool {
	c := make(chan struct{})

	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return true
	case <-ctx.Done():
		return false
	}
}
