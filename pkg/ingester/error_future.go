package ingester

import (
	"context"
	"sync"
)

// NewErrorFuture creates unfinished future. Must be finished by calling Finish method eventually.
func NewErrorFuture() *ErrorFuture {
	return &ErrorFuture{
		ch: make(chan struct{}),
	}
}

// NewFinishedErrorFuture creates finished future with given (possibly nil) error.
func NewFinishedErrorFuture(err error) *ErrorFuture {
	ef := NewErrorFuture()
	ef.Finish(err)
	return ef
}

// This is a Future object, for holding error.
type ErrorFuture struct {
	mu   sync.Mutex
	done bool
	err  error
	ch   chan struct{} // used by waiters
}

// Returns true if this future is done, and associated error. If future is not done yet, returns false.
func (f *ErrorFuture) Get() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done, f.err
}

// Waits for future to finish, and returns true and associated error set via Finish method.
// If context is finished first, returns false and error from context instead.
func (f *ErrorFuture) WaitAndGet(ctx context.Context) (bool, error) {
	d, err := f.Get()
	if d {
		return true, err
	}

	select {
	case <-f.ch:
		return f.Get() // must return true now
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// Mark this future as finished. Can only be called once.
func (f *ErrorFuture) Finish(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.done = true
	f.err = err
	close(f.ch) // will panic, if called twice, which is fine.
}
