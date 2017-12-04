package util

import (
	"math/rand"
	"time"
)

// BackoffConfig configures a Backoff
type BackoffConfig struct {
	MinBackoff time.Duration // start backoff at this level
	MaxBackoff time.Duration // increase exponentially to this level
	MaxRetries int           // give up after this many; zero means infinite retries
}

// Backoff implements exponential backoff with randomized wait times
type Backoff struct {
	cfg        BackoffConfig
	done       <-chan struct{}
	numRetries int
	cancelled  bool
	duration   time.Duration
}

// NewBackoff creates a Backoff object. Pass a 'done' channel that can be closed to terminate the operation.
func NewBackoff(cfg BackoffConfig, done <-chan struct{}) *Backoff {
	return &Backoff{
		cfg:      cfg,
		done:     done,
		duration: cfg.MinBackoff,
	}
}

// Reset the Backoff back to its initial condition
func (b *Backoff) Reset() {
	b.numRetries = 0
	b.cancelled = false
	b.duration = b.cfg.MinBackoff
}

// Ongoing returns true if caller should keep going
func (b *Backoff) Ongoing() bool {
	return !b.cancelled && (b.cfg.MaxRetries == 0 || b.numRetries < b.cfg.MaxRetries)
}

// NumRetries returns the number of retries so far
func (b *Backoff) NumRetries() int {
	return b.numRetries
}

// Wait sleeps for the backoff time then increases the retry count and backoff time
// Returns immediately if done channel is closed
func (b *Backoff) Wait() {
	b.numRetries++
	b.WaitWithoutCounting()
}

// WaitWithoutCounting sleeps for the backoff time then increases backoff time
// Returns immediately if done channel is closed
func (b *Backoff) WaitWithoutCounting() {
	if b.Ongoing() {
		select {
		case <-b.done:
			b.cancelled = true
		case <-time.After(b.duration):
		}
	}
	// Based on the "Decorrelated Jitter" approach from https://www.awsarchitectureblog.com/2015/03/backoff.html
	// sleep = min(cap, random_between(base, sleep * 3))
	b.duration = b.cfg.MinBackoff + time.Duration(rand.Int63n(int64((b.duration*3)-b.cfg.MinBackoff)))
	if b.duration > b.cfg.MaxBackoff {
		b.duration = b.cfg.MaxBackoff
	}
}
