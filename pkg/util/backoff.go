package util

import (
	"math/rand"
	"time"
)

type BackoffConfig struct {
	MinBackoff time.Duration // start backoff at this level
	MaxBackoff time.Duration // increase exponentially to this level
	MaxRetries int           // give up after this many; zero means infinite retries
}

type Backoff struct {
	cfg        BackoffConfig
	done       <-chan struct{}
	numRetries int
	cancelled  bool
	duration   time.Duration
}

func NewBackoff(cfg BackoffConfig, done <-chan struct{}) *Backoff {
	return &Backoff{
		cfg:      cfg,
		done:     done,
		duration: cfg.MinBackoff,
	}
}

func (b *Backoff) Reset() {
	b.numRetries = 0
	b.cancelled = false
	b.duration = b.cfg.MinBackoff
}

func (b *Backoff) Ongoing() bool {
	return !b.cancelled && (b.cfg.MaxRetries == 0 || b.numRetries < b.cfg.MaxRetries)
}

func (b *Backoff) NumRetries() int {
	return b.numRetries
}

func (b *Backoff) Wait() {
	b.numRetries++
	b.WaitWithoutCounting()
}

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
