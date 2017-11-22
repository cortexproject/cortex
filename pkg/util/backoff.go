package util

import (
	"math/rand"
	"time"
)

const (
	// Backoff for dynamoDB requests, to match AWS lib - see:
	// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/customizations.go
	minBackoff = 50 * time.Millisecond
	maxBackoff = 50 * time.Second
	maxRetries = 20
)

type Backoff struct {
	done       <-chan struct{}
	numRetries int
	cancelled  bool
	duration   time.Duration
}

func NewBackoff(done <-chan struct{}) *Backoff {
	return &Backoff{
		done:     done,
		duration: minBackoff,
	}
}

func (b *Backoff) Reset() {
	b.numRetries = 0
	b.cancelled = false
	b.duration = minBackoff
}

func (b *Backoff) Finished() bool {
	return b.cancelled || b.numRetries >= maxRetries
}

func (b *Backoff) NumRetries() int {
	return b.numRetries
}

func (b *Backoff) Wait() {
	b.numRetries++
	b.WaitWithoutCounting()
}

func (b *Backoff) WaitWithoutCounting() {
	if !b.Finished() {
		select {
		case <-b.done:
			b.cancelled = true
		case <-time.After(b.duration):
		}
	}
	// Based on the "Decorrelated Jitter" approach from https://www.awsarchitectureblog.com/2015/03/backoff.html
	// sleep = min(cap, random_between(base, sleep * 3))
	b.duration = minBackoff + time.Duration(rand.Int63n(int64((b.duration*3)-minBackoff)))
	if b.duration > maxBackoff {
		b.duration = maxBackoff
	}
}
