package util

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroup(t *testing.T) {
	// WaitGroup is done
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	wg := sync.WaitGroup{}
	success := WaitGroup(ctx, &wg)
	assert.True(t, success)

	// WaitGroup is not done and timeout has expired
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	wg = sync.WaitGroup{}
	wg.Add(1)
	success = WaitGroup(ctx, &wg)
	assert.False(t, success)

	// WaitGroup is not done and context is cancelled before timeout expires
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute)

	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	wg = sync.WaitGroup{}
	wg.Add(1)
	success = WaitGroup(ctx, &wg)
	assert.False(t, success)
}
