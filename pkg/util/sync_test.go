package util

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaitGroupWithTimeout(t *testing.T) {
	wg := sync.WaitGroup{}

	success := WaitGroupWithTimeout(&wg, 100*time.Millisecond)
	assert.True(t, success)

	wg.Add(1)
	success = WaitGroupWithTimeout(&wg, 100*time.Millisecond)
	assert.False(t, success)

	wg.Done()
	success = WaitGroupWithTimeout(&wg, 100*time.Millisecond)
	assert.True(t, success)
}
