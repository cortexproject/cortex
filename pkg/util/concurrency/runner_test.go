package concurrency

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestForEach(t *testing.T) {
	var (
		ctx = context.Background()

		// Keep track of processed jobs.
		processedMx sync.Mutex
		processed   []string
	)

	jobs := []string{"a", "b", "c"}

	err := ForEach(ctx, CreateJobsFromStrings(jobs), 2, func(ctx context.Context, job interface{}) error {
		processedMx.Lock()
		defer processedMx.Unlock()
		processed = append(processed, job.(string))
		return nil
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, jobs, processed)
}

func TestForEach_ShouldBreakOnFirstError(t *testing.T) {
	var (
		ctx = context.Background()

		// Keep the processed jobs count.
		processed atomic.Int32
	)

	err := ForEach(ctx, []interface{}{"a", "b", "c"}, 2, func(ctx context.Context, job interface{}) error {
		if processed.CAS(0, 1) {
			return errors.New("the first request is failing")
		}

		// Wait 1s and increase the number of processed jobs, unless the context get canceled earlier.
		select {
		case <-time.After(time.Second):
			processed.Add(1)
		case <-ctx.Done():
			return ctx.Err()
		}

		return nil
	})

	require.EqualError(t, err, "the first request is failing")

	// Since we expect the first error interrupts the workers, we should only see
	// 1 job processed (the one which immediately returned error).
	assert.Equal(t, int32(1), processed.Load())
}
