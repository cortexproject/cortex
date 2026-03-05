package backoff

import (
	"context"
	"testing"
	"time"

	utiltimer "github.com/cortexproject/cortex/pkg/util/timer"
)

func TestBackoff_NextDelay(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		minBackoff     time.Duration
		maxBackoff     time.Duration
		expectedRanges [][]time.Duration
	}{
		"exponential backoff with jitter honoring min and max": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 10 * time.Second,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{800 * time.Millisecond, 1600 * time.Millisecond},
				{1600 * time.Millisecond, 3200 * time.Millisecond},
				{3200 * time.Millisecond, 6400 * time.Millisecond},
				{6400 * time.Millisecond, 10000 * time.Millisecond},
				{6400 * time.Millisecond, 10000 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 800 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range + 1": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 801 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 800 * time.Millisecond},
				{800 * time.Millisecond, 801 * time.Millisecond},
				{800 * time.Millisecond, 801 * time.Millisecond},
			},
		},
		"exponential backoff with max equal to the end of a range - 1": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 799 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 400 * time.Millisecond},
				{400 * time.Millisecond, 799 * time.Millisecond},
				{400 * time.Millisecond, 799 * time.Millisecond},
			},
		},
		"min backoff is equal to max": {
			minBackoff: 100 * time.Millisecond,
			maxBackoff: 100 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{100 * time.Millisecond, 100 * time.Millisecond},
				{100 * time.Millisecond, 100 * time.Millisecond},
				{100 * time.Millisecond, 100 * time.Millisecond},
			},
		},
		"min backoff is greater then max": {
			minBackoff: 200 * time.Millisecond,
			maxBackoff: 100 * time.Millisecond,
			expectedRanges: [][]time.Duration{
				{200 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 200 * time.Millisecond},
				{200 * time.Millisecond, 200 * time.Millisecond},
			},
		},
	}

	for testName, testData := range tests {

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			b := New(context.Background(), Config{
				MinBackoff: testData.minBackoff,
				MaxBackoff: testData.maxBackoff,
				MaxRetries: len(testData.expectedRanges),
			})

			for _, expectedRange := range testData.expectedRanges {
				delay := b.NextDelay()

				if delay < expectedRange[0] || delay > expectedRange[1] {
					t.Errorf("%d expected to be within %d and %d", delay, expectedRange[0], expectedRange[1])
				}
			}
		})
	}
}

func TestBackoff_WaitReusesTimer(t *testing.T) {
	t.Parallel()

	b := New(context.Background(), Config{
		MinBackoff: time.Nanosecond,
		MaxBackoff: time.Nanosecond,
		MaxRetries: 0,
	})

	b.Wait()
	if b.waitTimer == nil {
		t.Fatal("expected wait timer to be initialized")
	}

	firstTimer := b.waitTimer

	b.Wait()
	if b.waitTimer != firstTimer {
		t.Fatal("expected wait timer to be reused")
	}
}

func TestBackoff_WaitReturnsWhenContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	b := New(ctx, Config{
		MinBackoff: time.Second,
		MaxBackoff: time.Second,
		MaxRetries: 0,
	})

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	startedAt := time.Now()
	b.Wait()

	if time.Since(startedAt) >= 900*time.Millisecond {
		t.Fatal("expected Wait to return quickly after context cancellation")
	}

	if b.waitTimer == nil {
		t.Fatal("expected wait timer to be initialized")
	}

	utiltimer.ResetTimer(b.waitTimer, time.Nanosecond)
	select {
	case <-b.waitTimer.C:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected wait timer to be reusable after cancellation")
	}
}
