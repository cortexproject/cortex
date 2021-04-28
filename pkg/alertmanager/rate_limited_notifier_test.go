package alertmanager

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestRateLimitedNotifier(t *testing.T) {
	mock := &mockNotifier{}
	counter := prometheus.NewCounter(prometheus.CounterOpts{})

	// Initial limits.
	limiter := &limiter{limit: 5, burst: 5}
	rateLimitedNotifier := newRateLimitedNotifier(mock, limiter, 10*time.Second, counter)

	runNotifications(t, rateLimitedNotifier, counter, 10, 5, 5, 5)

	// Disable limits.
	limiter.limit = rate.Inf
	limiter.burst = 0

	runNotifications(t, rateLimitedNotifier, counter, 10, 10, 0, 5)

	limiter.limit = 5
	limiter.burst = 5

	// We don't get any successful notifications until some time passes.
	runNotifications(t, rateLimitedNotifier, counter, 10, 0, 10, 15)

	time.Sleep(1 * time.Second) // Wait to refill rate-limiter's "bucket".
	runNotifications(t, rateLimitedNotifier, counter, 10, 5, 5, 20)
}

func runNotifications(t *testing.T, rateLimitedNotifier *rateLimitedNotifier, counter prometheus.Counter, count, expectedSuccess, expectedRateLimited, expectedCounter int) {
	rateLimitedNotifier.recheckAt.Store(0) // Force recheck of limits.

	success := 0
	rateLimited := 0

	for i := 0; i < count; i++ {
		retry, err := rateLimitedNotifier.Notify(context.Background(), &types.Alert{})

		if err == nil {
			success++
		} else if err == errRateLimited {
			rateLimited++
			assert.False(t, retry)
		} else {
			assert.NotNil(t, err)
		}
	}

	assert.Equal(t, expectedSuccess, success)
	assert.Equal(t, expectedRateLimited, rateLimited)
	assert.Equal(t, expectedCounter, int(testutil.ToFloat64(counter)))
}

type mockNotifier struct{}

func (m *mockNotifier) Notify(ctx context.Context, alert ...*types.Alert) (bool, error) {
	return false, nil
}

type limiter struct {
	limit rate.Limit
	burst int
}

func (l *limiter) RateLimit() rate.Limit {
	return l.limit
}

func (l *limiter) Burst() int {
	return l.burst
}
