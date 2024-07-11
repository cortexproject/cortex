package util

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type TokenBucket struct {
	remainingTokens int64
	maxCapacity     int64
	refillRate      int64
	lastRefill      time.Time
	mu              sync.Mutex

	remainingTokensMetric prometheus.Gauge
}

func NewTokenBucket(maxCapacity int64, remainingTokensMetric prometheus.Gauge) *TokenBucket {
	if remainingTokensMetric != nil {
		remainingTokensMetric.Set(float64(maxCapacity))
	}

	return &TokenBucket{
		remainingTokens:       maxCapacity,
		maxCapacity:           maxCapacity,
		refillRate:            maxCapacity,
		lastRefill:            time.Now(),
		remainingTokensMetric: remainingTokensMetric,
	}
}

// Retrieve always deducts token, even if there is not enough remaining tokens.
func (t *TokenBucket) Retrieve(amount int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.updateTokens()
	t.remainingTokens -= amount

	if t.remainingTokensMetric != nil {
		t.remainingTokensMetric.Set(float64(t.remainingTokens))
	}

	return t.remainingTokens
}

func (t *TokenBucket) MaxCapacity() int64 {
	return t.maxCapacity
}

func (t *TokenBucket) updateTokens() {
	now := time.Now()
	refilledTokens := int64(now.Sub(t.lastRefill).Seconds() * float64(t.refillRate))
	t.remainingTokens += refilledTokens
	t.lastRefill = now

	if t.remainingTokens > t.maxCapacity {
		t.remainingTokens = t.maxCapacity
	}
}
