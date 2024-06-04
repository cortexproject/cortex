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

func NewTokenBucket(maxCapacity, refillRate int64, remainingTokensMetric prometheus.Gauge) *TokenBucket {
	if remainingTokensMetric != nil {
		remainingTokensMetric.Set(float64(maxCapacity))
	}

	return &TokenBucket{
		remainingTokens:       maxCapacity,
		maxCapacity:           maxCapacity,
		refillRate:            refillRate,
		lastRefill:            time.Now(),
		remainingTokensMetric: remainingTokensMetric,
	}
}

func (t *TokenBucket) Retrieve(amount int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.updateTokens()

	if t.remainingTokens < amount {
		return false
	}

	t.remainingTokens -= amount
	if t.remainingTokensMetric != nil {
		t.remainingTokensMetric.Set(float64(t.remainingTokens))
	}

	return true
}

func (t *TokenBucket) ForceRetrieve(amount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.updateTokens()

	t.remainingTokens -= amount
	if t.remainingTokensMetric != nil {
		t.remainingTokensMetric.Set(float64(t.remainingTokens))
	}
}

func (t *TokenBucket) Refund(amount int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.updateTokens()

	t.remainingTokens += amount

	if t.remainingTokens > t.maxCapacity {
		t.remainingTokens = t.maxCapacity
	}
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
