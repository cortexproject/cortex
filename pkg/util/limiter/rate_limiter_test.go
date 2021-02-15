package limiter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestRateLimiter_RecheckPeriod(t *testing.T) {
	strategy := &increasingLimitStrategy{}
	limiter := NewRateLimiter(strategy, 10*time.Second)
	now := time.Now()

	// Since the strategy increases the limit and burst value each time
	// the strategy functions are called, we do assert if the recheck
	// period is honored increasing the input time
	assert.Equal(t, float64(1), limiter.Limit(now, "test"))
	assert.Equal(t, 1, limiter.Burst(now, "test"))

	assert.Equal(t, float64(1), limiter.Limit(now.Add(9*time.Second), "test"))
	assert.Equal(t, 1, limiter.Burst(now.Add(9*time.Second), "test"))

	assert.Equal(t, float64(2), limiter.Limit(now.Add(10*time.Second), "test"))
	assert.Equal(t, 2, limiter.Burst(now.Add(10*time.Second), "test"))

	assert.Equal(t, float64(2), limiter.Limit(now.Add(19*time.Second), "test"))
	assert.Equal(t, 2, limiter.Burst(now.Add(19*time.Second), "test"))

	assert.Equal(t, float64(3), limiter.Limit(now.Add(20*time.Second), "test"))
	assert.Equal(t, 3, limiter.Burst(now.Add(20*time.Second), "test"))
}

func TestRateLimiter_AllowN(t *testing.T) {
	strategy := &staticLimitStrategy{tenants: map[string]struct {
		limit float64
		burst int
	}{
		"tenant-1": {limit: 10, burst: 20},
		"tenant-2": {limit: 20, burst: 40},
	}}

	limiter := NewRateLimiter(strategy, 10*time.Second)
	now := time.Now()

	// Tenant #1
	assert.Equal(t, true, limiter.AllowN(now, "tenant-1", 8).OK())
	assert.Equal(t, true, limiter.AllowN(now, "tenant-1", 10).OK())
	assert.Equal(t, false, limiter.AllowN(now, "tenant-1", 3).OK())
	assert.Equal(t, true, limiter.AllowN(now, "tenant-1", 2).OK())

	assert.Equal(t, true, limiter.AllowN(now.Add(time.Second), "tenant-1", 8).OK())
	assert.Equal(t, false, limiter.AllowN(now.Add(time.Second), "tenant-1", 3).OK())
	assert.Equal(t, true, limiter.AllowN(now.Add(time.Second), "tenant-1", 2).OK())

	// Tenant #2
	assert.Equal(t, true, limiter.AllowN(now, "tenant-2", 18).OK())
	assert.Equal(t, true, limiter.AllowN(now, "tenant-2", 20).OK())
	assert.Equal(t, false, limiter.AllowN(now, "tenant-2", 3).OK())
	assert.Equal(t, true, limiter.AllowN(now, "tenant-2", 2).OK())

	assert.Equal(t, true, limiter.AllowN(now.Add(time.Second), "tenant-2", 18).OK())
	assert.Equal(t, false, limiter.AllowN(now.Add(time.Second), "tenant-2", 3).OK())
	assert.Equal(t, true, limiter.AllowN(now.Add(time.Second), "tenant-2", 2).OK())
}

func TestRateLimiter_AllowNCancelation(t *testing.T) {
	strategy := &staticLimitStrategy{tenants: map[string]struct {
		limit float64
		burst int
	}{
		"tenant-1": {limit: 10, burst: 20},
	}}

	limiter := NewRateLimiter(strategy, 10*time.Second)
	now := time.Now()

	assert.Equal(t, true, limiter.AllowN(now, "tenant-1", 12).OK())
	assert.Equal(t, false, limiter.AllowN(now, "tenant-1", 9).OK())

	r1 := limiter.AllowN(now, "tenant-1", 8)
	assert.Equal(t, true, r1.OK())
	r1.CancelAt(now)

	assert.Equal(t, true, limiter.AllowN(now, "tenant-1", 8).OK())

	// +10 tokens (1s)
	nowPlus := now.Add(time.Second)

	assert.Equal(t, true, limiter.AllowN(nowPlus, "tenant-1", 6).OK())
	assert.Equal(t, false, limiter.AllowN(nowPlus, "tenant-1", 5).OK())

	r2 := limiter.AllowN(nowPlus, "tenant-1", 4)
	assert.Equal(t, true, r2.OK())
	r2.CancelAt(nowPlus)

	assert.Equal(t, true, limiter.AllowN(nowPlus, "tenant-1", 2).OK())
	assert.Equal(t, false, limiter.AllowN(nowPlus, "tenant-1", 3).OK())
	assert.Equal(t, true, limiter.AllowN(nowPlus, "tenant-1", 2).OK())
	assert.Equal(t, false, limiter.AllowN(nowPlus, "tenant-1", 1).OK())
}

func BenchmarkRateLimiter_CustomMultiTenant(b *testing.B) {
	strategy := &increasingLimitStrategy{}
	limiter := NewRateLimiter(strategy, 10*time.Second)
	now := time.Now()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		limiter.AllowN(now, "test", 1)
	}
}

func BenchmarkRateLimiter_OriginalSingleTenant(b *testing.B) {
	limiter := rate.NewLimiter(rate.Limit(1), 1)
	now := time.Now()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		limiter.AllowN(now, 1)
	}
}

type increasingLimitStrategy struct {
	limit float64
	burst int
}

func (s *increasingLimitStrategy) Limit(tenantID string) float64 {
	s.limit++
	return s.limit
}

func (s *increasingLimitStrategy) Burst(tenantID string) int {
	s.burst++
	return s.burst
}

type staticLimitStrategy struct {
	tenants map[string]struct {
		limit float64
		burst int
	}
}

func (s *staticLimitStrategy) Limit(tenantID string) float64 {
	tenant, ok := s.tenants[tenantID]
	if !ok {
		return 0
	}

	return tenant.limit
}

func (s *staticLimitStrategy) Burst(tenantID string) int {
	tenant, ok := s.tenants[tenantID]
	if !ok {
		return 0
	}

	return tenant.burst
}
