package distributor

import (
	"math"

	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type localRequestStrategy struct {
	baseRequestRate limiter.RateLimiterStrategy
}

func newLocalRequestRateStrategy(limits *validation.Overrides) limiter.RateLimiterStrategy {
	return &localRequestStrategy{
		baseRequestRate: newBaseRequestRate(limits),
	}
}

func (s *localRequestStrategy) Limit(tenantID string) float64 {
	return s.baseRequestRate.Limit(tenantID)
}

func (s *localRequestStrategy) Burst(tenantID string) int {
	return s.baseRequestRate.Burst(tenantID)
}

type globalRequestStrategy struct {
	baseRequestRate limiter.RateLimiterStrategy
	ring            ReadLifecycler
}

func newGlobalRequestRateStrategy(limits *validation.Overrides, ring ReadLifecycler) limiter.RateLimiterStrategy {
	return &globalRequestStrategy{
		baseRequestRate: newBaseRequestRate(limits),
		ring:            ring,
	}
}

func (s *globalRequestStrategy) Limit(tenantID string) float64 {
	numDistributors := s.ring.HealthyInstancesCount()

	limit := s.baseRequestRate.Limit(tenantID)

	if numDistributors == 0 || limit == float64(rate.Inf) {
		return limit
	}
	return limit / float64(numDistributors)
}

func (s *globalRequestStrategy) Burst(tenantID string) int {
	return s.baseRequestRate.Burst(tenantID)
}

type baseRequestRate struct {
	limits *validation.Overrides
}

func newBaseRequestRate(limits *validation.Overrides) limiter.RateLimiterStrategy {
	return &baseRequestRate{
		limits: limits,
	}
}

func (s *baseRequestRate) Limit(tenantID string) float64 {
	if lm := s.limits.RequestRate(tenantID); lm > 0 {
		return lm
	}
	return float64(rate.Inf)
}

func (s *baseRequestRate) Burst(tenantID string) int {
	if s.limits.RequestRate(tenantID) <= 0 {
		// Burst is ignored when limit = rate.Inf
		return 0
	}
	if lm := s.limits.RequestBurstSize(tenantID); lm > 0 {
		return lm
	}
	return math.MaxInt
}

func newInfiniteRequestRateStrategy() limiter.RateLimiterStrategy {
	return &infiniteStrategy{}
}
