package transport

import (
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type localStrategy struct {
	limits *validation.Overrides
}

func newLocalQueryDataBytesRateStrategy(limits *validation.Overrides) limiter.RateLimiterStrategy {
	return &localStrategy{
		limits: limits,
	}
}

func (s *localStrategy) Limit(tenantID string) float64 {
	return s.limits.QueryDataBytesRatePerUser(tenantID)
}

func (s *localStrategy) Burst(tenantID string) int {
	return s.limits.QueryDataBytesBurstSizePerUser(tenantID)
}
