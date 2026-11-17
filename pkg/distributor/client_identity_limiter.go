package distributor

import (
	"github.com/cortexproject/cortex/pkg/util/clientlimiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

// clientIdentityIngestionStrategy adapts validation.Overrides' client-identity
// ingestion accessors to clientlimiter.Strategy. See
// docs/proposals/client-identity-ingestion-rate-limiting.md.
type clientIdentityIngestionStrategy struct {
	limits *validation.Overrides
}

func newClientIdentityIngestionStrategy(limits *validation.Overrides) clientlimiter.Strategy {
	return &clientIdentityIngestionStrategy{limits: limits}
}

func (s *clientIdentityIngestionStrategy) Limit(tenantID string) float64 {
	return s.limits.ClientIdentityIngestionRateLimit(tenantID)
}

func (s *clientIdentityIngestionStrategy) Burst(tenantID string) int {
	return s.limits.ClientIdentityIngestionBurstSize(tenantID)
}

func (s *clientIdentityIngestionStrategy) TrackedClientsLimit(tenantID string) int {
	return s.limits.ClientIdentityIngestionTrackedClientsLimit(tenantID)
}
