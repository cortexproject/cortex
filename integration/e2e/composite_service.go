package e2e

import (
	"context"
	"fmt"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
)

// CompositeHTTPService abstract an higher-level service composed, under the hood,
// by 2+ HTTPService.
type CompositeHTTPService struct {
	services []*HTTPService

	// Generic retry backoff.
	retryBackoff *util.Backoff
}

func NewCompositeHTTPService(services ...*HTTPService) *CompositeHTTPService {
	return &CompositeHTTPService{
		services: services,
		retryBackoff: util.NewBackoff(context.Background(), util.BackoffConfig{
			MinBackoff: 300 * time.Millisecond,
			MaxBackoff: 600 * time.Millisecond,
			MaxRetries: 50, // Sometimes the CI is slow ¯\_(ツ)_/¯
		}),
	}
}

func (s *CompositeHTTPService) NumInstances() int {
	return len(s.services)
}

func (s *CompositeHTTPService) Instances() []*HTTPService {
	return s.services
}

// WaitSumMetrics waits for at least one instance of each given metric names to be present and their sums, returning true
// when passed to given isExpected(...).
func (s *CompositeHTTPService) WaitSumMetrics(isExpected func(sums ...float64) bool, metricNames ...string) error {
	var (
		sums []float64
		err  error
	)

	for s.retryBackoff.Reset(); s.retryBackoff.Ongoing(); {
		sums, err = s.SumMetrics(metricNames...)
		if err != nil {
			return err
		}

		if isExpected(sums...) {
			return nil
		}

		s.retryBackoff.Wait()
	}

	return fmt.Errorf("unable to find metrics %s with expected values. Last values: %v", metricNames, sums)
}

// SumMetrics returns the sum of the values of each given metric names.
func (s *CompositeHTTPService) SumMetrics(metricNames ...string) ([]float64, error) {
	sums := make([]float64, len(metricNames))

	for _, service := range s.services {
		partials, err := service.SumMetrics(metricNames...)
		if err != nil {
			return nil, err
		}

		if len(partials) != len(sums) {
			return nil, fmt.Errorf("unexpected mismatching sum metrics results (got %d, expected %d)", len(partials), len(sums))
		}

		for i := 0; i < len(sums); i++ {
			sums[i] += partials[i]
		}
	}

	return sums, nil
}
