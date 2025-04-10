package limiter

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/cortexproject/cortex/pkg/util/resource"
)

const ErrResourceLimitReachedStr = "resource limit reached"

type ResourceLimitReachedError struct{}

func (e *ResourceLimitReachedError) Error() string {
	return ErrResourceLimitReachedStr
}

type ResourceBasedLimiter struct {
	resourceMonitor resource.IMonitor
	limits          map[resource.Type]float64
}

func NewResourceBasedLimiter(resourceMonitor resource.IMonitor, limits map[resource.Type]float64, registerer prometheus.Registerer) (*ResourceBasedLimiter, error) {
	for resType, limit := range limits {
		switch resType {
		case resource.CPU, resource.Heap:
			promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
				Name:        "cortex_resource_based_limiter_limit",
				ConstLabels: map[string]string{"resource": string(resType)},
			}).Set(limit)
		default:
			return nil, fmt.Errorf("unsupported resource type: [%s]", resType)
		}
	}

	return &ResourceBasedLimiter{
		resourceMonitor: resourceMonitor,
		limits:          limits,
	}, nil
}

func (l *ResourceBasedLimiter) AcceptNewRequest() error {
	for resType, limit := range l.limits {
		var utilization float64

		switch resType {
		case resource.CPU:
			utilization = l.resourceMonitor.GetCPUUtilization()
		case resource.Heap:
			utilization = l.resourceMonitor.GetHeapUtilization()
		}

		if utilization >= limit {
			return fmt.Errorf("%s utilization limit reached (limit: %.3f, utilization: %.3f)", resType, limit, utilization)
		}
	}

	return nil
}
