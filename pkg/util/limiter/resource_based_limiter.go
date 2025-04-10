package limiter

import (
	"fmt"

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

func NewResourceBasedLimiter(resourceMonitor resource.IMonitor, limits map[resource.Type]float64) *ResourceBasedLimiter {
	return &ResourceBasedLimiter{
		resourceMonitor: resourceMonitor,
		limits:          limits,
	}
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
