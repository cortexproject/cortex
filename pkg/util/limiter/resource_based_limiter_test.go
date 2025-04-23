package limiter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/resource"
)

func Test_ResourceBasedLimiter(t *testing.T) {
	limits := map[resource.Type]float64{
		resource.CPU:  0.5,
		resource.Heap: 0.5,
	}

	_, err := NewResourceBasedLimiter(&mockMonitor{}, limits, prometheus.DefaultRegisterer, "ingester")
	require.NoError(t, err)
}

type mockMonitor struct{}

func (m *mockMonitor) GetCPUUtilization() float64 {
	return 0
}

func (m *mockMonitor) GetHeapUtilization() float64 {
	return 0
}
