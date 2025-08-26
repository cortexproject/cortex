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

	limiter, err := NewResourceBasedLimiter(&MockMonitor{
		CpuUtilization:  0.2,
		HeapUtilization: 0.2,
	}, limits, prometheus.DefaultRegisterer, "ingester")
	require.NoError(t, err)

	err = limiter.AcceptNewRequest()
	require.NoError(t, err)
}
