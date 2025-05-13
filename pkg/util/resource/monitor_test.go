package resource

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func Test_Monitor(t *testing.T) {
	m, err := NewMonitor(map[Type]float64{}, prometheus.DefaultRegisterer)

	m.scanners[CPU] = &noopScanner{}
	m.containerLimit[CPU] = 1
	m.utilization[CPU] = 0.5

	require.NoError(t, err)
}
