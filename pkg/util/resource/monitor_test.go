package resource

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func Test_Monitor(t *testing.T) {
	m, err := NewMonitor(map[Type]float64{}, time.Second, time.Minute, prometheus.DefaultRegisterer)

	m.scanners[CPU] = &noopScanner{}
	m.containerLimit[CPU] = 1
	m.utilization[CPU] = 0.5

	require.NoError(t, err)
}
