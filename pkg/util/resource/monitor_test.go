package resource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/configs"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestMonitor(t *testing.T) {
	type test struct {
		limit       float64
		val         uint64
		threshold   float64
		utilization float64
		throwErr    bool
	}

	tests := map[string]test{
		"should not throw error if below threshold": {
			limit:       10,
			val:         1,
			utilization: 0.1,
			threshold:   0.2,
			throwErr:    false,
		},
		"should throw error if above threshold": {
			limit:       10,
			val:         5,
			utilization: 0.5,
			threshold:   0.2,
			throwErr:    true,
		},
		"should not throw error if limit is 0": {
			limit:       0,
			val:         5,
			utilization: 0,
			threshold:   0.2,
			throwErr:    false,
		},
	}

	for _, tc := range tests {
		limits := configs.Resources{Heap: tc.limit}
		thresholds := configs.Resources{Heap: tc.threshold}
		scanner := mockScanner{Heap: tc.val}

		monitor, err := NewMonitor(thresholds, limits, &scanner, nil)
		require.NoError(t, err)
		require.NoError(t, monitor.StartAsync(context.Background()))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), monitor))
		})

		time.Sleep(2 * time.Second) // let scanner store values

		require.Equal(t, tc.utilization, monitor.GetHeapUtilization())
		_, _, _, err = monitor.CheckResourceUtilization()

		if tc.throwErr {
			exhaustedErr := &ExhaustedError{}
			require.ErrorContains(t, err, exhaustedErr.Error())
		} else {
			require.NoError(t, err)
		}
	}
}

type mockScanner struct {
	CPU  float64
	Heap uint64
}

func (m *mockScanner) Scan() (Stats, error) {
	return Stats{
		cpu:  m.CPU,
		heap: m.Heap,
	}, nil
}
