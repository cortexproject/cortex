package configs

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Validate(t *testing.T) {
	for name, tc := range map[string]struct {
		queryProtection    QueryProtection
		monitoredResources []string
		err                error
	}{
		"correct config should pass validation": {
			queryProtection: QueryProtection{
				Rejection: rejection{
					Threshold: threshold{
						CPUUtilization:  0.5,
						HeapUtilization: 0.5,
					},
				},
			},
			monitoredResources: []string{"cpu", "heap"},
			err:                nil,
		},
		"utilization config less than 0 should fail validation": {
			queryProtection: QueryProtection{
				Rejection: rejection{
					Threshold: threshold{
						CPUUtilization:  -0.5,
						HeapUtilization: 0.5,
					},
				},
			},
			monitoredResources: []string{"cpu", "heap"},
			err:                errors.New("cpu_utilization must be between 0 and 1"),
		},
		"utilization config greater than 1 should fail validation": {
			queryProtection: QueryProtection{
				Rejection: rejection{
					Threshold: threshold{
						CPUUtilization:  0.5,
						HeapUtilization: 1.5,
					},
				},
			},
			monitoredResources: []string{"cpu", "heap"},
			err:                errors.New("heap_utilization must be between 0 and 1"),
		},
		"missing cpu in monitored_resources config should fail validation": {
			queryProtection: QueryProtection{
				Rejection: rejection{
					Threshold: threshold{
						CPUUtilization: 0.5,
					},
				},
			},
			monitoredResources: []string{"heap"},
			err:                errors.New("monitored_resources config must include \"cpu\" as well"),
		},
		"missing heap in monitored_resources config should fail validation": {
			queryProtection: QueryProtection{
				Rejection: rejection{
					Threshold: threshold{
						HeapUtilization: 0.5,
					},
				},
			},
			monitoredResources: []string{"cpu"},
			err:                errors.New("monitored_resources config must include \"heap\" as well"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.queryProtection.Validate(tc.monitoredResources)
			if tc.err != nil {
				require.Errorf(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
