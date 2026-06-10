package configs

import (
	"errors"
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/flagext"
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
					Threshold: Threshold{
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
					Threshold: Threshold{
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
					Threshold: Threshold{
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
					Threshold: Threshold{
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
					Threshold: Threshold{
						HeapUtilization: 0.5,
					},
				},
			},
			monitoredResources: []string{"cpu"},
			err:                errors.New("monitored_resources config must include \"heap\" as well"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.queryProtection.Validate(flagext.StringSliceCSV(tc.monitoredResources))
			if tc.err != nil {
				require.Errorf(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_EvictionConfig_Enabled(t *testing.T) {
	assert.False(t, EvictionConfig{}.Enabled())
	assert.True(t, EvictionConfig{Threshold: Threshold{CPUUtilization: 0.8}}.Enabled())
	assert.True(t, EvictionConfig{Threshold: Threshold{HeapUtilization: 0.85}}.Enabled())
	assert.True(t, EvictionConfig{Threshold: Threshold{CPUUtilization: 0.8, HeapUtilization: 0.85}}.Enabled())
}

func Test_EvictionConfig_Validation(t *testing.T) {
	validBase := func() QueryProtection {
		return QueryProtection{
			Eviction: EvictionConfig{
				Threshold:            Threshold{CPUUtilization: 0.8, HeapUtilization: 0.85},
				CheckInterval:        1 * time.Second,
				CooldownPeriod:       3,
				EvictionMetric:       "fetched_samples",
				MaxEvictionsPerCycle: 1,
			},
		}
	}

	tests := map[string]struct {
		modify             func(*QueryProtection)
		monitoredResources []string
		err                string
	}{
		"valid config passes":               {func(qp *QueryProtection) {}, []string{"cpu", "heap"}, ""},
		"cpu > 1 fails":                     {func(qp *QueryProtection) { qp.Eviction.Threshold.CPUUtilization = 1.5 }, []string{"cpu", "heap"}, "eviction cpu_utilization must be between 0 and 1"},
		"cpu < 0 fails":                     {func(qp *QueryProtection) { qp.Eviction.Threshold.CPUUtilization = -0.1 }, []string{"cpu", "heap"}, "eviction cpu_utilization must be between 0 and 1"},
		"heap > 1 fails":                    {func(qp *QueryProtection) { qp.Eviction.Threshold.HeapUtilization = 2.0 }, []string{"cpu", "heap"}, "eviction heap_utilization must be between 0 and 1"},
		"heap < 0 fails":                    {func(qp *QueryProtection) { qp.Eviction.Threshold.HeapUtilization = -0.5 }, []string{"cpu", "heap"}, "eviction heap_utilization must be between 0 and 1"},
		"check_interval 0 fails":            {func(qp *QueryProtection) { qp.Eviction.CheckInterval = 0 }, []string{"cpu", "heap"}, "eviction check_interval must be greater than 0 when eviction is enabled"},
		"cooldown < 0 fails":                {func(qp *QueryProtection) { qp.Eviction.CooldownPeriod = -1 }, []string{"cpu", "heap"}, "eviction cooldown_period must be >= 0"},
		"unknown metric fails":              {func(qp *QueryProtection) { qp.Eviction.EvictionMetric = "unknown" }, []string{"cpu", "heap"}, `unrecognized eviction_metric "unknown"; supported values: fetched_samples, fetched_series, fetched_chunks, fetched_chunk_bytes`},
		"cpu without monitored fails":       {func(qp *QueryProtection) {}, []string{"heap"}, `monitored_resources config must include "cpu" when eviction cpu threshold is set`},
		"heap without monitored fails":      {func(qp *QueryProtection) {}, []string{"cpu"}, `monitored_resources config must include "heap" when eviction heap threshold is set`},
		"cooldown 0 is valid":               {func(qp *QueryProtection) { qp.Eviction.CooldownPeriod = 0 }, []string{"cpu", "heap"}, ""},
		"max_evictions_per_cycle < 1 fails": {func(qp *QueryProtection) { qp.Eviction.MaxEvictionsPerCycle = 0 }, []string{"cpu", "heap"}, "eviction max_evictions_per_cycle must be >= 1"},
		"disabled skips interval check": {func(qp *QueryProtection) {
			qp.Eviction.Threshold = Threshold{}
			qp.Eviction.CheckInterval = 0
		}, []string{}, ""},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			qp := validBase()
			tc.modify(&qp)
			err := qp.Validate(flagext.StringSliceCSV(tc.monitoredResources))
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_RegisterFlagsWithPrefix_EvictionDefaults(t *testing.T) {
	var cfg QueryProtection
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix(fs, "querier.")
	require.NoError(t, fs.Parse([]string{}))

	assert.Equal(t, float64(0), cfg.Eviction.Threshold.CPUUtilization)
	assert.Equal(t, float64(0), cfg.Eviction.Threshold.HeapUtilization)
	assert.Equal(t, 1*time.Second, cfg.Eviction.CheckInterval)
	assert.Equal(t, 3, cfg.Eviction.CooldownPeriod)
	assert.Equal(t, "fetched_samples", cfg.Eviction.EvictionMetric)
	assert.Equal(t, 1, cfg.Eviction.MaxEvictionsPerCycle)
	assert.False(t, cfg.Eviction.Enabled())
}
