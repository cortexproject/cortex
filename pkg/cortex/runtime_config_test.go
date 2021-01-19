package cortex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util/validation"
)

// Given limits are usually loaded via a config file, and that
// a configmap is limited to 1MB, we need to minimise the limits file.
// One way to do it is via YAML anchors.
func TestLoadingAnchoredRuntimeYAML(t *testing.T) {
	yamlFile := strings.NewReader(`
overrides:
  '1234': &id001
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
    max_samples_per_query: 100000
    max_series_per_metric: 0
    max_series_per_query: 30000
    max_series_per_user: 0
    ruler_max_rule_groups_per_tenant: 20
    ruler_max_rules_per_rule_group: 20
  '1235': *id001
  '1236': *id001
`)
	runtimeCfg, err := loadRuntimeConfig(yamlFile)
	require.NoError(t, err)

	limits := validation.Limits{
		IngestionRate:               1500,
		IngestionBurstSize:          15000,
		MaxGlobalSeriesPerUser:      15000,
		MaxGlobalSeriesPerMetric:    7000,
		MaxSeriesPerQuery:           30000,
		MaxSamplesPerQuery:          100000,
		RulerMaxRulesPerRuleGroup:   20,
		RulerMaxRuleGroupsPerTenant: 20,
	}

	loadedLimits := runtimeCfg.(*runtimeConfigValues).TenantLimits
	require.Equal(t, 3, len(loadedLimits))
	require.Equal(t, limits, *loadedLimits["1234"])
	require.Equal(t, limits, *loadedLimits["1235"])
	require.Equal(t, limits, *loadedLimits["1236"])
}
