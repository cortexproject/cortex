package validation

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

// mockTenantLimits exposes per-tenant limits based on a provided map
type mockTenantLimits struct {
	limits map[string]*Limits
}

// newMockTenantLimits creates a new mockTenantLimits that returns per-tenant limits based on
// the given map
func newMockTenantLimits(limits map[string]*Limits) *mockTenantLimits {
	return &mockTenantLimits{
		limits: limits,
	}
}

func (l *mockTenantLimits) ByUserID(userID string) *Limits {
	return l.limits[userID]
}

func (l *mockTenantLimits) AllByUserID() map[string]*Limits {
	return l.limits
}

func TestLimits_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		limits           Limits
		shardByAllLabels bool
		expected         error
	}{
		"max-global-series-per-user disabled and shard-by-all-labels=false": {
			limits:           Limits{MaxGlobalSeriesPerUser: 0},
			shardByAllLabels: false,
			expected:         nil,
		},
		"max-global-series-per-user enabled and shard-by-all-labels=false": {
			limits:           Limits{MaxGlobalSeriesPerUser: 1000},
			shardByAllLabels: false,
			expected:         errMaxGlobalSeriesPerUserValidation,
		},
		"max-global-series-per-user disabled and shard-by-all-labels=true": {
			limits:           Limits{MaxGlobalSeriesPerUser: 1000},
			shardByAllLabels: true,
			expected:         nil,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.limits.Validate(testData.shardByAllLabels))
		})
	}
}

func TestOverridesManager_GetOverrides(t *testing.T) {
	tenantLimits := map[string]*Limits{}

	defaults := Limits{
		MaxLabelNamesPerSeries: 100,
	}
	ov, err := NewOverrides(defaults, newMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 0, ov.MaxLabelValueLength("user1"))

	// Update limits for tenant user1. We only update single field, the rest is copied from defaults.
	// (That is how limits work when loaded from YAML)
	l := Limits{}
	l = defaults
	l.MaxLabelValueLength = 150

	tenantLimits["user1"] = &l

	// Checking whether overrides were enforced
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 150, ov.MaxLabelValueLength("user1"))

	// Verifying user2 limits are not impacted by overrides
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user2"))
	require.Equal(t, 0, ov.MaxLabelValueLength("user2"))
}

func TestLimitsLoadingFromYaml(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	inp := `ingestion_rate: 0.5`

	l := Limits{}
	err := yaml.UnmarshalStrict([]byte(inp), &l)
	require.NoError(t, err)

	assert.Equal(t, 0.5, l.IngestionRate, "from yaml")
	assert.Equal(t, 100, l.MaxLabelNameLength, "from defaults")
}

func TestMetricRelabelConfigLimitsLoadingFromYaml(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{})

	inp := `
metric_relabel_configs:
- action: drop
  source_labels: [le]
  regex: .+
`
	exp := relabel.DefaultRelabelConfig
	exp.Action = relabel.Drop
	regex, err := relabel.NewRegexp(".+")
	require.NoError(t, err)
	exp.Regex = regex
	exp.SourceLabels = model.LabelNames([]model.LabelName{"le"})

	l := Limits{}
	err = yaml.UnmarshalStrict([]byte(inp), &l)
	require.NoError(t, err)

	assert.Equal(t, []*relabel.Config{&exp}, l.MetricRelabelConfigs)
}

func TestSmallestPositiveIntPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxQueryParallelism: 5,
		},
		"tenant-b": {
			MaxQueryParallelism: 10,
		},
	}

	defaults := Limits{
		MaxQueryParallelism: 0,
	}
	ov, err := NewOverrides(defaults, newMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  int
	}{
		{tenantIDs: []string{}, expLimit: 0},
		{tenantIDs: []string{"tenant-a"}, expLimit: 5},
		{tenantIDs: []string{"tenant-b"}, expLimit: 10},
		{tenantIDs: []string{"tenant-c"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 5},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 0},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveIntPerTenant(tc.tenantIDs, ov.MaxQueryParallelism))
	}
}

func TestSmallestPositiveNonZeroIntPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxQueriersPerTenant: 5,
		},
		"tenant-b": {
			MaxQueriersPerTenant: 10,
		},
	}

	defaults := Limits{
		MaxQueriersPerTenant: 0,
	}
	ov, err := NewOverrides(defaults, newMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  int
	}{
		{tenantIDs: []string{}, expLimit: 0},
		{tenantIDs: []string{"tenant-a"}, expLimit: 5},
		{tenantIDs: []string{"tenant-b"}, expLimit: 10},
		{tenantIDs: []string{"tenant-c"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 5},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 5},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroIntPerTenant(tc.tenantIDs, ov.MaxQueriersPerUser))
	}
}

func TestSmallestPositiveNonZeroDurationPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxQueryLength: time.Hour,
		},
		"tenant-b": {
			MaxQueryLength: 4 * time.Hour,
		},
	}

	defaults := Limits{
		MaxQueryLength: 0,
	}
	ov, err := NewOverrides(defaults, newMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-b"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-c"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: time.Hour},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroDurationPerTenant(tc.tenantIDs, ov.MaxQueryLength))
	}
}
