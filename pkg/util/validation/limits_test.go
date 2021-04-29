package validation

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util/flagext"
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

func TestOverrides_MaxChunksPerQueryFromStore(t *testing.T) {
	tests := map[string]struct {
		setup    func(limits *Limits)
		expected int
	}{
		"should return the default legacy setting with the default config": {
			setup:    func(limits *Limits) {},
			expected: 2000000,
		},
		"the new config option should take precedence over the deprecated one": {
			setup: func(limits *Limits) {
				limits.MaxChunksPerQueryFromStore = 10
				limits.MaxChunksPerQuery = 20
			},
			expected: 20,
		},
		"the deprecated config option should be used if the new config option is unset": {
			setup: func(limits *Limits) {
				limits.MaxChunksPerQueryFromStore = 10
			},
			expected: 10,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := Limits{}
			flagext.DefaultValues(&limits)
			testData.setup(&limits)

			overrides, err := NewOverrides(limits, nil)
			require.NoError(t, err)
			assert.Equal(t, testData.expected, overrides.MaxChunksPerQueryFromStore("test"))
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

func TestLimitsLoadingFromJson(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	inp := `{"ingestion_rate": 0.5}`

	l := Limits{}
	err := json.Unmarshal([]byte(inp), &l)
	require.NoError(t, err)

	assert.Equal(t, 0.5, l.IngestionRate, "from json")
	assert.Equal(t, 100, l.MaxLabelNameLength, "from defaults")
}

func TestLimitsTagsYamlMatchJson(t *testing.T) {
	limits := reflect.TypeOf(Limits{})
	n := limits.NumField()
	var mismatch []string

	for i := 0; i < n; i++ {
		field := limits.Field(i)

		// Note that we aren't requiring YAML and JSON tags to match, just that
		// they either both exist or both don't exist.
		hasYAMLTag := field.Tag.Get("yaml") != ""
		hasJSONTag := field.Tag.Get("json") != ""

		if hasYAMLTag != hasJSONTag {
			mismatch = append(mismatch, field.Name)
		}
	}

	assert.Empty(t, mismatch, "expected no mismatched JSON and YAML tags")
}

func TestLimitsStringDurationYamlMatchJson(t *testing.T) {
	inputYAML := `
max_query_lookback: 1s
max_query_length: 1s
`
	inputJSON := `{"max_query_lookback": "1s", "max_query_length": "1s"}`

	limitsYAML := Limits{}
	err := yaml.Unmarshal([]byte(inputYAML), &limitsYAML)
	require.NoError(t, err, "expected to be able to unmarshal from YAML")

	limitsJSON := Limits{}
	err = json.Unmarshal([]byte(inputJSON), &limitsJSON)
	require.NoError(t, err, "expected to be able to unmarshal from JSON")

	assert.Equal(t, limitsYAML, limitsJSON)
}

func TestLimitsAlwaysUsesPromDuration(t *testing.T) {
	stdlibDuration := reflect.TypeOf(time.Duration(0))
	limits := reflect.TypeOf(Limits{})
	n := limits.NumField()
	var badDurationType []string

	for i := 0; i < n; i++ {
		field := limits.Field(i)
		if field.Type == stdlibDuration {
			badDurationType = append(badDurationType, field.Name)
		}
	}

	assert.Empty(t, badDurationType, "some Limits fields are using stdlib time.Duration instead of model.Duration")
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
			MaxQueryLength: model.Duration(time.Hour),
		},
		"tenant-b": {
			MaxQueryLength: model.Duration(4 * time.Hour),
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

func TestAlertmanagerEmailNotificationLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		inputYAML         string
		expectedRateLimit rate.Limit
		expectedBurstSize int
	}{
		"zero limit": {
			inputYAML: `
alertmanager_email_notification_rate_limit: 0
alertmanager_email_notification_burst_size: 0
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: 0,
		},

		"negative limit": {
			inputYAML: `
alertmanager_email_notification_rate_limit: -10
alertmanager_email_notification_burst_size: 5
`,
			expectedRateLimit: 0,
			expectedBurstSize: 5,
		},

		"positive limit, negative burst": {
			inputYAML: `
alertmanager_email_notification_rate_limit: 222
alertmanager_email_notification_burst_size: -1
`,
			expectedRateLimit: 222,
			expectedBurstSize: 0,
		},

		"infinte limit": {
			inputYAML: `
alertmanager_email_notification_rate_limit: .inf
alertmanager_email_notification_burst_size: 50
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: 50,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(tc.inputYAML), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRateLimit, ov.EmailNotificationRateLimit("user"))
			require.Equal(t, tc.expectedBurstSize, ov.EmailNotificationBurst("user"))
		})
	}
}
