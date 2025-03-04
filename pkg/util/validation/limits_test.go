package validation

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
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
		"external-labels invalid label name": {
			limits:   Limits{RulerExternalLabels: labels.Labels{{Name: "123invalid", Value: "good"}}},
			expected: errInvalidLabelName,
		},
		"external-labels invalid label value": {
			limits:   Limits{RulerExternalLabels: labels.Labels{{Name: "good", Value: string([]byte{0xff, 0xfe, 0xfd})}}},
			expected: errInvalidLabelValue,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			assert.ErrorIs(t, testData.limits.Validate(testData.shardByAllLabels), testData.expected)
		})
	}
}

func TestOverrides_MaxChunksPerQueryFromStore(t *testing.T) {
	limits := Limits{}
	flagext.DefaultValues(&limits)

	overrides, err := NewOverrides(limits, nil)
	require.NoError(t, err)
	assert.Equal(t, 2000000, overrides.MaxChunksPerQueryFromStore("test"))
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
	require.Equal(t, 0, ov.MaxLabelsSizeBytes("user1"))

	// Update limits for tenant user1. We only update single field, the rest is copied from defaults.
	// (That is how limits work when loaded from YAML)
	l := defaults
	l.MaxLabelValueLength = 150
	l.MaxLabelsSizeBytes = 10

	tenantLimits["user1"] = &l

	// Checking whether overrides were enforced
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 150, ov.MaxLabelValueLength("user1"))
	require.Equal(t, 10, ov.MaxLabelsSizeBytes("user1"))

	// Verifying user2 limits are not impacted by overrides
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user2"))
	require.Equal(t, 0, ov.MaxLabelValueLength("user2"))
	require.Equal(t, 0, ov.MaxLabelsSizeBytes("user2"))
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

	// Unmarshal should fail if input contains unknown struct fields and
	// the decoder flag `json.Decoder.DisallowUnknownFields()` is set
	inp = `{"unknown_fields": 100}`
	l = Limits{}
	dec := json.NewDecoder(strings.NewReader(inp))
	dec.DisallowUnknownFields()
	err = dec.Decode(&l)
	assert.Error(t, err)
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

func TestOverrides_LimitsPerLabelSet(t *testing.T) {
	inputYAML := `
limits_per_label_set:
  - label_set:
      labelName1: LabelValue1
    limits:
      max_series: 10
`

	limitsYAML := Limits{}
	err := yaml.Unmarshal([]byte(inputYAML), &limitsYAML)
	require.NoError(t, err)
	require.Len(t, limitsYAML.LimitsPerLabelSet, 1)
	require.Len(t, limitsYAML.LimitsPerLabelSet[0].LabelSet, 1)
	require.Equal(t, limitsYAML.LimitsPerLabelSet[0].Limits.MaxSeries, 10)

	duplicatedInputYAML := `
limits_per_label_set:
  - label_set:
      labelName1: LabelValue1
    limits:
      max_series: 10
  - label_set:
      labelName1: LabelValue1
    limits:
      max_series: 10
`
	err = yaml.Unmarshal([]byte(duplicatedInputYAML), &limitsYAML)
	require.Equal(t, err, errDuplicatePerLabelSetLimit)
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

func TestSmallestPositiveNonZeroFloat64PerTenant(t *testing.T) {
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
		expLimit  float64
	}{
		{tenantIDs: []string{}, expLimit: 0},
		{tenantIDs: []string{"tenant-a"}, expLimit: 5},
		{tenantIDs: []string{"tenant-b"}, expLimit: 10},
		{tenantIDs: []string{"tenant-c"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 5},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 5},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroFloat64PerTenant(tc.tenantIDs, ov.MaxQueriersPerUser))
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

func TestAlertmanagerNotificationLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		inputYAML         string
		expectedRateLimit rate.Limit
		expectedBurstSize int
	}{
		"no email specific limit": {
			inputYAML: `
alertmanager_notification_rate_limit: 100
`,
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},
		"zero limit": {
			inputYAML: `
alertmanager_notification_rate_limit: 100

alertmanager_notification_rate_limit_per_integration:
  email: 0
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: maxInt,
		},

		"negative limit": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: -10
`,
			expectedRateLimit: 0,
			expectedBurstSize: 0,
		},

		"positive limit, negative burst": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: 222
`,
			expectedRateLimit: 222,
			expectedBurstSize: 222,
		},

		"infinite limit": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: .inf
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: maxInt,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(tc.inputYAML), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRateLimit, ov.NotificationRateLimit("user", "email"))
			require.Equal(t, tc.expectedBurstSize, ov.NotificationBurstSize("user", "email"))
		})
	}
}

func TestAlertmanagerNotificationLimitsOverrides(t *testing.T) {
	baseYaml := `
alertmanager_notification_rate_limit: 5

alertmanager_notification_rate_limit_per_integration:
 email: 100
`

	overrideGenericLimitsOnly := `
testuser:
  alertmanager_notification_rate_limit: 333
`

	overrideEmailLimits := `
testuser:
  alertmanager_notification_rate_limit_per_integration:
    email: 7777
`

	overrideGenericLimitsAndEmailLimits := `
testuser:
  alertmanager_notification_rate_limit: 333

  alertmanager_notification_rate_limit_per_integration:
    email: 7777
`

	differentUserOverride := `
differentuser:
  alertmanager_notification_limits_per_integration:
    email: 500
`

	for name, tc := range map[string]struct {
		testedIntegration string
		overrides         string
		expectedRateLimit rate.Limit
		expectedBurstSize int
	}{
		"no overrides, pushover": {
			testedIntegration: "pushover",
			expectedRateLimit: 5,
			expectedBurstSize: 5,
		},

		"no overrides, email": {
			testedIntegration: "email",
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},

		"generic override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideGenericLimitsOnly,
			expectedRateLimit: 333,
			expectedBurstSize: 333,
		},

		"generic override, email": {
			testedIntegration: "email",
			overrides:         overrideGenericLimitsOnly,
			expectedRateLimit: 100, // there is email-specific override in default config.
			expectedBurstSize: 100,
		},

		"email limit override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideEmailLimits,
			expectedRateLimit: 5, // loaded from defaults when parsing YAML
			expectedBurstSize: 5,
		},

		"email limit override, email": {
			testedIntegration: "email",
			overrides:         overrideEmailLimits,
			expectedRateLimit: 7777,
			expectedBurstSize: 7777,
		},

		"generic and email limit override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideGenericLimitsAndEmailLimits,
			expectedRateLimit: 333,
			expectedBurstSize: 333,
		},

		"generic and email limit override, email": {
			testedIntegration: "email",
			overrides:         overrideGenericLimitsAndEmailLimits,
			expectedRateLimit: 7777,
			expectedBurstSize: 7777,
		},

		"partial email limit override": {
			testedIntegration: "email",
			overrides: `
testuser:
  alertmanager_notification_rate_limit_per_integration:
    email: 500
`,
			expectedRateLimit: 500, // overridden
			expectedBurstSize: 500, // same as rate limit
		},

		"different user override, pushover": {
			testedIntegration: "pushover",
			overrides:         differentUserOverride,
			expectedRateLimit: 5,
			expectedBurstSize: 5,
		},

		"different user overridem, email": {
			testedIntegration: "email",
			overrides:         differentUserOverride,
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},
	} {
		t.Run(name, func(t *testing.T) {
			SetDefaultLimitsForYAMLUnmarshalling(Limits{})

			limitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(baseYaml), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			SetDefaultLimitsForYAMLUnmarshalling(limitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tc.overrides), &overrides)
			require.NoError(t, err, "parsing overrides")

			tl := newMockTenantLimits(overrides)

			ov, err := NewOverrides(limitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRateLimit, ov.NotificationRateLimit("testuser", tc.testedIntegration))
			require.Equal(t, tc.expectedBurstSize, ov.NotificationBurstSize("testuser", tc.testedIntegration))
		})
	}
}

func TestMaxExemplarsOverridesPerTenant(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	baseYAML := `
max_exemplars: 5`
	overridesYAML := `
tenant1:
  max_exemplars: 1
tenant2:
  max_exemplars: 3
`

	l := Limits{}
	err := yaml.UnmarshalStrict([]byte(baseYAML), &l)
	require.NoError(t, err)

	overrides := map[string]*Limits{}
	err = yaml.Unmarshal([]byte(overridesYAML), &overrides)
	require.NoError(t, err, "parsing overrides")

	tl := newMockTenantLimits(overrides)

	ov, err := NewOverrides(l, tl)
	require.NoError(t, err)

	require.Equal(t, 1, ov.MaxExemplars("tenant1"))
	require.Equal(t, 3, ov.MaxExemplars("tenant2"))
	require.Equal(t, 5, ov.MaxExemplars("tenant3"))
}

func TestEnableOOONativeHistograms(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	baseYAML := `
enable_ooo_native_histograms: false`
	overridesYAML := `
tenant1:
  enable_ooo_native_histograms: true
tenant2:
  enable_ooo_native_histograms: false
`

	l := Limits{}
	err := yaml.UnmarshalStrict([]byte(baseYAML), &l)
	require.NoError(t, err)

	overrides := map[string]*Limits{}
	err = yaml.Unmarshal([]byte(overridesYAML), &overrides)
	require.NoError(t, err, "parsing overrides")

	tl := newMockTenantLimits(overrides)

	ov, err := NewOverrides(l, tl)
	require.NoError(t, err)

	require.Equal(t, true, ov.EnableOOONativeHistograms("tenant1"))
	require.Equal(t, false, ov.EnableOOONativeHistograms("tenant2"))
	require.Equal(t, false, ov.EnableOOONativeHistograms("tenant3"))
}

func TestMaxDownloadedBytesPerRequestOverridesPerTenant(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	baseYAML := `
max_downloaded_bytes_per_request: 5`
	overridesYAML := `
tenant1:
  max_downloaded_bytes_per_request: 1
tenant2:
  max_downloaded_bytes_per_request: 3
`

	l := Limits{}
	err := yaml.UnmarshalStrict([]byte(baseYAML), &l)
	require.NoError(t, err)

	overrides := map[string]*Limits{}
	err = yaml.Unmarshal([]byte(overridesYAML), &overrides)
	require.NoError(t, err, "parsing overrides")

	tl := newMockTenantLimits(overrides)

	ov, err := NewOverrides(l, tl)
	require.NoError(t, err)

	require.Equal(t, 1, ov.MaxDownloadedBytesPerRequest("tenant1"))
	require.Equal(t, 3, ov.MaxDownloadedBytesPerRequest("tenant2"))
	require.Equal(t, 5, ov.MaxDownloadedBytesPerRequest("tenant3"))
}

func TestPartialDataOverridesPerTenant(t *testing.T) {
	SetDefaultLimitsForYAMLUnmarshalling(Limits{})

	baseYAML := `
query_partial_data: false
rules_partial_data: false`
	overridesYAML := `
tenant1:
  query_partial_data: true
tenant2:
  query_partial_data: true
  rules_partial_data: true`

	l := Limits{}
	err := yaml.UnmarshalStrict([]byte(baseYAML), &l)
	require.NoError(t, err)

	overrides := map[string]*Limits{}
	err = yaml.Unmarshal([]byte(overridesYAML), &overrides)
	require.NoError(t, err, "parsing overrides")

	tl := newMockTenantLimits(overrides)

	ov, err := NewOverrides(l, tl)
	require.NoError(t, err)

	require.True(t, ov.QueryPartialData("tenant1"))
	require.False(t, ov.RulesPartialData("tenant1"))
	require.True(t, ov.QueryPartialData("tenant2"))
	require.True(t, ov.RulesPartialData("tenant2"))
	require.False(t, ov.QueryPartialData("tenant3"))
	require.False(t, ov.RulesPartialData("tenant3"))
}

func TestHasQueryAttributeRegexChanged(t *testing.T) {
	l := Limits{
		QueryPriority: QueryPriority{
			Enabled: true,
			Priorities: []PriorityDef{
				{
					Priority: 1,
					QueryAttributes: []QueryAttribute{
						{
							Regex: "test",
						},
					},
				},
			},
		},
		QueryRejection: QueryRejection{
			Enabled: true,
			QueryAttributes: []QueryAttribute{
				{
					Regex: "testRejection",
				},
			},
		},
	}

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryPriority.Priorities[0].QueryAttributes[0].Regex = "new"

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryPriority.Priorities[0].QueryAttributes[0].TimeWindow.Start = model.Duration(2 * time.Hour)

	require.False(t, l.hasQueryAttributeRegexChanged())

	l.QueryPriority.Priorities[0].QueryAttributes = append(l.QueryPriority.Priorities[0].QueryAttributes, QueryAttribute{Regex: "hi"})

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryPriority.Priorities[0].QueryAttributes = l.QueryPriority.Priorities[0].QueryAttributes[:1]

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryRejection.QueryAttributes[0].Regex = "newRejectionRegex"

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryRejection.QueryAttributes = append(l.QueryRejection.QueryAttributes, QueryAttribute{Regex: "new element"})

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryRejection.QueryAttributes[1].UserAgentRegex = "New User agent regex"

	require.True(t, l.hasQueryAttributeRegexChanged())

	l.QueryRejection.QueryAttributes[1].DashboardUID = "New Dashboard Uid"

	require.False(t, l.hasQueryAttributeRegexChanged())

	l.QueryPriority.Enabled = false

	require.True(t, l.hasQueryAttributeRegexChanged())
}

func TestCompileQueryPriorityRegex(t *testing.T) {
	l := Limits{
		QueryPriority: QueryPriority{
			Enabled: true,
			Priorities: []PriorityDef{
				{
					Priority: 1,
					QueryAttributes: []QueryAttribute{
						{
							Regex: "test",
						},
					},
				},
			},
		},
		QueryRejection: QueryRejection{
			Enabled: false,
			QueryAttributes: []QueryAttribute{
				{
					Regex: "testRejection",
				},
			},
		},
	}

	require.Nil(t, l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)

	err := l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Equal(t, regexp.MustCompile("test"), l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)

	l.QueryPriority.Priorities[0].QueryAttributes[0].Regex = "new"

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Equal(t, regexp.MustCompile("new"), l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)

	l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = nil

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Equal(t, regexp.MustCompile("new"), l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)

	l.QueryPriority.Enabled = false
	l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex = nil

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Nil(t, l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)

	require.Nil(t, l.QueryRejection.QueryAttributes[0].CompiledRegex)

	l.QueryRejection.Enabled = true

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Equal(t, regexp.MustCompile("testRejection"), l.QueryRejection.QueryAttributes[0].CompiledRegex)
	require.Equal(t, regexp.MustCompile(""), l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledUserAgentRegex)

	l.QueryRejection.QueryAttributes[0].UserAgentRegex = "User agent added"

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Equal(t, regexp.MustCompile("User agent added"), l.QueryRejection.QueryAttributes[0].CompiledUserAgentRegex)

	l.QueryRejection.QueryAttributes[0].Regex = ""

	err = l.compileQueryAttributeRegex()
	require.NoError(t, err)
	require.Nil(t, l.QueryPriority.Priorities[0].QueryAttributes[0].CompiledRegex)
}

func TestEvaluationDelayHigherThanRulerQueryOffset(t *testing.T) {
	tenant := "tenant"
	evaluationDelay := time.Duration(10)
	tenantLimits := map[string]*Limits{
		tenant: {
			RulerQueryOffset:     5,
			RulerEvaluationDelay: model.Duration(evaluationDelay),
		},
	}

	defaults := Limits{}
	ov, err := NewOverrides(defaults, newMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	rulerQueryOffset := ov.RulerQueryOffset(tenant)
	assert.Equal(t, evaluationDelay, rulerQueryOffset)
}

func TestLimitsPerLabelSetsForSeries(t *testing.T) {
	for _, tc := range []struct {
		name           string
		limits         []LimitsPerLabelSet
		metric         labels.Labels
		expectedLimits []LimitsPerLabelSet
	}{
		{
			name:   "no limits",
			metric: labels.FromMap(map[string]string{"foo": "bar"}),
		},
		{
			name:   "no limits matched",
			metric: labels.FromMap(map[string]string{"foo": "bar"}),
			limits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "baz"})},
			},
			expectedLimits: []LimitsPerLabelSet{},
		},
		{
			name:   "one limit matched",
			metric: labels.FromMap(map[string]string{"foo": "bar"}),
			limits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "baz"})},
				{LabelSet: labels.FromMap(map[string]string{"foo": "bar"})},
			},
			expectedLimits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "bar"})},
			},
		},
		{
			name:   "default limit matched",
			metric: labels.FromMap(map[string]string{"foo": "bar"}),
			limits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "baz"})},
				{LabelSet: labels.FromMap(map[string]string{})},
			},
			expectedLimits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{})},
			},
		},
		{
			name:   "one limit matched so not picking default limit",
			metric: labels.FromMap(map[string]string{"foo": "bar", "cluster": "us-west-2"}),
			limits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "bar", "cluster": "us-west-2"})},
				{LabelSet: labels.FromMap(map[string]string{})},
			},
			expectedLimits: []LimitsPerLabelSet{
				{LabelSet: labels.FromMap(map[string]string{"foo": "bar", "cluster": "us-west-2"})},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			matched := LimitsPerLabelSetsForSeries(tc.limits, tc.metric)
			require.Equal(t, tc.expectedLimits, matched)
		})
	}
}
