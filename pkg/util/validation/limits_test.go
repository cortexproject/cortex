package validation

import (
	"encoding/json"
	"fmt"
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
		limits                     Limits
		shardByAllLabels           bool
		activeSeriesMetricsEnabled bool
		expected                   error
		nameValidationScheme       model.ValidationScheme
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
		"max-global-native-histogram-series-per-user disabled and shard-by-all-labels=false and active-series-metrics-enabled=false": {
			limits:                     Limits{MaxGlobalSeriesPerUser: 0},
			shardByAllLabels:           false,
			activeSeriesMetricsEnabled: false,
			expected:                   nil,
		},
		"max-global-native-histogram-series-per-user disabled and shard-by-all-labels=true and active-series-metrics-enabled=true": {
			limits:                     Limits{MaxGlobalNativeHistogramSeriesPerUser: 0},
			shardByAllLabels:           true,
			activeSeriesMetricsEnabled: true,
			expected:                   nil,
		},
		"max-global-native-histogram-series-per-user enabled and shard-by-all-labels=true and active-series-metrics-enabled=true": {
			limits:                     Limits{MaxGlobalNativeHistogramSeriesPerUser: 1000},
			shardByAllLabels:           true,
			activeSeriesMetricsEnabled: true,
			expected:                   nil,
		},
		"max-global-native-histogram-series-per-user enabled and shard-by-all-labels=false and active-series-metrics-enabled=true": {
			limits:                     Limits{MaxGlobalNativeHistogramSeriesPerUser: 1000},
			shardByAllLabels:           false,
			activeSeriesMetricsEnabled: true,
			expected:                   errMaxGlobalNativeHistogramSeriesPerUserValidation,
		},
		"max-global-native-histogram-series-per-user enabled and shard-by-all-labels=true and active-series-metrics-enabled=false": {
			limits:                     Limits{MaxGlobalNativeHistogramSeriesPerUser: 1000},
			shardByAllLabels:           true,
			activeSeriesMetricsEnabled: false,
			expected:                   errMaxGlobalNativeHistogramSeriesPerUserValidation,
		},
		"max-local-native-histogram-series-per-user disabled and shard-by-all-labels=true and active-series-metrics-enabled=false": {
			limits:                     Limits{MaxLocalNativeHistogramSeriesPerUser: 0},
			activeSeriesMetricsEnabled: false,
			expected:                   nil,
		},
		"max-local-native-histogram-series-per-user disabled and shard-by-all-labels=true and active-series-metrics-enabled=true": {
			limits:                     Limits{MaxLocalNativeHistogramSeriesPerUser: 0},
			activeSeriesMetricsEnabled: true,
			expected:                   nil,
		},
		"max-local-native-histogram-series-per-user enabled and shard-by-all-labels=true and active-series-metrics-enabled=true": {
			limits:                     Limits{MaxLocalNativeHistogramSeriesPerUser: 1000},
			activeSeriesMetricsEnabled: true,
			expected:                   nil,
		},
		"max-local-native-histogram-series-per-user enabled and shard-by-all-labels=true and active-series-metrics-enabled=false": {
			limits:                     Limits{MaxLocalNativeHistogramSeriesPerUser: 1000},
			activeSeriesMetricsEnabled: false,
			expected:                   errMaxLocalNativeHistogramSeriesPerUserValidation,
		},
		"external-labels invalid label name": {
			limits:   Limits{RulerExternalLabels: labels.FromStrings("123invalid", "good")},
			expected: errInvalidLabelName,
		},
		"external-labels invalid label value": {
			limits:   Limits{RulerExternalLabels: labels.FromStrings("good", string([]byte{0xff, 0xfe, 0xfd}))},
			expected: errInvalidLabelValue,
		},
		"utf8: external-labels utf8 label name and value": {
			limits:               Limits{RulerExternalLabels: labels.FromStrings("test.utf8.metric", "😄")},
			expected:             nil,
			nameValidationScheme: model.UTF8Validation,
		},
		"utf8: external-labels invalid label name": {
			limits:               Limits{RulerExternalLabels: labels.FromStrings("test.\xc5.metric", "😄")},
			expected:             errInvalidLabelName,
			nameValidationScheme: model.UTF8Validation,
		},
		"utf8: external-labels invalid label value": {
			limits:               Limits{RulerExternalLabels: labels.FromStrings("test.utf8.metric", "test.\xc5.value")},
			expected:             errInvalidLabelValue,
			nameValidationScheme: model.UTF8Validation,
		},
		"metric_relabel_configs nil entry": {
			limits: Limits{
				MetricRelabelConfigs: []*relabel.Config{nil},
			},
			expected: errInvalidMetricRelabelConfigs,
		},
		"metric_relabel_configs valid config": {
			limits: Limits{
				MetricRelabelConfigs: []*relabel.Config{
					{
						SourceLabels:         []model.LabelName{"__name__"},
						Action:               relabel.Drop,
						Regex:                relabel.MustNewRegexp("(foo)"),
						NameValidationScheme: model.LegacyValidation,
					},
				},
			},
			expected: nil,
		},
		"metric_relabel_configs invalid config empty action": {
			limits: Limits{
				MetricRelabelConfigs: []*relabel.Config{
					{
						SourceLabels:         []model.LabelName{"__name__"},
						Action:               "",
						Regex:                relabel.DefaultRelabelConfig.Regex,
						NameValidationScheme: model.LegacyValidation,
					},
				},
			},
			expected: errInvalidMetricRelabelConfigs,
		},
		"metric_relabel_configs invalid target_label for legacy": {
			limits: Limits{
				MetricRelabelConfigs: []*relabel.Config{
					{
						SourceLabels:         []model.LabelName{"cluster"},
						Action:               relabel.Replace,
						Regex:                relabel.DefaultRelabelConfig.Regex,
						TargetLabel:          "invalid-label-with-dash",
						Replacement:          "x",
						NameValidationScheme: model.LegacyValidation,
					},
				},
			},
			expected: errInvalidMetricRelabelConfigs,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			nameValidationScheme := model.LegacyValidation
			if testData.nameValidationScheme == model.UTF8Validation {
				nameValidationScheme = testData.nameValidationScheme
			}
			assert.ErrorIs(t, testData.limits.Validate(nameValidationScheme, testData.shardByAllLabels, testData.activeSeriesMetricsEnabled), testData.expected)
		})
	}
}

func TestOverrides_MaxChunksPerQueryFromStore(t *testing.T) {
	limits := Limits{}
	flagext.DefaultValues(&limits)

	overrides := NewOverrides(limits, nil)
	assert.Equal(t, 2000000, overrides.MaxChunksPerQueryFromStore("test"))
}

func TestOverridesManager_GetOverrides(t *testing.T) {
	tenantLimits := map[string]*Limits{}

	defaults := Limits{
		MaxLabelNamesPerSeries: 100,
	}
	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

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
	limits := reflect.TypeFor[Limits]()
	n := limits.NumField()
	var mismatch []string

	for i := range n {
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
	require.Equal(t, 1, limitsYAML.LimitsPerLabelSet[0].LabelSet.Len())
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
	stdlibDuration := reflect.TypeFor[time.Duration]()
	limits := reflect.TypeFor[Limits]()
	n := limits.NumField()
	var badDurationType []string

	for i := range n {
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
	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

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
	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

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
	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

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

			ov := NewOverrides(limitsYAML, nil)

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

			ov := NewOverrides(limitsYAML, tl)

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

	ov := NewOverrides(l, tl)

	require.Equal(t, 1, ov.MaxExemplars("tenant1"))
	require.Equal(t, 3, ov.MaxExemplars("tenant2"))
	require.Equal(t, 5, ov.MaxExemplars("tenant3"))
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

	ov := NewOverrides(l, tl)

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

	ov := NewOverrides(l, tl)

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
	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

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

func TestIsLimitError(t *testing.T) {
	assert.False(t, IsLimitError(fmt.Errorf("test error")))
	assert.True(t, IsLimitError(LimitError("test error")))
}
func TestLimits_ValidateQueryLimits(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		queryIngestersWithin                   time.Duration
		queryStoreAfter                        time.Duration
		shuffleShardingIngestersLookbackPeriod time.Duration
		closeIdleTSDBTimeout                   time.Duration
		expectedError                          string
	}{
		"all limits disabled (zero values) should be valid": {
			queryIngestersWithin:                   0,
			queryStoreAfter:                        0,
			shuffleShardingIngestersLookbackPeriod: 0,
			closeIdleTSDBTimeout:                   0,
			expectedError:                          "",
		},
		"valid configuration with all limits enabled": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "",
		},
		"valid configuration with overlap for safety": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        23 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 26 * time.Hour,
			closeIdleTSDBTimeout:                   30 * time.Hour,
			expectedError:                          "",
		},
		"valid configuration with only queryIngestersWithin enabled": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        0,
			shuffleShardingIngestersLookbackPeriod: 0,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "",
		},
		"valid configuration with only queryStoreAfter enabled": {
			queryIngestersWithin:                   0,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   0,
			expectedError:                          "",
		},
		"invalid: queryIngestersWithin >= closeIdleTSDBTimeout": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   25 * time.Hour,
			expectedError:                          "query_ingesters_within (25h0m0s) must be less than close_idle_tsdb_timeout (25h0m0s)",
		},
		"invalid: queryIngestersWithin > closeIdleTSDBTimeout": {
			queryIngestersWithin:                   26 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 26 * time.Hour,
			closeIdleTSDBTimeout:                   25 * time.Hour,
			expectedError:                          "query_ingesters_within (26h0m0s) must be less than close_idle_tsdb_timeout (25h0m0s)",
		},
		"invalid: queryStoreAfter >= queryIngestersWithin": {
			queryIngestersWithin:                   24 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "query_store_after (24h0m0s) must be less than query_ingesters_within (24h0m0s)",
		},
		"invalid: queryStoreAfter > queryIngestersWithin": {
			queryIngestersWithin:                   24 * time.Hour,
			queryStoreAfter:                        25 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 26 * time.Hour,
			closeIdleTSDBTimeout:                   27 * time.Hour,
			expectedError:                          "query_store_after (25h0m0s) must be less than query_ingesters_within (24h0m0s)",
		},
		"invalid: shuffleShardingLookback < queryStoreAfter": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 20 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "shuffle_sharding_ingesters_lookback_period (20h0m0s) is less than query_store_after (24h0m0s)",
		},
		"valid: shuffleShardingLookback between queryStoreAfter and queryIngestersWithin": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        20 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 22 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "",
		},
		"boundary: queryIngestersWithin exactly 1ms less than closeIdleTSDBTimeout": {
			queryIngestersWithin:                   25*time.Hour - time.Millisecond,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   25 * time.Hour,
			expectedError:                          "",
		},
		"boundary: queryStoreAfter exactly 1ms less than queryIngestersWithin": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        25*time.Hour - time.Millisecond,
			shuffleShardingIngestersLookbackPeriod: 25 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "",
		},
		"boundary: shuffleShardingLookback exactly equal to queryStoreAfter": {
			queryIngestersWithin:                   25 * time.Hour,
			queryStoreAfter:                        24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 24 * time.Hour,
			closeIdleTSDBTimeout:                   26 * time.Hour,
			expectedError:                          "",
		},
		"edge case: very large values": {
			queryIngestersWithin:                   365 * 24 * time.Hour, // 1 year
			queryStoreAfter:                        364 * 24 * time.Hour,
			shuffleShardingIngestersLookbackPeriod: 365 * 24 * time.Hour,
			closeIdleTSDBTimeout:                   366 * 24 * time.Hour,
			expectedError:                          "",
		},
		"edge case: very small values": {
			queryIngestersWithin:                   2 * time.Second,
			queryStoreAfter:                        1 * time.Second,
			shuffleShardingIngestersLookbackPeriod: 2 * time.Second,
			closeIdleTSDBTimeout:                   3 * time.Second,
			expectedError:                          "",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := Limits{
				QueryIngestersWithin:                   model.Duration(testData.queryIngestersWithin),
				QueryStoreAfter:                        model.Duration(testData.queryStoreAfter),
				ShuffleShardingIngestersLookbackPeriod: model.Duration(testData.shuffleShardingIngestersLookbackPeriod),
			}

			err := limits.ValidateQueryLimits("test-tenant", testData.closeIdleTSDBTimeout)

			if testData.expectedError == "" {
				assert.NoError(t, err, "expected no error but got: %v", err)
			} else {
				assert.Error(t, err, "expected error but got none")
				if err != nil {
					assert.Contains(t, err.Error(), testData.expectedError, "error message mismatch")
				}
			}
		})
	}
}

func TestQueryLimits_TenantOverrides(t *testing.T) {
	t.Parallel()

	// Setup: Create three tenants with different query limit configurations
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			QueryIngestersWithin:                   model.Duration(1 * time.Hour),
			QueryStoreAfter:                        model.Duration(30 * time.Minute),
			ShuffleShardingIngestersLookbackPeriod: model.Duration(1 * time.Hour),
		},
		"tenant-b": {
			QueryIngestersWithin:                   model.Duration(2 * time.Hour),
			QueryStoreAfter:                        model.Duration(1 * time.Hour),
			ShuffleShardingIngestersLookbackPeriod: model.Duration(2 * time.Hour),
		},
		"tenant-c": {
			// Uses defaults (all zeros - disabled)
			QueryIngestersWithin:                   0,
			QueryStoreAfter:                        0,
			ShuffleShardingIngestersLookbackPeriod: 0,
		},
	}

	defaults := Limits{
		QueryIngestersWithin:                   model.Duration(25 * time.Hour),
		QueryStoreAfter:                        model.Duration(24 * time.Hour),
		ShuffleShardingIngestersLookbackPeriod: model.Duration(25 * time.Hour),
	}

	ov := NewOverrides(defaults, newMockTenantLimits(tenantLimits))

	// Verify tenant-a gets their specific limits
	assert.Equal(t, 1*time.Hour, ov.QueryIngestersWithin("tenant-a"))
	assert.Equal(t, 30*time.Minute, ov.QueryStoreAfter("tenant-a"))
	assert.Equal(t, 1*time.Hour, ov.ShuffleShardingIngestersLookbackPeriod("tenant-a"))

	// Verify tenant-b gets their specific limits
	assert.Equal(t, 2*time.Hour, ov.QueryIngestersWithin("tenant-b"))
	assert.Equal(t, 1*time.Hour, ov.QueryStoreAfter("tenant-b"))
	assert.Equal(t, 2*time.Hour, ov.ShuffleShardingIngestersLookbackPeriod("tenant-b"))

	// Verify tenant-c gets their specific limits (zeros)
	assert.Equal(t, time.Duration(0), ov.QueryIngestersWithin("tenant-c"))
	assert.Equal(t, time.Duration(0), ov.QueryStoreAfter("tenant-c"))
	assert.Equal(t, time.Duration(0), ov.ShuffleShardingIngestersLookbackPeriod("tenant-c"))

	// Verify unknown tenant gets defaults
	assert.Equal(t, 25*time.Hour, ov.QueryIngestersWithin("tenant-unknown"))
	assert.Equal(t, 24*time.Hour, ov.QueryStoreAfter("tenant-unknown"))
	assert.Equal(t, 25*time.Hour, ov.ShuffleShardingIngestersLookbackPeriod("tenant-unknown"))
}

func TestQueryLimits_TenantOverridesValidation(t *testing.T) {
	t.Parallel()

	closeIdleTSDBTimeout := 26 * time.Hour

	tests := map[string]struct {
		tenantLimits  map[string]*Limits
		tenantID      string
		expectedError string
	}{
		"valid tenant configuration": {
			tenantLimits: map[string]*Limits{
				"valid-tenant": {
					QueryIngestersWithin:                   model.Duration(25 * time.Hour),
					QueryStoreAfter:                        model.Duration(24 * time.Hour),
					ShuffleShardingIngestersLookbackPeriod: model.Duration(25 * time.Hour),
				},
			},
			tenantID:      "valid-tenant",
			expectedError: "",
		},
		"invalid tenant: queryStoreAfter >= queryIngestersWithin": {
			tenantLimits: map[string]*Limits{
				"invalid-tenant": {
					QueryIngestersWithin:                   model.Duration(24 * time.Hour),
					QueryStoreAfter:                        model.Duration(25 * time.Hour),
					ShuffleShardingIngestersLookbackPeriod: model.Duration(26 * time.Hour),
				},
			},
			tenantID:      "invalid-tenant",
			expectedError: "query_store_after (25h0m0s) must be less than query_ingesters_within (24h0m0s)",
		},
		"invalid tenant: queryIngestersWithin >= closeIdleTSDBTimeout": {
			tenantLimits: map[string]*Limits{
				"invalid-tenant": {
					QueryIngestersWithin:                   model.Duration(26 * time.Hour),
					QueryStoreAfter:                        model.Duration(24 * time.Hour),
					ShuffleShardingIngestersLookbackPeriod: model.Duration(26 * time.Hour),
				},
			},
			tenantID:      "invalid-tenant",
			expectedError: "query_ingesters_within (26h0m0s) must be less than close_idle_tsdb_timeout (26h0m0s)",
		},
		"invalid tenant: shuffleShardingLookback < queryStoreAfter": {
			tenantLimits: map[string]*Limits{
				"invalid-tenant": {
					QueryIngestersWithin:                   model.Duration(25 * time.Hour),
					QueryStoreAfter:                        model.Duration(24 * time.Hour),
					ShuffleShardingIngestersLookbackPeriod: model.Duration(20 * time.Hour),
				},
			},
			tenantID:      "invalid-tenant",
			expectedError: "shuffle_sharding_ingesters_lookback_period (20h0m0s) is less than query_store_after (24h0m0s)",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := testData.tenantLimits[testData.tenantID]
			err := limits.ValidateQueryLimits(testData.tenantID, closeIdleTSDBTimeout)

			if testData.expectedError == "" {
				assert.NoError(t, err, "expected no error but got: %v", err)
			} else {
				assert.Error(t, err, "expected error but got none")
				if err != nil {
					assert.Contains(t, err.Error(), testData.expectedError, "error message mismatch")
					assert.Contains(t, err.Error(), testData.tenantID, "error should contain tenant ID")
				}
			}
		})
	}
}
