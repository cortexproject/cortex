package tenantfederation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
)

const (
	maxt, mint = 0, 10
	// matchersNotImplemented is a message used to indicate that the MergeQueryable does not yet support filtering by matchers.
	// This message will be removed once matchers have been implemented in the MergeQueryable.
	matchersNotImplemented = "matchers are not implemented in the MergeQueryable"
	// mockMatchersNotImplemented is a message used to indicate that the mockTenantQueryable used in the tests does not support filtering by matchers.
	mockMatchersNotImplemented = "matchers are not implemented in the mockTenantQueryable"
	// originalDefaultTenantLabel is the default tenant label with a prefix.
	// It is used to prevent matcher clashes for timeseries that happen to have a label with the same name as the default tenant label.
	originalDefaultTenantLabel = retainExistingPrefix + defaultTenantLabel
)

// mockTenantQueryableWithFilter is a storage.Queryable that can be use to return specific warnings or errors by tenant.
type mockTenantQueryableWithFilter struct {
	// extraLabels are labels added to all series for all tenants.
	extraLabels []string
	// warningsByTenant are warnings that will be returned for queries of that tenant.
	warningsByTenant map[string]storage.Warnings
	// queryErrByTenant is an error that will be returne for queries of that tenant.
	queryErrByTenant map[string]error
}

// Querier implements the storage.Queryable interface.
func (m *mockTenantQueryableWithFilter) Querier(ctx context.Context, _, _ int64) (storage.Querier, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}

	q := mockTenantQuerier{
		tenant:      tenantIDs[0],
		extraLabels: m.extraLabels,
		ctx:         ctx,
	}

	// set warning if exists
	if m.warningsByTenant != nil {
		if w, ok := m.warningsByTenant[q.tenant]; ok {
			q.warnings = append([]error(nil), w...)
		}
	}

	// set queryErr if exists
	if m.queryErrByTenant != nil {
		if err, ok := m.queryErrByTenant[q.tenant]; ok {
			q.queryErr = err
		}
	}

	return q, nil
}

// UseQueryable implements the querier.QueryableWithFilter interface.
// It ensures the mockTenantQueryableWithFilter storage.Queryable is always used.
func (m *mockTenantQueryableWithFilter) UseQueryable(_ time.Time, _, _ int64) bool {
	return true
}

type mockTenantQuerier struct {
	tenant      string
	extraLabels []string

	warnings storage.Warnings
	queryErr error
	ctx      context.Context
}

func (m mockTenantQuerier) matrix() model.Matrix {
	matrix := model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{
				"instance":                            "host1",
				"tenant-" + model.LabelName(m.tenant): "static",
			},
		},
		&model.SampleStream{
			Metric: model.Metric{
				"instance": "host2." + model.LabelValue(m.tenant),
			},
		},
	}

	// Add extra labels to every sample stream in the matrix.
	for pos := range m.extraLabels {
		if pos%2 == 0 {
			continue
		}

		for mPos := range matrix {
			matrix[mPos].Metric[model.LabelName(m.extraLabels[pos-1])] = model.LabelValue(m.extraLabels[pos])
		}
	}

	return matrix

}

// metricMatches returns whether or not the selector matches the provided metric.
func metricMatches(m model.Metric, selector labels.Selector) bool {
	var labelStrings []string
	for key, value := range m {
		labelStrings = append(labelStrings, string(key), string(value))
	}

	return selector.Matches(labels.FromStrings(labelStrings...))
}

type mockSeriesSet struct {
	upstream storage.SeriesSet
	warnings storage.Warnings
	queryErr error
}

func (m *mockSeriesSet) Next() bool {
	return m.upstream.Next()
}

// At implements the storage.SeriesSet interface. It returns full series. Returned series should be iterable even after Next is called.
func (m *mockSeriesSet) At() storage.Series {
	return m.upstream.At()
}

// Err implements the storage.SeriesSet interface. It returns the error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *mockSeriesSet) Err() error {
	return m.queryErr
}

// Warnings implements the storage.SeriesSet interface. It returns a collection of warnings for the whole set.
// Warnings could be returned even if iteration has not failed with error.
func (m *mockSeriesSet) Warnings() storage.Warnings {
	return m.warnings
}

// Select implements the storage.Querier interface.
func (m mockTenantQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, _ := spanlogger.New(m.ctx, "mockTenantQuerier.select")
	defer log.Span.Finish()
	var matrix model.Matrix

	for _, s := range m.matrix() {
		if metricMatches(s.Metric, matchers) {
			matrix = append(matrix, s)
		}
	}

	return &mockSeriesSet{
		upstream: series.MatrixToSeriesSet(matrix),
		warnings: m.warnings,
		queryErr: m.queryErr,
	}
}

// LabelValues implements the storage.LabelQuerier interface.
// The mockTenantQuerier returns all a sorted slice of all label values and does not support reducing the result set with matchers.
func (m mockTenantQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(matchers) > 0 {
		m.warnings = append(m.warnings, errors.New(mockMatchersNotImplemented))
	}

	if m.queryErr != nil {
		return nil, nil, m.queryErr
	}

	labelValues := make(map[string]struct{})
	for _, s := range m.matrix() {
		for k, v := range s.Metric {
			if k == model.LabelName(name) {
				labelValues[string(v)] = struct{}{}
			}
		}
	}
	var results []string
	for k := range labelValues {
		results = append(results, k)
	}
	sort.Strings(results)
	return results, m.warnings, nil
}

// LabelNames implements the storage.LabelQuerier interface.
// It returns a sorted slice of all label names in the querier.
func (m mockTenantQuerier) LabelNames() ([]string, storage.Warnings, error) {
	if m.queryErr != nil {
		return nil, nil, m.queryErr
	}

	labelValues := make(map[string]struct{})
	for _, s := range m.matrix() {
		for k := range s.Metric {
			labelValues[string(k)] = struct{}{}
		}
	}
	var results []string
	for k := range labelValues {
		results = append(results, k)
	}
	sort.Strings(results)
	return results, m.warnings, nil
}

// Close implements the storage.LabelQuerier interface.
func (mockTenantQuerier) Close() error {
	return nil
}

// mergeQueryableScenario is a setup for testing a a MergeQueryable.
type mergeQueryableScenario struct {
	// name is a description of the scenario.
	name string
	// tenants are the tenants over which queries will be merged.
	tenants   []string
	queryable mockTenantQueryableWithFilter
	// doNotByPassSingleQuerier determines whether the MergeQueryable is by-passed in favor of a single querier.
	doNotByPassSingleQuerier bool
}

func (s *mergeQueryableScenario) init() (storage.Querier, error) {
	// initialize with default tenant label
	q := NewQueryable(&s.queryable, !s.doNotByPassSingleQuerier)

	// inject tenants into context
	ctx := context.Background()
	if len(s.tenants) > 0 {
		ctx = user.InjectOrgID(ctx, strings.Join(s.tenants, "|"))
	}

	// retrieve querier
	return q.Querier(ctx, mint, maxt)
}

// selectTestCase is the inputs and expected outputs of a call to Select.
type selectTestCase struct {
	// name is a description of the test case.
	name string
	// matchers is a slice of label matchers used to filter series in the test case.
	matchers []*labels.Matcher
	// expectedSeriesCount is the expected number of series returned by a Select filtered by the Matchers in selector.
	expectedSeriesCount int
	// expectedWarnings is a slice of storage.Warnings messages expected when querying.
	expectedWarnings []string
	// expectedQueryErr is the error expected when querying.
	expectedQueryErr error
}

// selectScenario tests a call to Select over a range of test cases in a specific scenario.
type selectScenario struct {
	mergeQueryableScenario
	selectTestCases []selectTestCase
}

// labelNamesTestCase is the inputs and expected outputs of a call to LabelNames.
type labelNamesTestCase struct {
	// name is a description of the test case.
	name string
	// expectedLabelNames are the expected label names returned from the queryable.
	expectedLabelNames []string
	// expectedWarnings is a slice of storage.Warnings messages expected when querying.
	expectedWarnings []string
	// expectedQueryErr is the error expected when querying.
	expectedQueryErr error
}

// labelNamesScenario tests a call to LabelNames in a specific scenario.
type labelNamesScenario struct {
	mergeQueryableScenario
	labelNamesTestCase
}

// labeValuesTestCase is the inputs and expected outputs of a call to LabelValues.
type labelValuesTestCase struct {
	// name is a description of the test case.
	name string
	// labelName is the name of the label to query values for.
	labelName string
	// matchers is a slice of label matchers used to filter series in the test case.
	matchers []*labels.Matcher
	// expectedLabelValues are the expected label values returned from the queryable.
	expectedLabelValues []string
	// expectedWarnings is a slice of storage.Warnings messages expected when querying.
	expectedWarnings []string
	// expectedQueryErr is the error expected when querying.
	expectedQueryErr error
	// skipReason is a reason that the test case should be skipped. An empty reason, represented by an empty string will mean that a test case is not skipped.
	skipReason string
}

// labelValuesScenario tests a call to LabelValues over a range of test cases in a specific scenario.
type labelValuesScenario struct {
	mergeQueryableScenario
	labelValuesTestCases []labelValuesTestCase
}

func TestMergeQueryable_Querier(t *testing.T) {
	t.Run("querying without a tenant specified should error", func(t *testing.T) {
		queryable := &mockTenantQueryableWithFilter{}
		q := NewQueryable(queryable, false /* byPassWithSingleQuerier */)
		// Create a context with no tenant specified.
		ctx := context.Background()

		_, err := q.Querier(ctx, mint, maxt)
		require.EqualError(t, err, user.ErrNoOrgID.Error())
	})
}

var (
	singleTenantScenario = mergeQueryableScenario{
		name:    "single tenant",
		tenants: []string{"team-a"},
	}

	singleTenantNoBypassScenario = mergeQueryableScenario{
		name:                     "single tenant without bypass",
		tenants:                  []string{"team-a"},
		doNotByPassSingleQuerier: true,
	}

	threeTenantsScenario = mergeQueryableScenario{
		name:    "three tenants",
		tenants: []string{"team-a", "team-b", "team-c"},
	}

	threeTenantsWithDefaultTenantIDScenario = mergeQueryableScenario{
		name:    "three tenants and the __tenant_id__ label set",
		tenants: []string{"team-a", "team-b", "team-c"},
		queryable: mockTenantQueryableWithFilter{
			extraLabels: []string{defaultTenantLabel, "original-value"},
		},
	}

	threeTenantsWithWarningsScenario = mergeQueryableScenario{
		name:    "three tenants, two with warnings",
		tenants: []string{"team-a", "team-b", "team-c"},
		queryable: mockTenantQueryableWithFilter{
			warningsByTenant: map[string]storage.Warnings{
				"team-b": storage.Warnings([]error{errors.New("don't like them")}),
				"team-c": storage.Warnings([]error{errors.New("out of office")}),
			},
		},
	}

	threeTenantsWithErrorScenario = mergeQueryableScenario{
		name:    "three tenants, one erroring",
		tenants: []string{"team-a", "team-b", "team-c"},
		queryable: mockTenantQueryableWithFilter{
			queryErrByTenant: map[string]error{
				"team-b": errors.New("failure xyz"),
			},
		},
	}
)

func TestMergeQueryable_Select(t *testing.T) {
	// Set a multi tenant resolver.
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	for _, scenario := range []selectScenario{
		{
			mergeQueryableScenario: threeTenantsScenario,
			selectTestCases: []selectTestCase{
				{
					name:                "should return all series when no matchers are provided",
					expectedSeriesCount: 6,
				},
				{
					name:                "should return only series for team-a and team-c tenants when there is a not-equals matcher for the team-b tenant",
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchNotEqual}},
					expectedSeriesCount: 4,
				},
				{
					name:                "should return only series for team-b when there is an equals matcher for the team-b tenant",
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					expectedSeriesCount: 2,
				},
				{
					name:                "should return one series for each tenant when there is an equals matcher for the host1 instance",
					matchers:            []*labels.Matcher{{Name: "instance", Value: "host1", Type: labels.MatchEqual}},
					expectedSeriesCount: 3,
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithDefaultTenantIDScenario,
			selectTestCases: []selectTestCase{
				{
					name:                "should return all series when no matchers are provided",
					expectedSeriesCount: 6,
				},
				{
					name:                "should return only series for team-a and team-c tenants when there is with not-equals matcher for the team-b tenant",
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchNotEqual}},
					expectedSeriesCount: 4,
				},
				{
					name: "should return no series where there are conflicting tenant matchers",
					matchers: []*labels.Matcher{
						{Name: defaultTenantLabel, Value: "team-a", Type: labels.MatchEqual}, {Name: defaultTenantLabel, Value: "team-c", Type: labels.MatchEqual}},
					expectedSeriesCount: 0,
				},
				{
					name:                "should return only series for team-b when there is an equals matcher for team-b tenant",
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					expectedSeriesCount: 2,
				},
				{
					name:                "should return all series when there is an equals matcher for the original value of __tenant_id__ using the revised tenant label",
					matchers:            []*labels.Matcher{{Name: originalDefaultTenantLabel, Value: "original-value", Type: labels.MatchEqual}},
					expectedSeriesCount: 6,
				},
				{
					name:                "should return all series when there is a regexp matcher for the original value of __tenant_id__ using the revised tenant label",
					matchers:            []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, originalDefaultTenantLabel, "original-value")},
					expectedSeriesCount: 6,
				},
				{
					name:                "should return no series when there is a not-equals matcher for the original value of __tenant_id__ using the revised tenant label",
					matchers:            []*labels.Matcher{{Name: originalDefaultTenantLabel, Value: "original-value", Type: labels.MatchNotEqual}},
					expectedSeriesCount: 0,
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithWarningsScenario,
			selectTestCases: []selectTestCase{{
				name: "should return warnings from all tenant queryables",
				expectedWarnings: []string{
					`warning querying tenant_id team-b: don't like them`,
					`warning querying tenant_id team-c: out of office`,
				},
				expectedSeriesCount: 6,
			},
			}},
		{
			mergeQueryableScenario: threeTenantsWithErrorScenario,
			selectTestCases: []selectTestCase{{
				name:             "should return any error encountered with any tenant",
				expectedQueryErr: errors.New("error querying tenant_id team-b: failure xyz"),
			}},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			querier, err := scenario.init()
			require.NoError(t, err)

			for _, tc := range scenario.selectTestCases {
				t.Run(tc.name, func(t *testing.T) {
					seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt}, tc.matchers...)

					if tc.expectedQueryErr != nil {
						require.EqualError(t, seriesSet.Err(), tc.expectedQueryErr.Error())
					} else {
						require.NoError(t, seriesSet.Err())
						assertEqualWarnings(t, tc.expectedWarnings, seriesSet.Warnings())
					}

					count := 0
					for seriesSet.Next() {
						count++
					}
					require.Equal(t, tc.expectedSeriesCount, count)
				})
			}
		})
	}
}

func TestMergeQueryable_LabelNames(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	for _, scenario := range []labelNamesScenario{
		{
			mergeQueryableScenario: singleTenantScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:               "should not return the __tenant_id__ label as the MergeQueryable has been bypassed",
				expectedLabelNames: []string{"instance", "tenant-team-a"},
			},
		},
		{
			mergeQueryableScenario: singleTenantNoBypassScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:               "should return the __tenant_id__ label as the MergeQueryable has not been bypassed",
				expectedLabelNames: []string{defaultTenantLabel, "instance", "tenant-team-a"},
			},
		},
		{
			mergeQueryableScenario: threeTenantsScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:               "should return the __tenant_id__ label and all tenant team labels",
				expectedLabelNames: []string{defaultTenantLabel, "instance", "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithDefaultTenantIDScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:               "should return  the __tenant_id__ label and all tenant team labels, and the __original_tenant_id__ label",
				expectedLabelNames: []string{defaultTenantLabel, "instance", originalDefaultTenantLabel, "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithWarningsScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:               "should return warnings from all tenant queryables",
				expectedLabelNames: []string{defaultTenantLabel, "instance", "tenant-team-a", "tenant-team-b", "tenant-team-c"},
				expectedWarnings: []string{
					`warning querying tenant_id team-b: don't like them`,
					`warning querying tenant_id team-c: out of office`,
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithErrorScenario,
			labelNamesTestCase: labelNamesTestCase{
				name:             "should return any error encountered with any tenant",
				expectedQueryErr: errors.New("error querying tenant_id team-b: failure xyz"),
			},
		},
	} {
		t.Run(scenario.mergeQueryableScenario.name, func(t *testing.T) {
			querier, err := scenario.init()
			require.NoError(t, err)

			t.Run(scenario.labelNamesTestCase.name, func(t *testing.T) {
				labelNames, warnings, err := querier.LabelNames()
				if scenario.labelNamesTestCase.expectedQueryErr != nil {
					require.EqualError(t, err, scenario.labelNamesTestCase.expectedQueryErr.Error())
				} else {
					require.NoError(t, err)
					assert.Equal(t, scenario.labelNamesTestCase.expectedLabelNames, labelNames)
					assertEqualWarnings(t, scenario.labelNamesTestCase.expectedWarnings, warnings)
				}
			})
		})
	}
}

func TestMergeQueryable_LabelValues(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	for _, scenario := range []labelValuesScenario{
		{
			mergeQueryableScenario: singleTenantScenario,
			labelValuesTestCases: []labelValuesTestCase{
				{
					name:                "should return all label values for instance when no matchers are provided",
					labelName:           "instance",
					expectedLabelValues: []string{"host1", "host2.team-a"},
				},
				{
					name:                "should return no tenant values for the __tenant_id__ label as the MergeQueryable has been bypassed",
					labelName:           defaultTenantLabel,
					expectedLabelValues: nil,
				},
			},
		},
		{
			mergeQueryableScenario: singleTenantNoBypassScenario,
			labelValuesTestCases: []labelValuesTestCase{
				{
					name:                "should return all label values for instance when no matchers are provided",
					labelName:           "instance",
					expectedLabelValues: []string{"host1", "host2.team-a"},
				},
				{
					name:                "should return a tenant team value for the __tenant_id__ label as the MergeQueryable has not been bypassed",
					labelName:           defaultTenantLabel,
					expectedLabelValues: []string{"team-a"},
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsScenario,
			labelValuesTestCases: []labelValuesTestCase{
				{
					name:                "should return all label values for instance when no matchers are provided",
					labelName:           "instance",
					expectedLabelValues: []string{"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
				},
				{
					name: "should return no values for the instance label when there are conflicting tenant matchers",
					matchers: []*labels.Matcher{
						{Name: defaultTenantLabel, Value: "team-a", Type: labels.MatchEqual},
						{Name: defaultTenantLabel, Value: "team-c", Type: labels.MatchEqual},
					},
					labelName:           "instance",
					expectedLabelValues: []string{},
					skipReason:          matchersNotImplemented,
				},
				{
					name:                "should only query tenant-b when there is an equals matcher for team-b tenant",
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					labelName:           "instance",
					expectedLabelValues: []string{"host1", "host2.team-b"},
					expectedWarnings:    []string{"warning querying tenant_id team-b: " + mockMatchersNotImplemented},
					skipReason:          matchersNotImplemented,
				},
				{
					name:                "should return all tenant team values for the __tenant_id__ label when no matchers are provided",
					labelName:           defaultTenantLabel,
					expectedLabelValues: []string{"team-a", "team-b", "team-c"},
				},
				{
					name:                "should return only label values for team-a and team-c tenants when there is a not-equals matcher for team-b tenant",
					labelName:           defaultTenantLabel,
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchNotEqual}},
					expectedLabelValues: []string{"team-a", "team-c"},
					skipReason:          matchersNotImplemented,
				},
				{
					name:                "should return only label values for team-b tenant when there is an equals matcher for team-b tenant",
					labelName:           defaultTenantLabel,
					matchers:            []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					expectedLabelValues: []string{"team-b"},
					skipReason:          matchersNotImplemented,
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithDefaultTenantIDScenario,
			labelValuesTestCases: []labelValuesTestCase{
				{
					name:                "should return all label values for instance when no matchers are provided",
					labelName:           "instance",
					expectedLabelValues: []string{"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
				},
				{
					name:                "should return all tenant values for __tenant_id__ label name",
					labelName:           defaultTenantLabel,
					expectedLabelValues: []string{"team-a", "team-b", "team-c"},
				},
				{
					name:                "should return the original value for the revised tenant label name when no matchers are provided",
					labelName:           originalDefaultTenantLabel,
					expectedLabelValues: []string{"original-value"},
				},
				{
					name:                "should return the original value for the revised tenant label name with matchers",
					matchers:            []*labels.Matcher{{Name: originalDefaultTenantLabel, Value: "original-value", Type: labels.MatchEqual}},
					labelName:           originalDefaultTenantLabel,
					expectedLabelValues: []string{"original-value"},
					expectedWarnings: []string{
						"warning querying tenant_id team-a: " + mockMatchersNotImplemented,
						"warning querying tenant_id team-b: " + mockMatchersNotImplemented,
						"warning querying tenant_id team-c: " + mockMatchersNotImplemented,
					},
				},
			},
		},
		{
			mergeQueryableScenario: threeTenantsWithWarningsScenario,
			labelValuesTestCases: []labelValuesTestCase{{
				name:                "should return warnings from all tenant queryables",
				labelName:           "instance",
				expectedLabelValues: []string{"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
				expectedWarnings: []string{
					`warning querying tenant_id team-b: don't like them`,
					`warning querying tenant_id team-c: out of office`,
				},
			}},
		},
		{
			mergeQueryableScenario: threeTenantsWithWarningsScenario,
			labelValuesTestCases: []labelValuesTestCase{{
				name:                "should not return warnings as the underlying queryables are not queried in requests for the __tenant_id__ label",
				labelName:           defaultTenantLabel,
				expectedLabelValues: []string{"team-a", "team-b", "team-c"},
			}},
		},
		{
			mergeQueryableScenario: threeTenantsWithErrorScenario,
			labelValuesTestCases: []labelValuesTestCase{{
				name:             "should return any error encountered with any tenant",
				labelName:        "instance",
				expectedQueryErr: errors.New("error querying tenant_id team-b: failure xyz"),
			}},
		},
		{
			mergeQueryableScenario: threeTenantsWithErrorScenario,
			labelValuesTestCases: []labelValuesTestCase{{
				name:                "should not return errors as the underlying queryables are not queried in requests for the __tenant_id__ label",
				labelName:           defaultTenantLabel,
				expectedLabelValues: []string{"team-a", "team-b", "team-c"},
			}},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			querier, err := scenario.init()
			require.NoError(t, err)

			for _, tc := range scenario.labelValuesTestCases {
				t.Run(tc.name, func(t *testing.T) {
					if tc.skipReason != "" {
						t.Skip(tc.skipReason)
					}
					actLabelValues, warnings, err := querier.LabelValues(tc.labelName, tc.matchers...)
					if tc.expectedQueryErr != nil {
						require.EqualError(t, err, tc.expectedQueryErr.Error())
					} else {
						require.NoError(t, err)
						assert.Equal(t, tc.expectedLabelValues, actLabelValues, fmt.Sprintf("unexpected values for label '%s'", tc.labelName))
						assertEqualWarnings(t, tc.expectedWarnings, warnings)
					}
				})
			}
		})
	}
}

// assertEqualWarnings asserts that all the expected warning messages are present.
func assertEqualWarnings(t *testing.T, exp []string, act storage.Warnings) {
	if len(exp) == 0 && len(act) == 0 {
		return
	}
	var actStrings = make([]string, len(act))
	for pos := range act {
		actStrings[pos] = act[pos].Error()
	}
	assert.ElementsMatch(t, exp, actStrings)
}

func TestSetLabelsRetainExisting(t *testing.T) {
	for _, tc := range []struct {
		labels           labels.Labels
		additionalLabels labels.Labels
		expected         labels.Labels
	}{
		// Test adding labels at the end.
		{
			labels:           labels.Labels{{Name: "a", Value: "b"}},
			additionalLabels: labels.Labels{{Name: "c", Value: "d"}},
			expected:         labels.Labels{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
		},

		// Test adding labels at the beginning.
		{
			labels:           labels.Labels{{Name: "c", Value: "d"}},
			additionalLabels: labels.Labels{{Name: "a", Value: "b"}},
			expected:         labels.Labels{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
		},

		// Test we do override existing labels and expose the original value.
		{
			labels:           labels.Labels{{Name: "a", Value: "b"}},
			additionalLabels: labels.Labels{{Name: "a", Value: "c"}},
			expected:         labels.Labels{{Name: "a", Value: "c"}, {Name: "original_a", Value: "b"}},
		},

		// Test we do override existing labels but don't do it recursively.
		{
			labels:           labels.Labels{{Name: "a", Value: "b"}, {Name: "original_a", Value: "i am lost"}},
			additionalLabels: labels.Labels{{Name: "a", Value: "d"}},
			expected:         labels.Labels{{Name: "a", Value: "d"}, {Name: "original_a", Value: "b"}},
		},
	} {
		assert.Equal(t, tc.expected, setLabelsRetainExisting(tc.labels, tc.additionalLabels...))
	}
}

func TestTracingMergeQueryable(t *testing.T) {
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)
	ctx := user.InjectOrgID(context.Background(), "team-a|team-b")

	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())
	filter := mockTenantQueryableWithFilter{}
	q := NewQueryable(&filter, false)
	// retrieve querier if set
	querier, err := q.Querier(ctx, mint, maxt)
	require.NoError(t, err)

	seriesSet := querier.Select(true, &storage.SelectHints{Start: mint,
		End: maxt})

	require.NoError(t, seriesSet.Err())
	spans := mockTracer.FinishedSpans()
	assertSpanExist(t, spans, "mergeQuerier.Select", expectedTag{spanlogger.TenantIDTagName,
		[]string{"team-a", "team-b"}})
	assertSpanExist(t, spans, "mockTenantQuerier.select", expectedTag{spanlogger.TenantIDTagName,
		[]string{"team-a"}})
	assertSpanExist(t, spans, "mockTenantQuerier.select", expectedTag{spanlogger.TenantIDTagName,
		[]string{"team-b"}})
}

func assertSpanExist(t *testing.T,
	actualSpans []*mocktracer.MockSpan,
	name string,
	tag expectedTag) {
	for _, span := range actualSpans {
		if span.OperationName == name && containsTags(span, tag) {
			return
		}
	}
	require.FailNow(t, "can not find span matching params",
		"expected span with name `%v` and with "+
			"tags %v to be present but it was not. actual spans: %+v",
		name, tag, extractNameWithTags(actualSpans))
}

func extractNameWithTags(actualSpans []*mocktracer.MockSpan) []spanWithTags {
	result := make([]spanWithTags, len(actualSpans))
	for i, span := range actualSpans {
		result[i] = spanWithTags{span.OperationName, span.Tags()}
	}
	return result
}

func containsTags(span *mocktracer.MockSpan, expectedTag expectedTag) bool {
	return reflect.DeepEqual(span.Tag(expectedTag.key), expectedTag.values)
}

type spanWithTags struct {
	name string
	tags map[string]interface{}
}

type expectedTag struct {
	key    string
	values []string
}
