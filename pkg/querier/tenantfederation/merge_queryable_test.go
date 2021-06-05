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
	maxt, mint                 = 0, 10
	originalDefaultTenantLabel = retainExistingPrefix + defaultTenantLabel
)

type mockTenantQueryableWithFilter struct {
	extraLabels      []string
	warningsByTenant map[string]storage.Warnings
	queryErrByTenant map[string]error
}

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

	// add extra labels
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

// At returns full series. Returned series should be iterable even after Next is called.
func (m *mockSeriesSet) At() storage.Series {
	return m.upstream.At()
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *mockSeriesSet) Err() error {
	return m.queryErr
}

// A collection of warnings for the whole set.
// Warnings could be returned even if iteration has not failed with error.
func (m *mockSeriesSet) Warnings() storage.Warnings {
	return m.warnings
}

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

func (m mockTenantQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(matchers) > 0 {
		return nil, nil, errors.New("matchers are not implemented yet")
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

func (mockTenantQuerier) Close() error {
	return nil
}

type selectorTestCase struct {
	name        string
	selector    []*labels.Matcher
	seriesCount int
}

func (c selectorTestCase) test(querier storage.Querier) func(*testing.T) {
	return func(t *testing.T) {
		seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt}, c.selector...)
		require.NoError(t, seriesSet.Err())

		count := 0
		for seriesSet.Next() {
			count++
		}

		require.Equal(t, c.seriesCount, count)
	}
}

type mergeQueryableTestCase struct {
	name                string
	tenants             []string
	expectedQuerierErr  error
	labelNames          []string
	expectedLabelValues map[string][]string
	selectorCases       []selectorTestCase

	// do not bypass in the case of single queriers
	doNotByPassSingleQuerier bool

	// storage.Warnings expected when querying
	expectedWarnings []string

	// error expected when querying
	expectedQueryErr error

	queryable mockTenantQueryableWithFilter
}

func TestMergeQueryable(t *testing.T) {
	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	for _, tc := range []mergeQueryableTestCase{
		{
			name:               "no tenant",
			expectedQuerierErr: user.ErrNoOrgID,
		},
		{
			name:       "single tenant",
			tenants:    []string{"team-a"},
			labelNames: []string{"instance", "tenant-team-a"},
			expectedLabelValues: map[string][]string{
				"instance": {"host1", "host2.team-a"},
			},
		},
		{
			name:       "single tenant without bypass",
			tenants:    []string{"team-a"},
			labelNames: []string{"__tenant_id__", "instance", "tenant-team-a"},
			expectedLabelValues: map[string][]string{
				"instance": {"host1", "host2.team-a"},
			},
			doNotByPassSingleQuerier: true,
		},
		{
			name:       "three tenants",
			tenants:    []string{"team-a", "team-b", "team-c"},
			labelNames: []string{defaultTenantLabel, "instance", "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			expectedLabelValues: map[string][]string{
				"instance": {"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
			},
			selectorCases: []selectorTestCase{
				{
					name:        "selector-all",
					seriesCount: 6,
				},
				{
					name:        "selector-tenant-label-only-b",
					selector:    []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchNotEqual}},
					seriesCount: 4,
				},
				{
					name:        "selector-tenant-label-without-b",
					selector:    []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					seriesCount: 2,
				},
				{
					name:        "selector-tenant-label-instance-value",
					selector:    []*labels.Matcher{{Name: "instance", Value: "host1", Type: labels.MatchEqual}},
					seriesCount: 3,
				},
			},
		},
		{
			name:       "three tenants and a __tenant_id__ label set",
			tenants:    []string{"team-a", "team-b", "team-c"},
			labelNames: []string{defaultTenantLabel, "instance", originalDefaultTenantLabel, "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			queryable: mockTenantQueryableWithFilter{
				extraLabels: []string{"__tenant_id__", "original-value"},
			},
			expectedLabelValues: map[string][]string{
				"instance":                 {"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
				defaultTenantLabel:         {"team-a", "team-b", "team-c"},
				originalDefaultTenantLabel: {"original-value"},
			},
			selectorCases: []selectorTestCase{
				{
					name:        "selector-all",
					seriesCount: 6,
				},
				{
					name:        "selector-tenant-label-only-b",
					selector:    []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchNotEqual}},
					seriesCount: 4,
				},
				{
					name:        "selector-tenant-label-without-b",
					selector:    []*labels.Matcher{{Name: defaultTenantLabel, Value: "team-b", Type: labels.MatchEqual}},
					seriesCount: 2,
				},
				{
					name:        "selector-tenant-label-with-original-value",
					selector:    []*labels.Matcher{{Name: originalDefaultTenantLabel, Value: "original-value", Type: labels.MatchEqual}},
					seriesCount: 6,
				},
				{
					name: "selector-regex-tenant-label-with-original-value",
					selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchRegexp, originalDefaultTenantLabel, "original-value"),
					},
					seriesCount: 6,
				},
				{
					name:        "selector-tenant-label-without-original-value",
					selector:    []*labels.Matcher{{Name: originalDefaultTenantLabel, Value: "original-value", Type: labels.MatchNotEqual}},
					seriesCount: 0,
				},
			},
		},
		{
			name:       "three tenants with some return storage warnings",
			tenants:    []string{"team-a", "team-b", "team-c"},
			labelNames: []string{defaultTenantLabel, "instance", "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			expectedLabelValues: map[string][]string{
				"instance": {"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
			},
			queryable: mockTenantQueryableWithFilter{
				warningsByTenant: map[string]storage.Warnings{
					"team-b": storage.Warnings([]error{errors.New("don't like them")}),
					"team-c": storage.Warnings([]error{errors.New("out of office")}),
				},
			},
			expectedWarnings: []string{
				`warning querying tenant_id team-b: don't like them`,
				`warning querying tenant_id team-c: out of office`,
			},
		},
		{
			name:       "three tenants with one error",
			tenants:    []string{"team-a", "team-b", "team-c"},
			labelNames: []string{defaultTenantLabel, "instance", "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			expectedLabelValues: map[string][]string{
				"instance": {"host1", "host2.team-a", "host2.team-b", "host2.team-c"},
			},
			queryable: mockTenantQueryableWithFilter{
				queryErrByTenant: map[string]error{
					"team-b": errors.New("failure xyz"),
				},
			},
			expectedQueryErr: errors.New("error querying tenant_id team-b: failure xyz"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// initialize with default tenant label
			q := NewQueryable(&tc.queryable, !tc.doNotByPassSingleQuerier)

			// inject context if set
			ctx := context.Background()
			if len(tc.tenants) > 0 {
				ctx = user.InjectOrgID(ctx, strings.Join(tc.tenants, "|"))
			}

			// retrieve querier if set
			querier, err := q.Querier(ctx, mint, maxt)
			if tc.expectedQuerierErr != nil {
				require.EqualError(t, err, tc.expectedQuerierErr.Error())
				return
			}
			require.NoError(t, err)

			// select all series and don't expect an error
			seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt})
			if tc.expectedQueryErr != nil {
				require.EqualError(t, seriesSet.Err(), tc.expectedQueryErr.Error())
			} else {
				require.NoError(t, seriesSet.Err())
				assertEqualWarnings(t, tc.expectedWarnings, seriesSet.Warnings())

				// test individual matchers
				for _, sc := range tc.selectorCases {
					t.Run(sc.name, sc.test(querier))
				}
			}

			// check label names
			labelNames, warnings, err := querier.LabelNames()
			if tc.expectedQueryErr != nil {
				require.EqualError(t, err, tc.expectedQueryErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.labelNames, labelNames)
				assertEqualWarnings(t, tc.expectedWarnings, warnings)
			}

			// check label values method
			for labelName, expectedLabelValues := range tc.expectedLabelValues {
				actLabelValues, warnings, err := querier.LabelValues(labelName)
				if tc.expectedQueryErr != nil {
					require.EqualError(t, err, tc.expectedQueryErr.Error())
				} else {
					require.NoError(t, err)
					assert.Equal(t, expectedLabelValues, actLabelValues, fmt.Sprintf("unexpected values for label '%s'", labelName))
					assertEqualWarnings(t, tc.expectedWarnings, warnings)
				}
			}
		})
	}
}

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
