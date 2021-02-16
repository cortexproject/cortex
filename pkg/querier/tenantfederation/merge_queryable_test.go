package tenantfederation

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/querier/series"
	"github.com/cortexproject/cortex/pkg/tenant"
)

const (
	maxt, mint = 0, 10
)

type mockTenantQueryableWithFilter struct {
	extraLabels []string
}

func (m *mockTenantQueryableWithFilter) Querier(ctx context.Context, _, _ int64) (storage.Querier, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	return mockTenantQuerier{tenant: tenantIDs[0], extraLabels: m.extraLabels}, nil
}

func (m *mockTenantQueryableWithFilter) UseQueryable(_ time.Time, _, _ int64) bool {
	return true
}

type mockTenantQuerier struct {
	tenant      string
	extraLabels []string
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

func (m mockTenantQuerier) Select(_ bool, sp *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	var matrix model.Matrix

	for _, s := range m.matrix() {
		if metricMatches(s.Metric, matchers) {
			matrix = append(matrix, s)
		}
	}

	return series.MatrixToSeriesSet(matrix)
}

func (m mockTenantQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	if len(matchers) > 0 {
		return nil, nil, errors.New("matchers are not implemented yet")
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
	return results, nil, nil
}

func (m mockTenantQuerier) LabelNames() ([]string, storage.Warnings, error) {
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
	return results, nil, nil
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
	expectedErr         error
	extraLabels         []string
	labelNames          []string
	expectedLabelValues map[string][]string
	selectorCases       []selectorTestCase
}

func TestMergeQueryable(t *testing.T) {
	upstreamQueryable := &mockTenantQueryableWithFilter{}

	// set a multi tenant resolver
	tenant.WithDefaultResolver(tenant.NewMultiResolver())

	for _, tc := range []mergeQueryableTestCase{
		{
			name:        "no tenant",
			expectedErr: user.ErrNoOrgID,
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
			name:        "three tenants and a __tenant_id__ label set",
			tenants:     []string{"team-a", "team-b", "team-c"},
			labelNames:  []string{defaultTenantLabel, "instance", originalDefaultTenantLabel, "tenant-team-a", "tenant-team-b", "tenant-team-c"},
			extraLabels: []string{"__tenant_id__", "original-value"},
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstreamQueryable.extraLabels = tc.extraLabels

			// initialize with default tenant label
			q := mergeQueryable{
				upstream: upstreamQueryable,
			}

			// inject context if set
			ctx := context.Background()
			if len(tc.tenants) > 0 {
				ctx = user.InjectOrgID(ctx, strings.Join(tc.tenants, "|"))
			}

			// retrieve querier if set
			querier, err := q.Querier(ctx, mint, maxt)
			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
				return
			}
			require.NoError(t, err)

			// select all series and don't expect an error
			seriesSet := querier.Select(true, &storage.SelectHints{Start: mint, End: maxt})
			require.NoError(t, seriesSet.Err())

			// test individual matchers
			for _, sc := range tc.selectorCases {
				t.Run(sc.name, sc.test(querier))
			}

			labelNames, _, err := querier.LabelNames()
			require.NoError(t, err)
			assert.Equal(t, tc.labelNames, labelNames)

			// check label values
			for labelName, expectedLabelValues := range tc.expectedLabelValues {
				actLabelValues, _, err := querier.LabelValues(labelName)
				require.NoError(t, err)
				assert.Equal(t, expectedLabelValues, actLabelValues, fmt.Sprintf("unexpected values for label '%s'", labelName))
			}
		})
	}
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
