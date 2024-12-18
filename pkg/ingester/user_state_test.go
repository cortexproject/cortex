package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestMetricCounter(t *testing.T) {
	const metric = "metric"

	for name, tc := range map[string]struct {
		ignored                   []string
		localLimit                int
		series                    int
		expectedErrorOnLastSeries error
	}{
		"no ignored metrics, limit not reached": {
			ignored:                   nil,
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"no ignored metrics, limit reached": {
			ignored:                   nil,
			localLimit:                5,
			series:                    6,
			expectedErrorOnLastSeries: errMaxSeriesPerMetricLimitExceeded,
		},

		"ignored metric, limit not reached": {
			ignored:                   []string{metric},
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"ignored metric, over limit": {
			ignored:                   []string{metric},
			localLimit:                5,
			series:                    10,
			expectedErrorOnLastSeries: nil, // this metric is not checked for series-count.
		},

		"ignored different metric, limit not reached": {
			ignored:                   []string{"another_metric1", "another_metric2"},
			localLimit:                5,
			series:                    5,
			expectedErrorOnLastSeries: nil,
		},

		"ignored different metric, over limit": {
			ignored:                   []string{"another_metric1", "another_metric2"},
			localLimit:                5,
			series:                    6,
			expectedErrorOnLastSeries: errMaxSeriesPerMetricLimitExceeded,
		},
	} {
		t.Run(name, func(t *testing.T) {
			var ignored map[string]struct{}
			if tc.ignored != nil {
				ignored = make(map[string]struct{})
				for _, v := range tc.ignored {
					ignored[v] = struct{}{}
				}
			}

			limits := validation.Limits{MaxLocalSeriesPerMetric: tc.localLimit}

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// We're testing code that's not dependent on sharding strategy, replication factor, etc. To simplify the test,
			// we use local limit only.
			limiter := NewLimiter(overrides, nil, util.ShardingStrategyDefault, true, 3, false, "")
			mc := newMetricCounter(limiter, ignored)

			for i := 0; i < tc.series; i++ {
				err := mc.canAddSeriesFor("user", metric)
				if i < tc.series-1 {
					assert.NoError(t, err)
					mc.increaseSeriesForMetric(metric)
				} else {
					assert.Equal(t, tc.expectedErrorOnLastSeries, err)
				}
			}
		})
	}
}

func TestGetCardinalityForLimitsPerLabelSet(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("err")
	for _, tc := range []struct {
		name      string
		ir        tsdb.IndexReader
		limits    []validation.LimitsPerLabelSet
		idx       int
		cnt       int
		numSeries uint64
		err       error
	}{
		{
			name: "single label",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("foo", "bar")},
			},
			cnt: 5,
		},
		{
			name: "multiple limits",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"job": {"test": index.NewListPostings([]storage.SeriesRef{5, 6, 7, 8})},
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("job", "test")},
				{LabelSet: labels.FromStrings("foo", "bar")},
			},
			idx: 1,
			cnt: 5,
		},
		{
			name: "2 labels intersect",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
				"job": {"prometheus": index.NewListPostings([]storage.SeriesRef{1, 3, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("foo", "bar", "job", "prometheus")},
			},
			cnt: 3,
		},
		{
			name: "error",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.ErrPostings(testErr)},
				"job": {"prometheus": index.NewListPostings([]storage.SeriesRef{1, 3, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("foo", "bar", "job", "prometheus")},
			},
			err: testErr,
		},
		{
			name: "no match and no default partition",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"job": {"test": index.NewListPostings([]storage.SeriesRef{5, 6, 7, 8})},
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("foo", "baz")},
			},
			idx: 0,
			cnt: 0,
		},
		{
			name: "no match",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"job": {"test": index.NewListPostings([]storage.SeriesRef{5, 6, 7, 8})},
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{LabelSet: labels.FromStrings("foo", "baz")},
			},
			cnt: 0,
		},
		{
			// Total cardinality 12. Cardinality of existing label set limits 8.
			// Default partition cardinality 4.
			name: "default partition",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"job": {"test": index.NewListPostings([]storage.SeriesRef{3, 4, 5, 6, 7, 8})},
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			limits: []validation.LimitsPerLabelSet{
				{Hash: 1, LabelSet: labels.FromStrings("foo", "bar")},
				{Hash: 2, LabelSet: labels.FromStrings("job", "test")},
				{}, // Default partition.
			},
			idx:       2,
			cnt:       4,
			numSeries: 12,
		},
		{
			name: "default partition with error getting postings",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"job": {"test": index.NewListPostings([]storage.SeriesRef{5, 6, 7, 8})},
				"foo": {"bar": index.ErrPostings(testErr)},
			}},
			limits: []validation.LimitsPerLabelSet{
				{Hash: 1, LabelSet: labels.FromStrings("foo", "bar")},
				{Hash: 2, LabelSet: labels.FromStrings("job", "test")},
				{}, // Default partition.
			},
			idx: 2,
			err: testErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cnt, err := getCardinalityForLimitsPerLabelSet(ctx, tc.numSeries, tc.ir, tc.limits, tc.limits[tc.idx])
			if err != nil {
				require.EqualError(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.cnt, cnt)
			}
		})
	}
}

func TestGetPostingForLabels(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("err")
	for _, tc := range []struct {
		name     string
		ir       tsdb.IndexReader
		lbls     labels.Labels
		postings []storage.SeriesRef
		err      error
	}{
		{
			name: "single label",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
			}},
			lbls:     labels.FromStrings("foo", "bar"),
			postings: []storage.SeriesRef{1, 2, 3, 4, 5},
		},
		{
			name: "intersect",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5})},
				"job": {"prometheus": index.NewListPostings([]storage.SeriesRef{1, 3, 5})},
			}},
			lbls:     labels.FromStrings("foo", "bar", "job", "prometheus"),
			postings: []storage.SeriesRef{1, 3, 5},
		},
		{
			name: "error",
			ir: &mockIndexReader{postings: map[string]map[string]index.Postings{
				"foo": {"bar": index.ErrPostings(testErr)},
				"job": {"prometheus": index.NewListPostings([]storage.SeriesRef{1, 3, 5})},
			}},
			lbls:     labels.FromStrings("foo", "bar", "job", "prometheus"),
			postings: []storage.SeriesRef{1, 3, 5},
			err:      testErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := getPostingForLabels(ctx, tc.ir, tc.lbls)
			if err != nil {
				require.EqualError(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
				series := make([]storage.SeriesRef, 0)
				for p.Next() {
					series = append(series, p.At())
				}
				if tc.err != nil {
					require.EqualError(t, p.Err(), tc.err.Error())
				} else {
					require.Equal(t, tc.postings, series)
				}
			}
		})
	}
}

func TestGetPostingCardinality(t *testing.T) {
	testErr := errors.New("err")
	for _, tc := range []struct {
		name        string
		p           index.Postings
		cardinality int
		err         error
	}{
		{
			name: "empty",
			p:    index.EmptyPostings(),
		},
		{
			name: "err",
			p:    index.ErrPostings(testErr),
			err:  testErr,
		},
		{
			name:        "cardinality",
			p:           index.NewListPostings([]storage.SeriesRef{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
			cardinality: 10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cnt, err := getPostingCardinality(tc.p)
			if tc.err != nil {
				require.EqualError(t, err, tc.err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.cardinality, cnt)
			}
		})
	}
}

type mockIndexReader struct {
	postings map[string]map[string]index.Postings
}

func (ir *mockIndexReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	if entries, ok := ir.postings[name]; ok {
		if postings, ok2 := entries[values[0]]; ok2 {
			return postings, nil
		}
	}
	return index.EmptyPostings(), nil
}

func (ir *mockIndexReader) Symbols() index.StringIter { return nil }

func (ir *mockIndexReader) SortedLabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir *mockIndexReader) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir *mockIndexReader) PostingsForLabelMatching(ctx context.Context, name string, match func(value string) bool) index.Postings {
	return nil
}

func (ir *mockIndexReader) SortedPostings(index.Postings) index.Postings { return nil }

func (ir *mockIndexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	return nil
}

func (ir *mockIndexReader) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	return nil
}

func (ir *mockIndexReader) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, error) {
	return nil, nil
}

func (ir *mockIndexReader) LabelValueFor(ctx context.Context, id storage.SeriesRef, label string) (string, error) {
	return "", nil
}

func (ir *mockIndexReader) LabelNamesFor(ctx context.Context, postings index.Postings) ([]string, error) {
	return nil, nil
}

func (ir *mockIndexReader) Close() error { return nil }
