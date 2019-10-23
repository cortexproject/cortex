package querysharding

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

var (
	start  = time.Unix(1000, 0)
	end    = start.Add(3 * time.Minute)
	step   = 30 * time.Second
	ctx    = context.Background()
	engine = promql.NewEngine(promql.EngineOpts{
		Reg:                prometheus.DefaultRegisterer,
		MaxConcurrent:      100,
		Logger:             util.Logger,
		Timeout:            1 * time.Hour,
		MaxSamples:         10e6,
		ActiveQueryTracker: nil,
	})
)

// This test allows to verify which PromQL expressions can be parallelized.
func Test_PromQL(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		normalQuery string
		shardQuery  string
		shouldEqual bool
	}{
		// Vector can be parallelized but we need to remove the cortex shard label.
		// It should be noted that the __cortex_shard__ label is required by the engine
		// and therefore should be returned by the storage.
		// Range vectors `bar1{baz="blip"}[1m]` are not tested here because it is not supported
		// by range queries.
		{
			`bar1{baz="blip"}`,
			`label_replace(
				bar1{__cortex_shard__="0_of_3",baz="blip"} or
				bar1{__cortex_shard__="1_of_3",baz="blip"} or
				bar1{__cortex_shard__="2_of_3",baz="blip"},
				"__cortex_shard__","","",""
			)`,
			true,
		},
		// __cortex_shard__ label is required otherwise the or will keep only the first series.
		{
			`sum(bar1{baz="blip"})`,
			`sum(
				sum (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		{
			`sum(bar1{baz="blip"})`,
			`sum(
				sum by(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`sum by (foo) (bar1{baz="blip"})`,
			`sum by (foo) (
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(foo,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		// __cortex_shard__ needs to be removed but after the or operation
		{
			`sum without (foo,bar) (bar1{baz="blip"})`,
			`sum without (foo,bar,__cortex_shard__)(
				sum without(foo,bar) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum without(foo,bar) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum without(foo,bar) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`sum by (foo,bar) (bar1{baz="blip"})`,
			`sum by (foo,bar)(
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`sum without (foo,bar) (bar1{baz="blip"})`,
			`sum without (foo,bar)(
				sum without(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				sum without(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				sum without(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`min by (foo,bar) (bar1{baz="blip"})`,
			`min by (foo,bar)(
				min by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				min by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				min by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		{
			`max by (foo,bar) (bar1{baz="blip"})`,
			` max by (foo,bar)(
				max by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				max by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				max by(foo,bar,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			true,
		},
		// avg generally cant be parallelized
		{
			`avg(bar1{baz="blip"})`,
			`avg(
				avg by(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				avg by(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				avg by(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		// stddev can't be parallelized.
		{
			`stddev(bar1{baz="blip"})`,
			` stddev(
				stddev by(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				stddev by(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				stddev by(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		// stdvar can't be parallelized.
		{
			`stdvar(bar1{baz="blip"})`,
			`stdvar(
				stdvar by(__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				stdvar by(__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				stdvar by(__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			  )`,
			false,
		},
		{
			`count(bar1{baz="blip"})`,
			`count(
				count without (__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
				)`,
			true,
		},
		{
			`count by (foo,bar) (bar1{baz="blip"})`,
			`count by (foo,bar) (
				count by (foo,bar,__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				count by (foo,bar,__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				count by (foo,bar,__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			)`,
			true,
		},
		// different ways to represent count without.
		{
			`count without (foo) (bar1{baz="blip"})`,
			`count without (foo) (
				count without (__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			)`,
			true,
		},
		{
			`count without (foo) (bar1{baz="blip"})`,
			`sum without (__cortex_shard__) (
				count without (foo) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				count without (foo) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				count without (foo) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			)`,
			true,
		},
		{
			`count without (foo, bar) (bar1{baz="blip"})`,
			`count without (foo, bar) (
				count without (__cortex_shard__) (bar1{__cortex_shard__="0_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="1_of_3",baz="blip"}) or
				count without (__cortex_shard__) (bar1{__cortex_shard__="2_of_3",baz="blip"})
			)`,
			true,
		},
		{
			`topk(2,bar1{baz="blip"})`,
			`label_replace(
				topk(2,
					topk(2,(bar1{__cortex_shard__="0_of_3",baz="blip"})) without(__cortex_shard__) or
					topk(2,(bar1{__cortex_shard__="1_of_3",baz="blip"})) without(__cortex_shard__) or
					topk(2,(bar1{__cortex_shard__="2_of_3",baz="blip"})) without(__cortex_shard__)
				),
                          "__cortex_shard__","","","")`,
			true,
		},
		{
			`bottomk(2,bar1{baz="blip"})`,
			`label_replace(
				bottomk(2,
					bottomk(2,(bar1{__cortex_shard__="0_of_3",baz="blip"})) without(__cortex_shard__) or
					bottomk(2,(bar1{__cortex_shard__="1_of_3",baz="blip"})) without(__cortex_shard__) or
					bottomk(2,(bar1{__cortex_shard__="2_of_3",baz="blip"})) without(__cortex_shard__)
				),
                          "__cortex_shard__","","","")`,
			true,
		},
		// {
		// 	`sum by (foo,bar) (rate(bar1{baz="blip"}[1m]))`,
		// 	` sum by (foo,bar)(
		// 		sum by(foo,bar,__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (rate(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
		// 	  )`,
		// 	true,
		// },
		// {
		// 	`sum by (foo,bar) (count_over_time(bar1{baz="blip"}[1m]))`,
		// 	` sum by (foo,bar)(
		// 		sum by(foo,bar,__cortex_shard__) (count_over_time(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (count_over_time(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (count_over_time(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
		// 	  )`,
		// 	true,
		// },
		// {
		// 	`sum by (foo,bar) (avg_over_time(bar1{baz="blip"}[1m]))`,
		// 	` sum by (foo,bar)(
		// 		sum by(foo,bar,__cortex_shard__) (avg_over_time(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (avg_over_time(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (avg_over_time(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
		// 	  )`,
		// 	true,
		// },
		// {
		// 	`sum by (foo,bar) (min_over_time(bar1{baz="blip"}[1m]))`,
		// 	` sum by (foo,bar)(
		// 		sum by(foo,bar,__cortex_shard__) (min_over_time(bar1{__cortex_shard__="0_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (min_over_time(bar1{__cortex_shard__="1_of_3",baz="blip"}[1m])) or
		// 		sum by(foo,bar,__cortex_shard__) (min_over_time(bar1{__cortex_shard__="2_of_3",baz="blip"}[1m]))
		// 	  )`,
		// 	true,
		// },
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.normalQuery, func(t *testing.T) {
			// t.Parallel()

			baseQuery, err := engine.NewRangeQuery(shardAwareQueryable, tt.normalQuery, start, end, step)
			require.Nil(t, err)
			shardQuery, err := engine.NewRangeQuery(shardAwareQueryable, tt.shardQuery, start, end, step)
			require.Nil(t, err)
			baseResult := baseQuery.Exec(ctx)
			shardResult := shardQuery.Exec(ctx)
			t.Logf("base: %v\n", baseResult)
			t.Logf("shard: %v\n", shardResult)
			if tt.shouldEqual {
				require.Equal(t, baseResult, shardResult)
				return
			}
			require.NotEqual(t, baseResult, shardResult)
		})
	}

}

var shardAwareQueryable = storage.QueryableFunc(func(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &matrix{
		series: []*promql.StorageSeries{
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "barr"}}, factor(5)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "bazz"}}, factor(7)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "buzz"}}, factor(12)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "bozz"}}, factor(11)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blop"}, {"foo", "buzz"}}, factor(8)),
			newSeries(labels.Labels{{"__name__", "bar1"}, {"baz", "blip"}, {"bar", "blap"}, {"foo", "bazz"}}, identity),
		},
	}, nil
})

type matrix struct {
	series []*promql.StorageSeries
}

func (m matrix) Next() bool { return len(m.series) != 0 }

func (m *matrix) At() storage.Series {
	res := m.series[0]
	m.series = m.series[1:]
	return res
}

func (m *matrix) Err() error { return nil }

func (m *matrix) Select(selectParams *storage.SelectParams, matchers ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	shardIndex := -1
	shardTotal := 0
	var err error
	for _, matcher := range matchers {
		if matcher.Name != "__cortex_shard__" {
			continue
		}
		shardData := strings.Split(matcher.Value, "_")
		shardIndex, err = strconv.Atoi(shardData[0])
		if err != nil {
			panic(err)
		}
		shardTotal, err = strconv.Atoi(shardData[2])
		if err != nil {
			panic(err)
		}
	}
	if shardIndex >= 0 {
		shard := splitByShard(shardIndex, shardTotal, m)
		return shard, nil, nil
	}
	return m, nil, nil
}

func (m *matrix) LabelValues(name string) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}
func (m *matrix) LabelNames() ([]string, storage.Warnings, error) { return nil, nil, nil }
func (m *matrix) Close() error                                    { return nil }

func newSeries(metric labels.Labels, generator func(float64) float64) *promql.StorageSeries {
	sort.Sort(metric)
	var points []promql.Point

	for ts := start.Add(-step); ts.Unix() <= end.Unix(); ts = ts.Add(step) {
		t := ts.Unix() * 1e3
		points = append(points, promql.Point{
			T: t,
			V: generator(float64(t)),
		})
	}

	return promql.NewStorageSeries(promql.Series{
		Metric: metric,
		Points: points,
	})
}

func identity(t float64) float64 {
	return float64(t)
}

func factor(f float64) func(float64) float64 {
	i := 0.
	return func(float64) float64 {
		i++
		res := i * f
		return res
	}
}

// var identity(t int64) float64 {
// 	return float64(t)
// }

// splitByShard returns the shard subset of a matrix.
// e.g if a matrix has 6 series, and we want 3 shard, then each shard will contain
// 2 series.
func splitByShard(shardIndex, shardTotal int, matrixes *matrix) *matrix {
	res := &matrix{}
	for i, s := range matrixes.series {
		if i%shardTotal != shardIndex {
			continue
		}
		var points []promql.Point
		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			points = append(points, promql.Point{
				T: t,
				V: v,
			})

		}
		lbs := s.Labels().Copy()
		lbs = append(lbs, labels.Label{Name: "__cortex_shard__", Value: fmt.Sprintf("%d_of_%d", shardIndex, shardTotal)})
		res.series = append(res.series, promql.NewStorageSeries(promql.Series{
			Metric: lbs,
			Points: points,
		}))
	}
	return res
}
