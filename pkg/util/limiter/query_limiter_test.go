package limiter

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestPerQueryLimiter_AddFingerPrint(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		matchers = []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
		}

		limiter = NewQueryLimiter(100)
	)
	limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1), matchers)
	err := limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2), matchers)
	assert.Equal(t, 2, limiter.UniqueSeries())
	assert.Nil(t, err)

}

func TestPerQueryLimiter_AddFingerPrintExceedLimit(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_1",
			"series1":         "1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		matchers = []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
		}

		limiter = NewQueryLimiter(1)
	)
	err := limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1), matchers)
	assert.Equal(t, nil, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2), matchers)
	require.Error(t, err)
}

func BenchmarkPerQueryLimiter_AddFingerPrint(b *testing.B) {
	const (
		metricName = "test_metric"
	)
	var series []labels.Labels
	for i := 0; i < 10000; i++ {
		series = append(series,
			labels.FromMap(map[string]string{
				labels.MetricName: metricName + "_1",
				"series1":         fmt.Sprint(i),
			}))
	}
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName),
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		limiter := NewQueryLimiter(10000)
		for _, s := range series {
			limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(s), matchers)
		}
	}

}
