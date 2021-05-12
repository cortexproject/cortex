package limiter

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
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

		limiter = NewPerQueryLimiter(100, 100)
	)
	limiter.AddFingerPrint(cortexpb.FromLabelsToLabelAdapters(series1), matchers)
	err := limiter.AddFingerPrint(cortexpb.FromLabelsToLabelAdapters(series2), matchers)
	assert.Equal(t, 2, limiter.UniqueFingerPrints())
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

		limiter = NewPerQueryLimiter(1, 1)
	)
	err := limiter.AddFingerPrint(cortexpb.FromLabelsToLabelAdapters(series1), matchers)
	assert.Equal(t, nil, err)
	err = limiter.AddFingerPrint(cortexpb.FromLabelsToLabelAdapters(series2), matchers)
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
		limiter := NewPerQueryLimiter(10000, 10000)
		for _, s := range series {
			limiter.AddFingerPrint(cortexpb.FromLabelsToLabelAdapters(s), matchers)
		}
	}

}

func TestPerQueryLimiter_AddChunkBytes(t *testing.T) {
	limiter := NewPerQueryLimiter(100, 100)
	err := limiter.AddChunkBytes(int32(10))
	assert.Equal(t, int32(10), limiter.ChunkBytesCount())
	assert.Nil(t, err)
}

func TestPerQueryLimiter_AddChunkBytesExceedLimit(t *testing.T) {
	limiter := NewPerQueryLimiter(100, 10)
	err := limiter.AddChunkBytes(int32(11))
	assert.Equal(t, int32(11), limiter.ChunkBytesCount())
	require.Error(t, err)
}
