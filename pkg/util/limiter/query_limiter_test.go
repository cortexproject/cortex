package limiter

import (
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/cortexpb"
)

func TestQueryLimiter_AddSeries_ShouldReturnNoErrorOnLimitNotExceeded(t *testing.T) {
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
		limiter = NewQueryLimiter(100, 0, 0, 0)
	)
	err := limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1))
	assert.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2))
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())

	// Re-add previous series to make sure it's not double counted
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1))
	assert.NoError(t, err)
	assert.Equal(t, 2, limiter.uniqueSeriesCount())
}

func TestQueryLimiter_AddSeriers_ShouldReturnErrorOnLimitExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	var (
		series1 = []cortexpb.LabelAdapter{
			{
				Name:  labels.MetricName,
				Value: metricName + "_1",
			},
			{
				Name:  "series1",
				Value: "1",
			},
		}

		series1OtherOrderLabels = []cortexpb.LabelAdapter{
			{
				Name:  "series1",
				Value: "1",
			},
			{
				Name:  labels.MetricName,
				Value: metricName + "_1",
			},
		}

		series1FromMap = labels.FromMap(map[string]string{
			"series1":         "1",
			labels.MetricName: metricName + "_1",
		})
		series2 = labels.FromMap(map[string]string{
			labels.MetricName: metricName + "_2",
			"series2":         "1",
		})
		limiter = NewQueryLimiter(1, 0, 0, 0)
	)
	err := limiter.AddSeries(series1)
	require.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series1FromMap))
	require.NoError(t, err)
	err = limiter.AddSeries(series1OtherOrderLabels)
	require.NoError(t, err)
	err = limiter.AddSeries(cortexpb.FromLabelsToLabelAdapters(series2))
	require.Error(t, err)
}

func TestQueryLimiter_AddSeriesBatch_ShouldReturnErrorOnLimitExceeded(t *testing.T) {
	const (
		metricName = "test_metric"
	)

	limiter := NewQueryLimiter(10, 0, 0, 0)
	series := make([][]cortexpb.LabelAdapter, 0, 10)

	for i := 0; i < 10; i++ {
		s := []cortexpb.LabelAdapter{
			{
				Name:  labels.MetricName,
				Value: fmt.Sprintf("%v_%v", metricName, i),
			},
		}
		series = append(series, s)
	}
	err := limiter.AddSeries(series...)
	require.NoError(t, err)

	series1 := []cortexpb.LabelAdapter{
		{
			Name:  labels.MetricName,
			Value: metricName + "_11",
		},
	}

	err = limiter.AddSeries(series1)
	require.Error(t, err)
}

func TestQueryLimiter_AddChunkBytes(t *testing.T) {
	var limiter = NewQueryLimiter(0, 100, 0, 0)

	err := limiter.AddChunkBytes(100)
	require.NoError(t, err)
	err = limiter.AddChunkBytes(1)
	require.Error(t, err)
}

func TestQueryLimiter_AddDataBytes(t *testing.T) {
	var limiter = NewQueryLimiter(0, 0, 0, 100)

	err := limiter.AddDataBytes(100)
	require.NoError(t, err)
	err = limiter.AddDataBytes(1)
	require.Error(t, err)
}

func BenchmarkQueryLimiter_AddSeries(b *testing.B) {
	AddSeriesConcurrentBench(b, 1)
}

func BenchmarkQueryLimiter_AddSeriesBatch(b *testing.B) {
	AddSeriesConcurrentBench(b, 128)
}

func AddSeriesConcurrentBench(b *testing.B, batchSize int) {
	b.ResetTimer()
	const (
		metricName = "test_metric"
	)

	limiter := NewQueryLimiter(b.N+1, 0, 0, 0)

	// Concurrent goroutines trying to add duplicated series
	const numWorkers = 100
	var wg sync.WaitGroup

	worker := func(w int) {
		defer wg.Done()
		var series []labels.Labels
		for i := 0; i < b.N; i++ {
			series = append(series,
				labels.FromMap(map[string]string{
					labels.MetricName: metricName + "_1",
					"series1":         fmt.Sprint(i),
				}))
		}

		for i := 0; i < len(series); i += batchSize {
			s := make([][]cortexpb.LabelAdapter, 0, batchSize)
			j := i + batchSize
			if j > len(series) {
				j = len(series)
			}
			for k := i; k < j; k++ {
				s = append(s, cortexpb.FromLabelsToLabelAdapters(series[k]))
			}

			err := limiter.AddSeries(s...)
			assert.NoError(b, err)
		}
	}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w)
	}

	wg.Wait()
}
