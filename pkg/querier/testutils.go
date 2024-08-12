package querier

import (
	"context"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type MockDistributor struct {
	mock.Mock
}

func (m *MockDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.ExemplarQueryResponse), args.Error(1)
}
func (m *MockDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.QueryStreamResponse), args.Error(1)
}
func (m *MockDistributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, lbl model.LabelName, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, hints, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelValuesForLabelNameStream(ctx context.Context, from, to model.Time, lbl model.LabelName, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, hints, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelNames(ctx context.Context, from, to model.Time, hints *storage.LabelHints) ([]string, error) {
	args := m.Called(ctx, from, to, hints)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelNamesStream(ctx context.Context, from, to model.Time, hints *storage.LabelHints) ([]string, error) {
	args := m.Called(ctx, from, to, hints)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) MetricsForLabelMatchers(ctx context.Context, from, to model.Time, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]model.Metric, error) {
	args := m.Called(ctx, from, to, hints, matchers)
	return args.Get(0).([]model.Metric), args.Error(1)
}
func (m *MockDistributor) MetricsForLabelMatchersStream(ctx context.Context, from, to model.Time, hints *storage.SelectHints, matchers ...*labels.Matcher) ([]model.Metric, error) {
	args := m.Called(ctx, from, to, hints, matchers)
	return args.Get(0).([]model.Metric), args.Error(1)
}

func (m *MockDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	args := m.Called(ctx)
	return args.Get(0).([]scrape.MetricMetadata), args.Error(1)
}

type MockLimitingDistributor struct {
	MockDistributor
	response *client.QueryStreamResponse
}

func (m *MockLimitingDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	var (
		queryLimiter = limiter.QueryLimiterFromContextWithFallback(ctx)
	)
	s := make([][]cortexpb.LabelAdapter, 0, len(m.response.Chunkseries))

	response := &client.QueryStreamResponse{}
	for _, series := range m.response.Chunkseries {
		for _, label := range series.Labels {
			for _, matcher := range matchers {
				if matcher.Matches(label.Value) {
					s = append(s, series.Labels)
					response.Chunkseries = append(response.Chunkseries, series)
				}
			}
		}
	}

	if limitErr := queryLimiter.AddSeries(s...); limitErr != nil {
		return nil, validation.LimitError(limitErr.Error())
	}
	return response, nil
}

type TestConfig struct {
	Cfg         Config
	Distributor Distributor
	Stores      []QueryableWithFilter
}

func DefaultQuerierConfig() Config {
	querierCfg := Config{}
	flagext.DefaultValues(&querierCfg)
	return querierCfg
}

func DefaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func ConvertToChunks(t *testing.T, samples []cortexpb.Sample, histograms []*cortexpb.Histogram) []client.Chunk {
	// We need to make sure that there is at least one chunk present,
	// else no series will be selected.
	var chk chunkenc.Chunk
	if len(samples) > 0 {
		chk = chunkenc.NewXORChunk()
		appender, err := chk.Appender()
		require.NoError(t, err)

		for _, s := range samples {
			appender.Append(s.TimestampMs, s.Value)
		}
	} else if len(histograms) > 0 {
		if histograms[0].IsFloatHistogram() {
			chk = chunkenc.NewFloatHistogramChunk()
			appender, err := chk.Appender()
			require.NoError(t, err)
			for _, h := range histograms {
				_, _, _, err = appender.AppendFloatHistogram(nil, h.TimestampMs, cortexpb.FloatHistogramProtoToFloatHistogram(*h), true)
			}
			require.NoError(t, err)
		} else {
			chk = chunkenc.NewHistogramChunk()
			appender, err := chk.Appender()
			require.NoError(t, err)
			for _, h := range histograms {
				_, _, _, err = appender.AppendHistogram(nil, h.TimestampMs, cortexpb.HistogramProtoToHistogram(*h), true)
			}
			require.NoError(t, err)
		}
	}

	c := chunk.NewChunk(nil, chk, model.Time(samples[0].TimestampMs), model.Time(samples[len(samples)-1].TimestampMs))
	clientChunks, err := chunkcompat.ToChunks([]chunk.Chunk{c})
	require.NoError(t, err)

	return clientChunks
}
