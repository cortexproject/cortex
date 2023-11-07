package querier

import (
	"context"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/mock"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type MockDistributor struct {
	mock.Mock
}

func (m *MockDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(model.Matrix), args.Error(1)
}
func (m *MockDistributor) QueryExemplars(ctx context.Context, from, to model.Time, matchers ...[]*labels.Matcher) (*client.ExemplarQueryResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.ExemplarQueryResponse), args.Error(1)
}
func (m *MockDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).(*client.QueryStreamResponse), args.Error(1)
}
func (m *MockDistributor) LabelValuesForLabelName(ctx context.Context, from, to model.Time, lbl model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelValuesForLabelNameStream(ctx context.Context, from, to model.Time, lbl model.LabelName, matchers ...*labels.Matcher) ([]string, error) {
	args := m.Called(ctx, from, to, lbl, matchers)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelNames(ctx context.Context, from, to model.Time) ([]string, error) {
	args := m.Called(ctx, from, to)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) LabelNamesStream(ctx context.Context, from, to model.Time) ([]string, error) {
	args := m.Called(ctx, from, to)
	return args.Get(0).([]string), args.Error(1)
}
func (m *MockDistributor) MetricsForLabelMatchers(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]metric.Metric), args.Error(1)
}
func (m *MockDistributor) MetricsForLabelMatchersStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	args := m.Called(ctx, from, to, matchers)
	return args.Get(0).([]metric.Metric), args.Error(1)
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
	s := make([][]cortexpb.LabelAdapter, 0, len(m.response.Chunkseries)+len(m.response.Timeseries))

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

	for _, series := range m.response.Timeseries {
		for _, label := range series.Labels {
			for _, matcher := range matchers {
				if matcher.Matches(label.Value) {
					s = append(s, series.Labels)
					response.Timeseries = append(response.Timeseries, series)
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
