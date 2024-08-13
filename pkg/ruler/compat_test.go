package ruler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

type fakePusher struct {
	request  *cortexpb.WriteRequest
	response *cortexpb.WriteResponse
	err      error
}

func (p *fakePusher) Push(ctx context.Context, r *cortexpb.WriteRequest) (*cortexpb.WriteResponse, error) {
	p.request = r
	return p.response, p.err
}

func TestPusherAppendable(t *testing.T) {
	pusher := &fakePusher{}
	pa := NewPusherAppendable(pusher, "user-1", nil, prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}))

	lbls1 := cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{labels.MetricName: "foo_bar"}))
	lbls2 := cortexpb.FromLabelsToLabelAdapters(labels.FromMap(map[string]string{labels.MetricName: "ALERTS", labels.AlertName: "boop"}))

	testHistogram := tsdbutil.GenerateTestHistogram(1)
	testFloatHistogram := tsdbutil.GenerateTestFloatHistogram(2)
	testHistogramWithNaN := tsdbutil.GenerateTestHistogram(1)
	testFloatHistogramWithNaN := tsdbutil.GenerateTestFloatHistogram(1)
	testHistogramWithNaN.Sum = math.Float64frombits(value.StaleNaN)
	testFloatHistogramWithNaN.Sum = math.Float64frombits(value.StaleNaN)

	for _, tc := range []struct {
		name           string
		series         string
		value          float64
		histogram      *histogram.Histogram
		floatHistogram *histogram.FloatHistogram
		expectedReq    *cortexpb.WriteRequest
	}{
		{
			name:   "tenant, normal value",
			series: "foo_bar",
			value:  1.234,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Samples: []cortexpb.Sample{
								{Value: 1.234, TimestampMs: 120_000},
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:   "tenant, stale nan value",
			series: "foo_bar",
			value:  math.Float64frombits(value.StaleNaN),
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Samples: []cortexpb.Sample{
								{Value: math.Float64frombits(value.StaleNaN), TimestampMs: 120_000},
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:   "ALERTS, normal value",
			series: `ALERTS{alertname="boop"}`,
			value:  1.234,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls2,
							Samples: []cortexpb.Sample{
								{Value: 1.234, TimestampMs: 120_000},
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:   "ALERTS, stale nan value",
			series: `ALERTS{alertname="boop"}`,
			value:  math.Float64frombits(value.StaleNaN),
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls2,
							Samples: []cortexpb.Sample{
								{Value: math.Float64frombits(value.StaleNaN), TimestampMs: 120_000},
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:      "tenant, normal histogram",
			series:    "foo_bar",
			histogram: testHistogram,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Histograms: []cortexpb.Histogram{
								cortexpb.HistogramToHistogramProto(120_000, testHistogram),
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:           "tenant, float histogram",
			series:         "foo_bar",
			floatHistogram: testFloatHistogram,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Histograms: []cortexpb.Histogram{
								cortexpb.FloatHistogramToHistogramProto(120_000, testFloatHistogram),
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:      "tenant, both sample and histogram",
			series:    "foo_bar",
			value:     1.234,
			histogram: testHistogram,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Samples: []cortexpb.Sample{
								{Value: 1.234, TimestampMs: 120_000},
							},
						},
					},
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Histograms: []cortexpb.Histogram{
								cortexpb.HistogramToHistogramProto(120_000, testHistogram),
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
		{
			name:           "tenant, both sample and float histogram",
			series:         "foo_bar",
			value:          1.234,
			floatHistogram: testFloatHistogram,
			expectedReq: &cortexpb.WriteRequest{
				Timeseries: []cortexpb.PreallocTimeseries{
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Samples: []cortexpb.Sample{
								{Value: 1.234, TimestampMs: 120_000},
							},
						},
					},
					{
						TimeSeries: &cortexpb.TimeSeries{
							Labels: lbls1,
							Histograms: []cortexpb.Histogram{
								cortexpb.FloatHistogramToHistogramProto(120_000, testFloatHistogram),
							},
						},
					},
				},
				Source: cortexpb.RULE,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			lbls, err := parser.ParseMetric(tc.series)
			require.NoError(t, err)

			pusher.response = &cortexpb.WriteResponse{}
			a := pa.Appender(ctx)
			// We don't ingest sample if value is set to 0 for testing purpose.
			if tc.value != 0 {
				_, err = a.Append(0, lbls, 120_000, tc.value)
				require.NoError(t, err)
			}

			if tc.histogram != nil {
				_, err = a.AppendHistogram(0, lbls, 120_000, tc.histogram, nil)
			} else if tc.floatHistogram != nil {
				_, err = a.AppendHistogram(0, lbls, 120_000, nil, tc.floatHistogram)
			}
			require.NoError(t, err)

			require.NoError(t, a.Commit())
			require.Equal(t, tc.expectedReq.String(), pusher.request.String())
		})
	}
}

func TestPusherErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		returnedError    error
		expectedWrites   int
		expectedFailures int
	}{
		"no error": {
			expectedWrites:   1,
			expectedFailures: 0,
		},

		"400 error": {
			returnedError:    httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedWrites:   1,
			expectedFailures: 0, // 400 errors not reported as failures.
		},

		"500 error": {
			returnedError:    httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedWrites:   1,
			expectedFailures: 1, // 500 errors are failures
		},

		"unknown error": {
			returnedError:    errors.New("test error"),
			expectedWrites:   1,
			expectedFailures: 1, // unknown errors are not 400, so they are reported.
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			pusher := &fakePusher{err: tc.returnedError, response: &cortexpb.WriteResponse{}}

			writes := prometheus.NewCounter(prometheus.CounterOpts{})
			failures := prometheus.NewCounter(prometheus.CounterOpts{})

			pa := NewPusherAppendable(pusher, "user-1", ruleLimits{}, writes, failures)

			lbls, err := parser.ParseMetric("foo_bar")
			require.NoError(t, err)

			a := pa.Appender(ctx)
			_, err = a.Append(0, lbls, int64(model.Now()), 123456)
			require.NoError(t, err)

			_, err = a.AppendHistogram(0, lbls, int64(model.Now()), tsdbutil.GenerateTestHistogram(1), nil)
			require.NoError(t, err)
			_, err = a.AppendHistogram(0, lbls, int64(model.Now()), nil, tsdbutil.GenerateTestFloatHistogram(2))
			require.NoError(t, err)

			require.Equal(t, tc.returnedError, a.Commit())

			require.Equal(t, tc.expectedWrites, int(testutil.ToFloat64(writes)))
			require.Equal(t, tc.expectedFailures, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestMetricsQueryFuncErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		returnedError          error
		expectedQueries        int
		expectedFailedQueries  int
		notWrapQueryableErrors bool
	}{
		"no error": {
			expectedQueries:       1,
			expectedFailedQueries: 0,
		},

		"400 error": {
			returnedError:         httpgrpc.Errorf(http.StatusBadRequest, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // 400 errors not reported as failures.
		},

		"500 error": {
			returnedError:         httpgrpc.Errorf(http.StatusInternalServerError, "test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // 500 errors are failures
		},

		"promql.ErrStorage": {
			returnedError:         promql.ErrStorage{Err: errors.New("test error")},
			expectedQueries:       1,
			expectedFailedQueries: 1,
		},

		"promql.ErrQueryCanceled": {
			returnedError:         promql.ErrQueryCanceled("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrQueryTimeout": {
			returnedError:         promql.ErrQueryTimeout("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"promql.ErrTooManySamples": {
			returnedError:         promql.ErrTooManySamples("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 0, // Not interesting.
		},

		"unknown error": {
			returnedError:         errors.New("test error"),
			expectedQueries:       1,
			expectedFailedQueries: 1, // unknown errors are not 400, so they are reported.
		},

		"max query length validation error": {
			returnedError:          validation.LimitError(fmt.Sprintf(validation.ErrQueryTooLong, "10000", "1000")),
			expectedQueries:        1,
			expectedFailedQueries:  0,
			notWrapQueryableErrors: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			queries := prometheus.NewCounter(prometheus.CounterOpts{})
			failures := prometheus.NewCounter(prometheus.CounterOpts{})

			mockFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
				err := tc.returnedError
				if !tc.notWrapQueryableErrors {
					err = WrapQueryableErrors(err)
				}
				return promql.Vector{}, err
			}
			qf := MetricsQueryFunc(mockFunc, queries, failures)

			_, err := qf(context.Background(), "test", time.Now())
			require.Equal(t, tc.returnedError, err)

			require.Equal(t, tc.expectedQueries, int(testutil.ToFloat64(queries)))
			require.Equal(t, tc.expectedFailedQueries, int(testutil.ToFloat64(failures)))
		})
	}
}

func TestRecordAndReportRuleQueryMetrics(t *testing.T) {
	queryTime := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"user"})

	mockFunc := func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
		time.Sleep(1 * time.Second)
		return promql.Vector{}, nil
	}
	qf := RecordAndReportRuleQueryMetrics(mockFunc, queryTime.WithLabelValues("userID"), log.NewNopLogger())
	_, _ = qf(context.Background(), "test", time.Now())

	require.GreaterOrEqual(t, testutil.ToFloat64(queryTime.WithLabelValues("userID")), float64(1))
}
