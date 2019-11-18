package ingester

import (
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
)

func TestIngester_v2Push(t *testing.T) {
	metricLabelAdapters := []client.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := client.FromLabelAdaptersToLabels(metricLabelAdapters)

	tests := map[string]struct {
		reqs             []*client.WriteRequest
		expectedErr      error
		expectedIngested []client.TimeSeries
		expectedMetrics  string
	}{
		"should succeed on valid series": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 9}},
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 10}},
					client.API),
			},
			expectedErr: nil,
			expectedIngested: []client.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []client.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
			`,
		},
		"should soft fail on sample out of order": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 10}},
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 9}},
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, tsdb.ErrOutOfOrderSample.Error()),
			expectedIngested: []client.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []client.Sample{{Value: 2, TimestampMs: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
			`,
		},
		"should soft fail on sample out of bound": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 1575043969}},
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 1575043969 - (86400 * 1000)}},
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, tsdb.ErrOutOfBounds.Error()),
			expectedIngested: []client.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []client.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 1575043969}},
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 1575043969}},
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, tsdb.ErrAmendSample.Error()),
			expectedIngested: []client.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []client.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), registry)
			require.NoError(t, err)
			defer i.Shutdown()
			defer cleanup()

			ctx := user.InjectOrgID(context.Background(), "test")

			// Push timeseries
			for idx, req := range testData.reqs {
				_, err := i.v2Push(ctx, req)

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					assert.NoError(t, err)
				} else {
					assert.Equal(t, testData.expectedErr, err)
				}
			}

			// Read back samples to see what has been really ingested
			res, err := i.v2Query(ctx, &client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, testData.expectedIngested, res.Timeseries)

			// Check tracked Prometheus metrics
			metricNames := []string{"cortex_ingester_ingested_samples_total", "cortex_ingester_ingested_samples_failures_total"}
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), metricNames...)
			assert.NoError(t, err)
		})
	}
}

func Test_Ingester_v2LabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := []string{"__name__", "status", "route"}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Get label names
	res, err := i.v2LabelNames(ctx, &client.LabelNamesRequest{})
	require.NoError(t, err)
	assert.ElementsMatch(t, expected, res.LabelNames)
}

func Test_Ingester_v2LabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := map[string][]string{
		"__name__": {"test_1", "test_2"},
		"status":   {"200", "500"},
		"route":    {"get_user"},
		"unknown":  {},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Get label values
	for labelName, expectedValues := range expected {
		req := &client.LabelValuesRequest{LabelName: labelName}
		res, err := i.v2LabelValues(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expectedValues, res.LabelValues)
	}
}

func Test_Ingester_v2Query(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatcher
		expected []client.TimeSeries
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: []client.TimeSeries{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[0].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: client.FromLabelsToLabelAdapters(series[1].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[2].lbls), Samples: []client.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[0].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: client.FromLabelsToLabelAdapters(series[1].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[2].lbls), Samples: []client.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[1].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []client.TimeSeries{
				{Labels: client.FromLabelsToLabelAdapters(series[0].lbls), Samples: []client.Sample{{Value: 1, TimestampMs: 100000}}},
			},
		},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	defer i.Shutdown()
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Get label values
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &client.QueryRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				Matchers:         testData.matchers,
			}

			res, err := i.v2Query(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Timeseries)
		})
	}
}

func mockWriteRequest(lbls labels.Labels, value float64, timestampMs int64) (*client.WriteRequest, *client.QueryResponse) {
	samples := []client.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := client.ToWriteRequest([]labels.Labels{lbls}, samples, client.API)

	// Generate the expected response
	expectedResponse := &client.QueryResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels:  client.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	return req, expectedResponse
}

func newIngesterMockWithTSDBStorage(ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, func(), error) {
	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits)
	if err != nil {
		return nil, nil, err
	}

	// Create a temporary directory for TSDB
	tempDir, err := ioutil.TempDir("", "tsdb")
	if err != nil {
		return nil, nil, err
	}

	ingesterCfg.TSDBEnabled = true
	ingesterCfg.TSDBConfig.Dir = tempDir
	ingesterCfg.TSDBConfig.Backend = "s3"
	ingesterCfg.TSDBConfig.S3.Endpoint = "localhost"

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil, registerer)
	if err != nil {
		return nil, nil, err
	}

	// Create a cleanup function that the caller should call with defer
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return ingester, cleanup, nil
}
