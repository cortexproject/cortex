package ingester

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestIngester_v2Push(t *testing.T) {
	metricLabelAdapters := []client.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := client.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_discarded_samples_total",
	}
	userID := "test"

	tests := map[string]struct {
		reqs                     []*client.WriteRequest
		expectedErr              error
		expectedIngested         []client.TimeSeries
		expectedMetadataIngested []*client.MetricMetadata
		expectedMetrics          string
		additionalMetrics        []string
	}{
		"should succeed on valid series and metadata": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 9}},
					[]*client.MetricMetadata{
						{MetricName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: client.COUNTER},
					},
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 10}},
					[]*client.MetricMetadata{
						{MetricName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: client.GAUGE},
					},
					client.API),
			},
			expectedErr: nil,
			expectedIngested: []client.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []client.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*client.MetricMetadata{
				{MetricName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: client.GAUGE},
				{MetricName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: client.COUNTER},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
			`,
		},
		"should soft fail on sample out of order": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(errors.Wrapf(storage.ErrOutOfOrderSample, "series=%s, timestamp=%s", metricLabels.String(), model.Time(9).Time().UTC().Format(time.RFC3339Nano)), userID).Error()),
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
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-order",user="test"} 1
			`,
		},
		"should soft fail on sample out of bound": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 1575043969 - (86400 * 1000)}},
					nil,
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(errors.Wrapf(storage.ErrOutOfBounds, "series=%s, timestamp=%s", metricLabels.String(), model.Time(1575043969-(86400*1000)).Time().UTC().Format(time.RFC3339Nano)), userID).Error()),
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
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-bounds",user="test"} 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*client.WriteRequest{
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					client.API),
				client.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]client.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					client.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(errors.Wrapf(storage.ErrDuplicateSampleForTimestamp, "series=%s, timestamp=%s", metricLabels.String(), model.Time(1575043969).Time().UTC().Format(time.RFC3339Nano)), userID).Error()),
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
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="new-value-for-timestamp",user="test"} 1
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			registry.MustRegister(validation.DiscardedSamples)
			validation.DiscardedSamples.Reset()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig()
			cfg.LifecyclerConfig.JoinAfter = 0

			i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
			defer cleanup()

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

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

			// Read back metadata to see what has been really ingested.
			mres, err := i.MetricsMetadata(ctx, &client.MetricsMetadataRequest{})

			require.NoError(t, err)
			require.NotNil(t, res)

			// Order is never guaranteed.
			assert.ElementsMatch(t, testData.expectedMetadataIngested, mres.Metadata)

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)
		})
	}
}

func TestIngester_v2Push_ShouldHandleTheCaseTheCachedReferenceIsInvalid(t *testing.T) {
	metricLabelAdapters := []client.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := client.FromLabelAdaptersToLabels(metricLabelAdapters)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	ctx := user.InjectOrgID(context.Background(), userID)

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Set a wrong cached reference for the series we're going to push.
	db, err := i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)
	require.NotNil(t, db)
	db.refCache.SetRef(time.Now(), metricLabels, 12345)

	// Push the same series multiple times, each time with an increasing timestamp
	for j := 1; j <= 3; j++ {
		req := client.ToWriteRequest(
			[]labels.Labels{metricLabels},
			[]client.Sample{{Value: float64(j), TimestampMs: int64(j)}},
			nil,
			client.API)

		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)

		// Invalidate reference between pushes. It triggers different AddFast path.
		// On first push, "initAppender" is used, on next pushes, "headAppender" is used.
		// Unfortunately they return ErrNotFound differently.
		db.refCache.SetRef(time.Now(), metricLabels, 12345)
	}

	// Read back samples to see what has been really ingested
	res, err := i.v2Query(ctx, &client.QueryRequest{
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
	})

	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []client.TimeSeries{
		{Labels: metricLabelAdapters, Samples: []client.Sample{
			{Value: 1, TimestampMs: 1},
			{Value: 2, TimestampMs: 2},
			{Value: 3, TimestampMs: 3},
		}},
	}, res.Timeseries)
}

func TestIngester_v2Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := []client.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := client.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*client.WriteRequest{
			client.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]client.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				client.API),
			client.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]client.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				client.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.v2Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total 4
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total 0
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 2
		# HELP cortex_ingester_memory_series The current number of series in memory.
		# TYPE cortex_ingester_memory_series gauge
		cortex_ingester_memory_series 2
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func Benchmark_Ingester_v2PushOnOutOfBoundsSamplesWithHighConcurrency(b *testing.B) {
	const (
		numSamplesPerRequest = 1000
		numRequestsPerClient = 10
		numConcurrentClients = 10000
	)

	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	ingester, cleanup, err := newIngesterMockWithTSDBStorage(cfg, registry)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
	defer cleanup()

	// Wait until the ingester is ACTIVE
	test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	// Push a single time series to set the TSDB min time.
	metricLabelAdapters := []client.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := client.FromLabelAdaptersToLabels(metricLabelAdapters)

	currTimeReq := client.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]client.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
		nil,
		client.API)
	_, err = ingester.v2Push(ctx, currTimeReq)
	require.NoError(b, err)

	// Prepare a request containing out of bound samples.
	metrics := make([]labels.Labels, 0, numSamplesPerRequest)
	samples := make([]client.Sample, 0, numSamplesPerRequest)
	for i := 0; i < numSamplesPerRequest; i++ {
		metrics = append(metrics, metricLabels)
		samples = append(samples, client.Sample{Value: float64(i), TimestampMs: 0})
	}
	outOfBoundReq := client.ToWriteRequest(metrics, samples, nil, client.API)

	// Run the benchmark.
	wg := sync.WaitGroup{}
	wg.Add(numConcurrentClients)
	start := make(chan struct{})

	for c := 0; c < numConcurrentClients; c++ {
		go func() {
			defer wg.Done()
			<-start

			for n := 0; n < numRequestsPerClient; n++ {
				ingester.v2Push(ctx, outOfBoundReq) // nolint:errcheck
			}
		}()
	}

	b.ResetTimer()
	close(start)
	wg.Wait()
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
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
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
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
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
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
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
func TestIngester_v2Query_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.QueryRequest{}

	res, err := i.v2Query(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.QueryResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2LabelValues_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelValuesRequest{}

	res, err := i.v2LabelValues(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelValuesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2LabelNames_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelNamesRequest{}

	res, err := i.v2LabelNames(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelNamesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_v2Push_ShouldNotCreateTSDBIfNotInActiveState(t *testing.T) {
	// Configure the lifecycler to not immediately join the ring, to make sure
	// the ingester will NOT be in the ACTIVE state when we'll push samples.
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 10 * time.Second

	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.WriteRequest{}

	res, err := i.v2Push(ctx, req)
	assert.Equal(t, wrapWithUser(fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), userID).Error(), err.Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_getOrCreateTSDB_ShouldNotAllowToCreateTSDBIfIngesterStateIsNotActive(t *testing.T) {
	tests := map[string]struct {
		state       ring.IngesterState
		expectedErr error
	}{
		"not allow to create TSDB if in PENDING state": {
			state:       ring.PENDING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.PENDING),
		},
		"not allow to create TSDB if in JOINING state": {
			state:       ring.JOINING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.JOINING),
		},
		"not allow to create TSDB if in LEAVING state": {
			state:       ring.LEAVING,
			expectedErr: fmt.Errorf(errTSDBCreateIncompatibleState, ring.LEAVING),
		},
		"allow to create TSDB if in ACTIVE state": {
			state:       ring.ACTIVE,
			expectedErr: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig()
			cfg.LifecyclerConfig.JoinAfter = 60 * time.Second

			i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
			defer cleanup()

			// Switch ingester state to the expected one in the test
			if i.lifecycler.GetState() != testData.state {
				var stateChain []ring.IngesterState

				if testData.state == ring.LEAVING {
					stateChain = []ring.IngesterState{ring.ACTIVE, ring.LEAVING}
				} else {
					stateChain = []ring.IngesterState{testData.state}
				}

				for _, s := range stateChain {
					err = i.lifecycler.ChangeState(context.Background(), s)
					require.NoError(t, err)
				}
			}

			db, err := i.getOrCreateTSDB("test", false)
			assert.Equal(t, testData.expectedErr, err)

			if testData.expectedErr != nil {
				assert.Nil(t, db)
			} else {
				assert.NotNil(t, db)
			}
		})
	}
}

func Test_Ingester_v2MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatchers
		expected []*client.Metric
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*client.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should NOT filter metrics by time range to always return known metrics even when queried for older time ranges": {
			from: 100,
			to:   1000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*client.Metric{
				{Labels: client.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: client.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
			}

			res, err := i.v2MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)
		})
	}
}

func TestIngester_v2QueryStream(t *testing.T) {
	// Create ingester.
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)
	lbls := labels.Labels{{Name: labels.MetricName, Value: "foo"}}
	req, _, expectedResponse := mockWriteRequest(lbls, 123000, 456)
	_, err = i.v2Push(ctx, req)
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   200000,
		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	count := 0
	var lastResp *client.QueryStreamResponse
	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		count += len(resp.Timeseries)
		lastResp = resp
	}
	require.Equal(t, 1, count)
	require.Equal(t, expectedResponse, lastResp)
}

func mockWriteRequest(lbls labels.Labels, value float64, timestampMs int64) (*client.WriteRequest, *client.QueryResponse, *client.QueryStreamResponse) {
	samples := []client.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := client.ToWriteRequest([]labels.Labels{lbls}, samples, nil, client.API)

	// Generate the expected response
	expectedQueryRes := &client.QueryResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels:  client.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	expectedQueryStreamRes := &client.QueryStreamResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels:  client.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	return req, expectedQueryRes, expectedQueryStreamRes
}

func newIngesterMockWithTSDBStorage(ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, func(), error) {
	// Create a temporary directory for TSDB
	tempDir, err := ioutil.TempDir("", "tsdb")
	if err != nil {
		return nil, nil, err
	}

	// Create a cleanup function that the caller should call with defer
	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	ingester, err := newIngesterMockWithTSDBStorageAndLimits(ingesterCfg, defaultLimitsTestConfig(), tempDir, registerer)

	return ingester, cleanup, err
}

func newIngesterMockWithTSDBStorageAndLimits(ingesterCfg Config, limits validation.Limits, dir string, registerer prometheus.Registerer) (*Ingester, error) {
	clientCfg := defaultClientTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, err
	}

	ingesterCfg.BlocksStorageEnabled = true
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dir
	ingesterCfg.BlocksStorageConfig.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.S3.Endpoint = "localhost"

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, registerer)
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func TestIngester_v2LoadTSDBOnStartup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		setup func(*testing.T, string)
		check func(*testing.T, *Ingester)
	}{
		"empty user dir": {
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user0"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Empty(t, i.getTSDB("user0"), "tsdb created for empty user dir")
			},
		},
		"empty tsdbs": {
			setup: func(t *testing.T, dir string) {},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs), "user tsdb's were created on empty dir")
			},
		},
		"missing tsdb dir": {
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(dir))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs), "user tsdb's were created on missing dir")
			},
		},
		"populated user dirs with unpopulated": {
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user2"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.NotNil(t, i.getTSDB("user0"), "tsdb not created for non-empty user dir")
				require.NotNil(t, i.getTSDB("user1"), "tsdb not created for non-empty user dir")
				require.Empty(t, i.getTSDB("user2"), "tsdb created for empty user dir")
			},
		},
	}

	for name, test := range tests {
		testName := name
		testData := test
		t.Run(testName, func(t *testing.T) {
			clientCfg := defaultClientTestConfig()
			limits := defaultLimitsTestConfig()

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Create a temporary directory for TSDB
			tempDir, err := ioutil.TempDir("", "tsdb")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			ingesterCfg := defaultIngesterTestConfig()
			ingesterCfg.BlocksStorageEnabled = true
			ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
			ingesterCfg.BlocksStorageConfig.Backend = "s3"
			ingesterCfg.BlocksStorageConfig.S3.Endpoint = "localhost"

			// setup the tsdbs dir
			testData.setup(t, tempDir)

			ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

			defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

			testData.check(t, ingester)
		})
	}
}

func TestIngester_shipBlocks(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 10*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Create the TSDB for 3 users and then replace the shipper with the mocked one
	mocks := []*shipperMock{}
	for _, userID := range []string{"user-1", "user-2", "user-3"} {
		userDB, err := i.getOrCreateTSDB(userID, false)
		require.NoError(t, err)
		require.NotNil(t, userDB)

		m := &shipperMock{}
		m.On("Sync", mock.Anything).Return(0, nil)
		mocks = append(mocks, m)

		userDB.shipper = m
	}

	// Ship blocks and assert on the mocked shipper
	i.shipBlocks(context.Background())

	for _, m := range mocks {
		m.AssertNumberOfCalls(t, "Sync", 1)
	}
}

type shipperMock struct {
	mock.Mock
}

// Sync mocks Shipper.Sync()
func (m *shipperMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func TestIngester_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, m *shipperMock)
	}{
		"ingesterShutdown": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, m *shipperMock) {
				pushSingleSample(t, i)

				// Nothing shipped yet.
				m.AssertNumberOfCalls(t, "Sync", 0)

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				m.AssertNumberOfCalls(t, "Sync", 1)
			},
		},

		"shutdownHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, m *shipperMock) {
				pushSingleSample(t, i)

				// Nothing shipped yet.
				m.AssertNumberOfCalls(t, "Sync", 0)

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/shutdown", nil))

				verifyCompactedHead(t, i, true)
				m.AssertNumberOfCalls(t, "Sync", 1)
			},
		},

		"flushHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, m *shipperMock) {
				pushSingleSample(t, i)

				// Nothing shipped yet.
				m.AssertNumberOfCalls(t, "Sync", 0)

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/shutdown", nil))

				// Flush handler only triggers compactions, but doesn't wait for them to finish. Let's wait for a moment, and then verify.
				time.Sleep(1 * time.Second)

				verifyCompactedHead(t, i, true)
				m.AssertNumberOfCalls(t, "Sync", 1)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig()
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute // Long enough to not be reached during the test.

			if tc.setupIngester != nil {
				tc.setupIngester(&cfg)
			}

			// Create ingester
			i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
			require.NoError(t, err)
			t.Cleanup(cleanup)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's ACTIVE
			test.Poll(t, 10*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// mock user's shipper
			m := mockUserShipper(t, i)
			m.On("Sync", mock.Anything).Return(0, nil)

			tc.action(t, i, m)
		})
	}
}

func TestIngester_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 10*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// mock user's shipper
	m := mockUserShipper(t, i)
	m.On("Sync", mock.Anything).Return(0, nil)

	// Push some data.
	pushSingleSample(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	m.AssertNumberOfCalls(t, "Sync", 0)

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	i, err = NewV2ForFlusher(i.cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	m = mockUserShipper(t, i)
	m.On("Sync", mock.Anything).Return(0, nil)

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	m.AssertNumberOfCalls(t, "Sync", 1)

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
}

func mockUserShipper(t *testing.T, i *Ingester) *shipperMock {
	m := &shipperMock{}
	userDB, err := i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)
	require.NotNil(t, userDB)

	userDB.shipper = m
	return m
}

func Test_Ingester_v2UserStats(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.tick()
		db.ingestedRuleSamples.tick()
	}

	// Get label names
	res, err := i.v2UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	assert.Equal(t, uint64(3), res.NumSeries)
}

func Test_Ingester_v2AllUserStats(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	defer cleanup()

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _, _ := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.tick()
		db.ingestedRuleSamples.tick()
	}

	// Get label names
	res, err := i.v2AllUserStats(context.Background(), &client.UserStatsRequest{})
	require.NoError(t, err)

	expect := []*client.UserIDStatsResponse{
		{
			UserId: "user-1",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.2,
				NumSeries:         3,
				ApiIngestionRate:  0.2,
				RuleIngestionRate: 0,
			},
		},
		{
			UserId: "user-2",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.13333333333333333,
				NumSeries:         2,
				ApiIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
			},
		},
	}
	assert.ElementsMatch(t, expect, res.Stats)
}

func TestIngesterCompactIdleBlock(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.

	r := prometheus.NewRegistry()

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, r)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 10*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSample(t, i)

	i.compactBlocks(context.Background(), false)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName))

	// wait one second -- TSDB is now idle.
	time.Sleep(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout)

	i.compactBlocks(context.Background(), false)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName))

	// Pushing another sample still works.
	pushSingleSample(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName))
}

func verifyCompactedHead(t *testing.T, i *Ingester, expected bool) {
	db := i.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()
	require.Equal(t, expected, h.NumSeries() == 0)
}

func pushSingleSample(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _ := mockWriteRequest(labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	_, err := i.v2Push(ctx, req)
	require.NoError(t, err)
}

func TestHeadCompactionOnStartup(t *testing.T) {
	// Create a temporary directory for TSDB
	tempDir, err := ioutil.TempDir("", "tsdb")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	// Build TSDB for user, with data covering 24 hours.
	{
		// Number of full chunks, 12 chunks for 24hrs.
		numFullChunks := 12
		chunkRange := 2 * time.Hour.Milliseconds()

		userDir := filepath.Join(tempDir, userID)
		require.NoError(t, os.Mkdir(userDir, 0700))

		db, err := tsdb.Open(userDir, nil, nil, &tsdb.Options{
			RetentionDuration: int64(time.Hour * 25 / time.Millisecond),
			NoLockfile:        true,
			MinBlockDuration:  chunkRange,
			MaxBlockDuration:  chunkRange,
		})
		require.NoError(t, err)

		db.DisableCompactions()
		head := db.Head()

		l := labels.Labels{{Name: "n", Value: "v"}}
		for i := 0; i < numFullChunks; i++ {
			// Not using db.Appender() as it checks for compaction.
			app := head.Appender(context.Background())
			_, err := app.Add(l, int64(i)*chunkRange+1, 9.99)
			require.NoError(t, err)
			_, err = app.Add(l, int64(i+1)*chunkRange, 9.99)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}

		dur := time.Duration(head.MaxTime()-head.MinTime()) * time.Millisecond
		require.True(t, dur > 23*time.Hour)
		require.Equal(t, 0, len(db.Blocks()))
		require.NoError(t, db.Close())
	}

	clientCfg := defaultClientTestConfig()
	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	ingesterCfg := defaultIngesterTestConfig()
	ingesterCfg.BlocksStorageEnabled = true
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
	ingesterCfg.BlocksStorageConfig.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.S3.Endpoint = "localhost"
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = 2 * 24 * time.Hour // Make sure that no newly created blocks are deleted.

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()

	dur := time.Duration(h.MaxTime()-h.MinTime()) * time.Millisecond
	require.True(t, dur <= 2*time.Hour)
	require.Equal(t, 11, len(db.Blocks()))
}

func TestIngester_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, cleanup, err := newIngesterMockWithTSDBStorage(cfg, nil)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 10*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSample(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}
