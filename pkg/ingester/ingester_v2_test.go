package ingester

import (
	"bytes"
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	util_math "github.com/cortexproject/cortex/pkg/util/math"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestIngester_v2Push(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_discarded_samples_total",
		"cortex_ingester_active_series",
	}
	userID := "test"

	tests := map[string]struct {
		reqs                     []*cortexpb.WriteRequest
		expectedErr              error
		expectedIngested         []cortexpb.TimeSeries
		expectedMetadataIngested []*cortexpb.MetricMetadata
		expectedMetrics          string
		additionalMetrics        []string
		disableActiveSeries      bool
	}{
		"should succeed on valid series and metadata": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					[]*cortexpb.MetricMetadata{
						{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
					},
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					[]*cortexpb.MetricMetadata{
						{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
					},
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*cortexpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
				{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
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
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetrics: `
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
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfOrderSample, model.Time(9), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
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
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on sample out of bound": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (86400 * 1000)}},
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(1575043969-(86400*1000)), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
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
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*cortexpb.WriteRequest{
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrDuplicateSampleForTimestamp, model.Time(1575043969), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
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
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
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
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries

			i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

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

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.v2UpdateActiveSeries()
			}

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)
		})
	}
}

func TestIngester_v2Push_ShouldHandleTheCaseTheCachedReferenceIsInvalid(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

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
		req := cortexpb.ToWriteRequest(
			[]labels.Labels{metricLabels},
			[]cortexpb.Sample{{Value: float64(j), TimestampMs: int64(j)}},
			nil,
			cortexpb.API)

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
	assert.Equal(t, []cortexpb.TimeSeries{
		{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{
			{Value: 1, TimestampMs: 1},
			{Value: 2, TimestampMs: 2},
			{Value: 3, TimestampMs: 3},
		}},
	}, res.Timeseries)
}

func TestIngester_v2Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequest{
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.v2Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series for metrics check.
	i.v2UpdateActiveSeries()

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
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 1
		cortex_ingester_active_series{user="test-2"} 1
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_v2Push_DecreaseInactiveSeries(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig()
	cfg.ActiveSeriesMetricsIdleTimeout = 100 * time.Millisecond
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequest{
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequest(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.v2Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Wait a bit to make series inactive (set to 100ms above).
	time.Sleep(200 * time.Millisecond)

	// Update active series for metrics check. This will remove inactive series.
	i.v2UpdateActiveSeries()

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 0
		cortex_ingester_active_series{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func Benchmark_Ingester_v2PushOnError(b *testing.B) {
	var (
		ctx             = user.InjectOrgID(context.Background(), userID)
		sampleTimestamp = int64(100)
		metricName      = "test"
	)

	scenarios := map[string]struct {
		numSeriesPerRequest  int
		numConcurrentClients int
	}{
		"no concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1,
		},
		"low concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 100,
		},
		"high concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1000,
		},
	}

	tests := map[string]struct {
		prepareConfig   func(limits *validation.Limits)
		beforeBenchmark func(b *testing.B, ingester *Ingester, numSeriesPerRequest int)
		runBenchmark    func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample)
	}{
		"out of bound samples": {
			prepareConfig: func(limits *validation.Limits) {},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a single time series to set the TSDB min time.
				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{{{Name: labels.MetricName, Value: metricName}}},
					[]cortexpb.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
					nil,
					cortexpb.API)
				_, err := ingester.v2Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfBounds.Error()

				// Push out of bound samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.v2Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, cortexpb.API)) // nolint:errcheck

					if !strings.Contains(err.Error(), expectedErr) {
						b.Fatalf("unexpected error. expected: %s actual: %s", expectedErr, err.Error())
					}
				}
			},
		},
		"out of order samples": {
			prepareConfig: func(limits *validation.Limits) {},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// For each series, push a single sample with a timestamp greater than next pushes.
				for i := 0; i < numSeriesPerRequest; i++ {
					currTimeReq := cortexpb.ToWriteRequest(
						[]labels.Labels{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}}},
						[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
						nil,
						cortexpb.API)

					_, err := ingester.v2Push(ctx, currTimeReq)
					require.NoError(b, err)
				}
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfOrderSample.Error()

				// Push out of order samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.v2Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, cortexpb.API)) // nolint:errcheck

					if !strings.Contains(err.Error(), expectedErr) {
						b.Fatalf("unexpected error. expected: %s actual: %s", expectedErr, err.Error())
					}
				}
			},
		},
		"per-user series limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLocalSeriesPerUser = 1
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with a metric name different than the one used during the benchmark.
				metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "another"}}
				metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)

				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					cortexpb.API)
				_, err := ingester.v2Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := "per-user series limit"

				// Push series with a different name than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.v2Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, cortexpb.API)) // nolint:errcheck

					if !strings.Contains(err.Error(), expectedErr) {
						b.Fatalf("unexpected error. expected: %s actual: %s", expectedErr, err.Error())
					}
				}
			},
		},
		"per-metric series limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLocalSeriesPerMetric = 1
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with the same metric name but different labels than the one used during the benchmark.
				metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: "another"}}
				metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)

				currTimeReq := cortexpb.ToWriteRequest(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					cortexpb.API)
				_, err := ingester.v2Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := "per-metric series limit"

				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.v2Push(ctx, cortexpb.ToWriteRequest(metrics, samples, nil, cortexpb.API)) // nolint:errcheck

					if !strings.Contains(err.Error(), expectedErr) {
						b.Fatalf("unexpected error. expected: %s actual: %s", expectedErr, err.Error())
					}
				}
			},
		},
	}

	for testName, testData := range tests {
		for scenarioName, scenario := range scenarios {
			b.Run(fmt.Sprintf("failure: %s, scenario: %s", testName, scenarioName), func(b *testing.B) {
				registry := prometheus.NewRegistry()

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig()
				cfg.LifecyclerConfig.JoinAfter = 0

				limits := defaultLimitsTestConfig()
				testData.prepareConfig(&limits)

				ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, "", registry)
				require.NoError(b, err)
				require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
				defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

				// Wait until the ingester is ACTIVE
				test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
					return ingester.lifecycler.GetState()
				})

				testData.beforeBenchmark(b, ingester, scenario.numSeriesPerRequest)

				// Prepare the request.
				metrics := make([]labels.Labels, 0, scenario.numSeriesPerRequest)
				samples := make([]cortexpb.Sample, 0, scenario.numSeriesPerRequest)
				for i := 0; i < scenario.numSeriesPerRequest; i++ {
					metrics = append(metrics, labels.Labels{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}})
					samples = append(samples, cortexpb.Sample{Value: float64(i), TimestampMs: sampleTimestamp})
				}

				// Run the benchmark.
				wg := sync.WaitGroup{}
				wg.Add(scenario.numConcurrentClients)
				start := make(chan struct{})

				b.ReportAllocs()
				b.ResetTimer()

				for c := 0; c < scenario.numConcurrentClients; c++ {
					go func() {
						defer wg.Done()
						<-start

						testData.runBenchmark(b, ingester, metrics, samples)
					}()
				}

				b.ResetTimer()
				close(start)
				wg.Wait()
			})
		}
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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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
		expected []cortexpb.TimeSeries
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: []cortexpb.TimeSeries{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

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

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &cortexpb.WriteRequest{}

	res, err := i.v2Push(ctx, req)
	assert.Equal(t, wrapWithUser(fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), userID).Error(), err.Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_getOrCreateTSDB_ShouldNotAllowToCreateTSDBIfIngesterStateIsNotActive(t *testing.T) {
	tests := map[string]struct {
		state       ring.InstanceState
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

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Switch ingester state to the expected one in the test
			if i.lifecycler.GetState() != testData.state {
				var stateChain []ring.InstanceState

				if testData.state == ring.LEAVING {
					stateChain = []ring.InstanceState{ring.ACTIVE, ring.LEAVING}
				} else {
					stateChain = []ring.InstanceState{testData.state}
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
		expected []*cortexpb.Metric
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*cortexpb.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
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
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
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
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
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
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
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
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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

func Test_Ingester_v2MetricsForLabelMatchers_Deduplication(t *testing.T) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeries(t, userID, numSeries, now)
	ctx := user.InjectOrgID(context.Background(), "test")

	req := &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: now,
		EndTimestampMs:   now,
		// Overlapping matchers to make sure series are correctly deduplicated.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}},
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*0"},
			}},
		},
	}

	res, err := i.v2MetricsForLabelMatchers(ctx, req)
	require.NoError(t, err)
	require.Len(t, res.GetMetric(), numSeries)
}

func Benchmark_Ingester_v2MetricsForLabelMatchers(b *testing.B) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeries(b, userID, numSeries, now)
	ctx := user.InjectOrgID(context.Background(), "test")

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		req := &client.MetricsForLabelMatchersRequest{
			StartTimestampMs: now,
			EndTimestampMs:   now,
			MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}}},
		}

		res, err := i.v2MetricsForLabelMatchers(ctx, req)
		require.NoError(b, err)
		require.Len(b, res.GetMetric(), numSeries)
	}
}

// createIngesterWithSeries creates an ingester and push numSeries with 1 sample
// per series.
func createIngesterWithSeries(t testing.TB, userID string, numSeries int, timestamp int64) *Ingester {
	const maxBatchSize = 1000

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures.
	ctx := user.InjectOrgID(context.Background(), userID)

	for o := 0; o < numSeries; o += maxBatchSize {
		batchSize := util_math.Min(maxBatchSize, numSeries-o)

		// Generate metrics and samples (1 for each series).
		metrics := make([]labels.Labels, 0, batchSize)
		samples := make([]cortexpb.Sample, 0, batchSize)

		for s := 0; s < batchSize; s++ {
			metrics = append(metrics, labels.Labels{
				{Name: labels.MetricName, Value: fmt.Sprintf("test_%d", o+s)},
			})

			samples = append(samples, cortexpb.Sample{
				TimestampMs: timestamp,
				Value:       1,
			})
		}

		// Send metrics to the ingester.
		req := cortexpb.ToWriteRequest(metrics, samples, nil, cortexpb.API)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	return i
}

func TestIngester_v2QueryStream(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig()

	// change stream type in runtime.
	var streamType QueryStreamType
	cfg.StreamTypeFn = func() QueryStreamType {
		return streamType
	}

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)
	lbls := labels.Labels{{Name: labels.MetricName, Value: "foo"}}
	req, _, expectedResponseSamples, expectedResponseChunks := mockWriteRequest(t, lbls, 123000, 456)
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

	queryRequest := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   200000,
		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	}

	samplesTest := func(t *testing.T) {
		s, err := c.QueryStream(ctx, queryRequest)
		require.NoError(t, err)

		count := 0
		var lastResp *client.QueryStreamResponse
		for {
			resp, err := s.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.Zero(t, len(resp.Chunkseries)) // No chunks expected
			count += len(resp.Timeseries)
			lastResp = resp
		}
		require.Equal(t, 1, count)
		require.Equal(t, expectedResponseSamples, lastResp)
	}

	chunksTest := func(t *testing.T) {
		s, err := c.QueryStream(ctx, queryRequest)
		require.NoError(t, err)

		count := 0
		var lastResp *client.QueryStreamResponse
		for {
			resp, err := s.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			require.Zero(t, len(resp.Timeseries)) // No samples expected
			count += len(resp.Chunkseries)
			lastResp = resp
		}
		require.Equal(t, 1, count)
		require.Equal(t, expectedResponseChunks, lastResp)
	}

	streamType = QueryStreamDefault
	t.Run("default", samplesTest)

	streamType = QueryStreamSamples
	t.Run("samples", samplesTest)

	streamType = QueryStreamChunks
	t.Run("chunks", chunksTest)
}

func TestIngester_v2QueryStreamManySamples(t *testing.T) {
	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 100000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 10k samples encode to around 140 KiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:10000]))
	require.NoError(t, err)

	// 100k samples encode to around 1.4 MiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 50k samples encode to around 716 KiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:50000]))
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
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Timeseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Timeseries)

		for _, ts := range resp.Timeseries {
			totalSamples += len(ts.Samples)
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (10k + 50k in first, 100k in second)
	// or 3 messages (small series first, 100k second, small series last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 10000+50000+samplesCount, totalSamples)
}

func TestIngester_v2QueryStreamManySamplesChunks(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig()
	cfg.StreamChunksWhenUsingBlocks = true

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 100k samples in chunks use about 154 KiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:100000]))
	require.NoError(t, err)

	// 1M samples in chunks use about 1.51 MiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 500k samples in chunks need 775 KiB,
	_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:500000]))
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
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Chunkseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Chunkseries)

		for _, ts := range resp.Chunkseries {
			for _, c := range ts.Chunks {
				ch, err := encoding.NewForEncoding(encoding.Encoding(c.Encoding))
				require.NoError(t, err)
				require.NoError(t, ch.UnmarshalFromBuf(c.Data))

				totalSamples += ch.Len()
			}
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (100k + 500k in first, 1M in second)
	// or 3 messages (100k or 500k first, 1M second, and 500k or 100k last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+samplesCount, totalSamples)
}

func writeRequestSingleSeries(lbls labels.Labels, samples []cortexpb.Sample) *cortexpb.WriteRequest {
	req := &cortexpb.WriteRequest{
		Source: cortexpb.API,
	}

	ts := cortexpb.TimeSeries{}
	ts.Labels = cortexpb.FromLabelsToLabelAdapters(lbls)
	ts.Samples = samples
	req.Timeseries = append(req.Timeseries, cortexpb.PreallocTimeseries{TimeSeries: &ts})

	return req
}

type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockQueryStreamServer) Send(response *client.QueryStreamResponse) error {
	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

func BenchmarkIngester_v2QueryStream_Samples(b *testing.B) {
	benchmarkV2QueryStream(b, false)
}

func BenchmarkIngester_v2QueryStream_Chunks(b *testing.B) {
	benchmarkV2QueryStream(b, true)
}

func benchmarkV2QueryStream(b *testing.B, streamChunks bool) {
	cfg := defaultIngesterTestConfig()
	cfg.StreamChunksWhenUsingBlocks = streamChunks

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(b, cfg, nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(b, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	const seriesCount = 100
	for s := 0; s < seriesCount; s++ {
		_, err = i.v2Push(ctx, writeRequestSingleSeries(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: strconv.Itoa(s)}}, samples))
		require.NoError(b, err)
	}

	req := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	}

	mockStream := &mockQueryStreamServer{ctx: ctx}

	b.ResetTimer()

	for ix := 0; ix < b.N; ix++ {
		err := i.v2QueryStream(req, mockStream)
		require.NoError(b, err)
	}
}

func mockWriteRequest(t *testing.T, lbls labels.Labels, value float64, timestampMs int64) (*cortexpb.WriteRequest, *client.QueryResponse, *client.QueryStreamResponse, *client.QueryStreamResponse) {
	samples := []cortexpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := cortexpb.ToWriteRequest([]labels.Labels{lbls}, samples, nil, cortexpb.API)

	// Generate the expected response
	expectedQueryRes := &client.QueryResponse{
		Timeseries: []cortexpb.TimeSeries{
			{
				Labels:  cortexpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	expectedQueryStreamResSamples := &client.QueryStreamResponse{
		Timeseries: []cortexpb.TimeSeries{
			{
				Labels:  cortexpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(t, err)
	app.Append(timestampMs, value)
	chunk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         int32(encoding.PrometheusXorChunk),
						Data:             chunk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryRes, expectedQueryStreamResSamples, expectedQueryStreamResChunks
}

func prepareIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, error) {
	return prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, defaultLimitsTestConfig(), "", registerer)
}

func prepareIngesterWithBlocksStorageAndLimits(t testing.TB, ingesterCfg Config, limits validation.Limits, dataDir string, registerer prometheus.Registerer) (*Ingester, error) {
	// Create a data dir if none has been provided.
	if dataDir == "" {
		var err error
		if dataDir, err = ioutil.TempDir("", "ingester"); err != nil {
			return nil, err
		}

		t.Cleanup(func() {
			require.NoError(t, os.RemoveAll(dataDir))
		})
	}

	bucketDir, err := ioutil.TempDir("", "bucket")
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(bucketDir))
	})

	clientCfg := defaultClientTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, err
	}

	ingesterCfg.BlocksStorageEnabled = true
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dataDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = bucketDir

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, registerer, log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func TestIngester_v2OpenExistingTSDBOnStartup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		concurrency int
		setup       func(*testing.T, string)
		check       func(*testing.T, *Ingester)
		expectedErr string
	}{
		"should not load TSDB if the user directory is empty": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user0"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Nil(t, i.getTSDB("user0"))
			},
		},
		"should not load any TSDB if the root directory is empty": {
			concurrency: 10,
			setup:       func(t *testing.T, dir string) {},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs))
			},
		},
		"should not load any TSDB is the root directory is missing": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(dir))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.TSDBState.dbs))
			},
		},
		"should load TSDB for any non-empty user directory": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user2"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 2, len(i.TSDBState.dbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
			},
		},
		"should load all TSDBs on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.TSDBState.dbs))
				require.NotNil(t, i.getTSDB("user0"))
				require.NotNil(t, i.getTSDB("user1"))
				require.NotNil(t, i.getTSDB("user2"))
				require.NotNil(t, i.getTSDB("user3"))
				require.NotNil(t, i.getTSDB("user4"))
			},
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency > number of TSDBs": {
			concurrency: 10,
			setup: func(t *testing.T, dir string) {
				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "chunks_head", ""), 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000002"), nil, 0700))

				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.TSDBState.dbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
			},
			expectedErr: "unable to open TSDB for user user0",
		},
		"should fail and rollback if an error occur while loading a TSDB on concurrency < number of TSDBs": {
			concurrency: 2,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user3", "dummy"), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user4", "dummy"), 0700))

				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "chunks_head", ""), 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, ioutil.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000002"), nil, 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.TSDBState.dbs))
				require.Nil(t, i.getTSDB("user0"))
				require.Nil(t, i.getTSDB("user1"))
				require.Nil(t, i.getTSDB("user2"))
				require.Nil(t, i.getTSDB("user3"))
				require.Nil(t, i.getTSDB("user4"))
			},
			expectedErr: "unable to open TSDB for user user2",
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
			ingesterCfg.BlocksStorageConfig.TSDB.MaxTSDBOpeningConcurrencyOnStartup = testData.concurrency
			ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
			ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"

			// setup the tsdbs dir
			testData.setup(t, tempDir)

			ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil, log.NewNopLogger())
			require.NoError(t, err)

			startErr := services.StartAndAwaitRunning(context.Background(), ingester)
			if testData.expectedErr == "" {
				require.NoError(t, startErr)
			} else {
				require.Error(t, startErr)
				assert.Contains(t, startErr.Error(), testData.expectedErr)
			}

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
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
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

func TestIngester_dontShipBlocksWhenTenantDeletionMarkerIsPresent(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	i.TSDBState.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)
	i.compactBlocks(context.Background(), true)
	i.shipBlocks(context.Background())

	numObjects := len(bucket.Objects())
	require.NotZero(t, numObjects)

	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), bucket, userID, nil, cortex_tsdb.NewTenantDeletionMark(time.Now())))
	numObjects++ // For deletion marker

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	db.lastDeletionMarkCheck.Store(0)

	// After writing tenant deletion mark,
	pushSingleSampleWithMetadata(t, i)
	i.compactBlocks(context.Background(), true)
	i.shipBlocks(context.Background())

	numObjectsAfterMarkingTenantForDeletion := len(bucket.Objects())
	require.Equal(t, numObjects, numObjectsAfterMarkingTenantForDeletion)
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_closeAndDeleteUserTSDBIfIdle_shouldNotCloseTSDBIfShippingIsInProgress(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Mock the shipper to slow down Sync() execution.
	s := mockUserShipper(t, i)
	s.On("Sync", mock.Anything).Run(func(args mock.Arguments) {
		time.Sleep(3 * time.Second)
	}).Return(0, nil)

	// Mock the shipper meta (no blocks).
	db := i.getTSDB(userID)
	require.NoError(t, shipper.WriteMetaFile(log.NewNopLogger(), db.db.Dir(), &shipper.Meta{
		Version: shipper.MetaVersion1,
	}))

	// Run blocks shipping in a separate go routine.
	go i.shipBlocks(ctx)

	// Wait until shipping starts.
	test.Poll(t, 1*time.Second, activeShipping, func() interface{} {
		db.stateMtx.RLock()
		defer db.stateMtx.RUnlock()
		return db.state
	})

	assert.Equal(t, tsdbNotActive, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_closingAndOpeningTsdbConcurrently(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig()
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	_, err = i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)

	iterations := 5000
	chanErr := make(chan error, 1)
	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				_, err = i.getOrCreateTSDB(userID, false)
				if err != nil {
					chanErr <- err
				}
			}
		}
	}()

	for k := 0; k < iterations; k++ {
		i.closeAndDeleteUserTSDBIfIdle(userID)
	}

	select {
	case err := <-chanErr:
		assert.Fail(t, err.Error())
		quit <- true
	default:
		quit <- true
	}
}

func TestIngester_idleCloseEmptyTSDB(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig()
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	db, err := i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Run compaction and shipping.
	i.compactBlocks(context.Background(), true)
	i.shipBlocks(context.Background())

	// Make sure we can close completely empty TSDB without problems.
	require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Verify that it was closed.
	db = i.getTSDB(userID)
	require.Nil(t, db)

	// And we can recreate it again, if needed.
	db, err = i.getOrCreateTSDB(userID, true)
	require.NoError(t, err)
	require.NotNil(t, db)
}

type shipperMock struct {
	mock.Mock
}

// Sync mocks Shipper.Sync()
func (m *shipperMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func TestIngester_invalidSamplesDontChangeLastUpdateTime(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sampleTimestamp := int64(model.Now())

	{
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, sampleTimestamp)
		_, err = i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	lastUpdate := db.lastUpdate.Load()

	// Wait until 1 second passes.
	test.Poll(t, 1*time.Second, time.Now().Unix()+1, func() interface{} {
		return time.Now().Unix()
	})

	// Push another sample to the same metric and timestamp, with different value. We expect to get error.
	{
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 1, sampleTimestamp)
		_, err = i.v2Push(ctx, req)
		require.Error(t, err)
	}

	// Make sure last update hasn't changed.
	require.Equal(t, lastUpdate, db.lastUpdate.Load())
}

func TestIngester_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, reg *prometheus.Registry)
	}{
		"ingesterShutdown": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"shutdownHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/shutdown", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush", nil))

				// Flush handler only triggers compactions, but doesn't wait for them to finish. Let's wait for a moment, and then verify.
				test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
					db := i.getTSDB(userID)
					if db == nil {
						return false
					}
					return db.Head().NumSeries()
				})

				// The above waiting only ensures compaction, waiting another second to register the Sync call.
				time.Sleep(1 * time.Second)

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushMultipleBlocksWithDataSpanning3Days": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				// Pushing 5 samples, spanning over 3 days.
				// First block
				pushSingleSampleAtTime(t, i, 23*time.Hour.Milliseconds())
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()-1)

				// Second block
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()+1)
				pushSingleSampleAtTime(t, i, 25*time.Hour.Milliseconds())

				// Third block, far in the future.
				pushSingleSampleAtTime(t, i, 50*time.Hour.Milliseconds())

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush", nil))

				// Flush handler only triggers compactions, but doesn't wait for them to finish. Let's wait for a moment, and then verify.
				test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
					db := i.getTSDB(userID)
					if db == nil {
						return false
					}
					return db.Head().NumSeries()
				})

				// The above waiting only ensures compaction, waiting another second to register the Sync call.
				time.Sleep(1 * time.Second)

				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 3
				`), "cortex_ingester_shipper_uploads_total"))

				userDB := i.getTSDB(userID)
				require.NotNil(t, userDB)

				blocks := userDB.Blocks()
				require.Equal(t, 3, len(blocks))
				require.Equal(t, 23*time.Hour.Milliseconds(), blocks[0].Meta().MinTime)
				require.Equal(t, 24*time.Hour.Milliseconds(), blocks[0].Meta().MaxTime) // Block maxt is exclusive.

				require.Equal(t, 24*time.Hour.Milliseconds()+1, blocks[1].Meta().MinTime)
				require.Equal(t, 26*time.Hour.Milliseconds(), blocks[1].Meta().MaxTime)

				require.Equal(t, 50*time.Hour.Milliseconds()+1, blocks[2].Meta().MaxTime) // Block maxt is exclusive.
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
			reg := prometheus.NewPedanticRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's ACTIVE
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// mock user's shipper
			tc.action(t, i, reg)
		})
	}
}

func TestIngester_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	reg = prometheus.NewPedanticRegistry()
	i, err = NewV2ForFlusher(i.cfg, i.limits, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))

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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
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
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)

	i.compactBlocks(context.Background(), false)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	i.compactBlocks(context.Background(), false)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// Pushing another sample still works.
	pushSingleSampleWithMetadata(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))
}

func TestIngesterCompactAndCloseIdleTSDB(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Second // Required to enable shipping.
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = 100 * time.Millisecond

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadata(t, i)
	i.v2UpdateActiveSeries()

	metricsToCheck := []string{memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users", "cortex_ingester_active_series",
		"cortex_ingester_memory_metadata", "cortex_ingester_memory_metadata_created_total", "cortex_ingester_memory_metadata_removed_total"}

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))

	// Wait until TSDB has been closed and removed.
	test.Poll(t, 10*time.Second, 0, func() interface{} {
		i.userStatesMtx.Lock()
		defer i.userStatesMtx.Unlock()
		return len(i.TSDBState.dbs)
	})

	require.Greater(t, testutil.ToFloat64(i.TSDBState.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))), float64(0))
	i.v2UpdateActiveSeries()

	// Verify that user has disappeared from metrics.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 0

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
    `), metricsToCheck...))

	// Pushing another sample will recreate TSDB.
	pushSingleSampleWithMetadata(t, i)
	i.v2UpdateActiveSeries()

	// User is back.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))
}

func verifyCompactedHead(t *testing.T, i *Ingester, expected bool) {
	db := i.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()
	require.Equal(t, expected, h.NumSeries() == 0)
}

func pushSingleSampleWithMetadata(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	req.Metadata = append(req.Metadata, &cortexpb.MetricMetadata{MetricFamilyName: "test", Help: "a help for metric", Unit: "", Type: cortexpb.COUNTER})
	_, err := i.v2Push(ctx, req)
	require.NoError(t, err)
}

func pushSingleSampleAtTime(t *testing.T, i *Ingester, ts int64) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, ts)
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
			_, err := app.Append(0, l, int64(i)*chunkRange+1, 9.99)
			require.NoError(t, err)
			_, err = app.Append(0, l, int64(i+1)*chunkRange, 9.99)
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
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = 2 * 24 * time.Hour // Make sure that no newly created blocks are deleted.

	ingester, err := NewV2(ingesterCfg, clientCfg, overrides, nil, log.NewNopLogger())
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

func TestIngesterCacheUpdatesOnRefChange(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push a sample, verify that the labels are in ref-cache.
	// Compact the head to remove the labels from HEAD but they will still exist in ref-cache.
	// Push again to make the ref change and verify the refCache is updated in this case.

	pushSingleSampleAtTime(t, i, 10)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	startAppend := time.Now()
	l := labels.Labels{{Name: labels.MetricName, Value: "test"}}
	cachedRef, _, cachedRefExists := db.refCache.Ref(startAppend, l)
	require.True(t, cachedRefExists)
	require.Equal(t, uint64(1), cachedRef)

	// Compact to remove the series from HEAD.
	i.compactBlocks(context.Background(), true)
	cachedRef, _, cachedRefExists = db.refCache.Ref(startAppend, l)
	require.True(t, cachedRefExists)
	require.Equal(t, uint64(1), cachedRef)

	// New sample to create a new ref.
	pushSingleSampleAtTime(t, i, 11)
	cachedRef, _, cachedRefExists = db.refCache.Ref(startAppend, l)
	require.True(t, cachedRefExists)
	require.Equal(t, uint64(2), cachedRef)
}

func TestIngester_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig()
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}

func TestIngesterNotDeleteUnshippedBlocks(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig()
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = time.Millisecond // Which means delete all but first block.
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.Nil(t, db.Compact())

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Saying that we have shipped the second block, so only that should get deleted.
	require.Nil(t, shipper.WriteMetaFile(nil, db.db.Dir(), &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact())

	// Only the second block should be gone along with a new block.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[0].Meta().ULID, newBlocks[0].Meta().ULID)    // First block remains same.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID)    // 3rd block becomes 2nd now.
	require.NotEqual(t, oldBlocks[1].Meta().ULID, newBlocks[2].Meta().ULID) // The new block won't match previous 2nd block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Shipping 2 more blocks, hence all the blocks from first round.
	require.Nil(t, shipper.WriteMetaFile(nil, db.db.Dir(), &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID, newBlocks[0].Meta().ULID, newBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(6); j < 7; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.v2Push(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact())

	// All blocks from the old blocks should be gone now.
	newBlocks2 := db.Blocks()
	require.Equal(t, 2, len(newBlocks2))

	require.Equal(t, newBlocks[2].Meta().ULID, newBlocks2[0].Meta().ULID) // Block created in last round.
	for _, b := range oldBlocks {
		// Second block is not one among old blocks.
		require.NotEqual(t, b.Meta().ULID, newBlocks2[1].Meta().ULID)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks2[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))
}

func TestIngesterPushErrorDuringForcedCompaction(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push a sample, it should succeed.
	pushSingleSampleWithMetadata(t, i)

	// We mock a flushing by setting the boolean.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.casState(active, forceCompacting))

	// Ingestion should fail with a 503.
	req, _, _, _ := mockWriteRequest(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = i.v2Push(ctx, req)
	require.Equal(t, httpgrpc.Errorf(http.StatusServiceUnavailable, wrapWithUser(errors.New("forced compaction in progress"), userID).Error()), err)

	// Ingestion is successful after a flush.
	require.True(t, db.casState(forceCompacting, active))
	pushSingleSampleWithMetadata(t, i)
}

func TestIngesterNoFlushWithInFlightRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(), registry)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push few samples.
	for j := 0; j < 5; j++ {
		pushSingleSampleWithMetadata(t, i)
	}

	// Verifying that compaction won't happen when a request is in flight.

	// This mocks a request in flight.
	db := i.getTSDB(userID)
	require.NoError(t, db.acquireAppendLock())

	// Flush handler only triggers compactions, but doesn't wait for them to finish.
	i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush", nil))

	// Flushing should not have succeeded even after 5 seconds.
	time.Sleep(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// No requests in flight after this.
	db.releaseAppendLock()

	// Let's wait until all head series have been flushed.
	test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
		db := i.getTSDB(userID)
		if db == nil {
			return false
		}
		return db.Head().NumSeries()
	})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 1
	`), "cortex_ingester_tsdb_compactions_total"))
}
