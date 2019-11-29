package ingester

import (
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
				Matchers:         []*client.LabelMatcher{{client.REGEX_MATCH, labels.MetricName, ".*"}},
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

	/*

		require.Equal(t, ring.PENDING, i.lifecycler.GetState())

		// Mock request
		userID := "test"
		ctx := user.InjectOrgID(context.Background(), userID)
		req := &client.WriteRequest{}

		res, err := i.v2Push(ctx, req)
		assert.Equal(t, nil, err)
		assert.Nil(t, res)

		// Check if the TSDB has been created
		_, tsdbCreated := i.TSDBState.dbs[userID]
		assert.False(t, tsdbCreated)
	*/
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
