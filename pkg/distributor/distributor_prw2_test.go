package distributor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/cortexpbv2"
	"github.com/cortexproject/cortex/pkg/ingester"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	ring_client "github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/tenant"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/limiter"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	emptyResponseV2 = &cortexpbv2.WriteResponse{}
)

func TestDistributorPRW2_Push_LabelRemoval_RemovingNameLabelWillError(t *testing.T) {
	t.Parallel()
	ctx := user.InjectOrgID(context.Background(), "user")
	type testcase struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		removeReplica  bool
		removeLabels   []string
	}

	tc := testcase{
		removeReplica: true,
		removeLabels:  []string{"__name__"},
		inputSeries: labels.Labels{
			{Name: "__name__", Value: "some_metric"},
			{Name: "cluster", Value: "one"},
			{Name: "__replica__", Value: "two"},
		},
		expectedSeries: labels.Labels{},
	}

	var err error
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.DropLabels = tc.removeLabels
	limits.AcceptHASamples = tc.removeReplica

	ds, _, _, _ := prepare(t, prepConfig{
		numIngesters:     2,
		happyIngesters:   2,
		numDistributors:  1,
		shardByAllLabels: true,
		limits:           &limits,
	})

	// Push the series to the distributor
	req := mockWriteRequestV2([]labels.Labels{tc.inputSeries}, 1, 1, false)
	_, err = ds[0].PushV2(ctx, req)
	require.Error(t, err)
	assert.Equal(t, "rpc error: code = Code(400) desc = sample missing metric name", err.Error())
}

func TestDistributorPRW2_Push_LabelRemoval(t *testing.T) {
	t.Parallel()
	ctx := user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		removeReplica  bool
		removeLabels   []string
		exemplars      []cortexpbv2.Exemplar
	}

	cases := []testcase{
		// Remove both cluster and replica label.
		{
			removeReplica: true,
			removeLabels:  []string{"cluster"},
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
			},
		},
		// Remove multiple labels and replica.
		{
			removeReplica: true,
			removeLabels:  []string{"foo", "some"},
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
				{Name: "foo", Value: "bar"},
				{Name: "some", Value: "thing"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
			},
		},
		// Don't remove any labels.
		{
			removeReplica: false,
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "__replica__", Value: "two"},
				{Name: "cluster", Value: "one"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "__replica__", Value: "two"},
				{Name: "cluster", Value: "one"},
			},
		},
		// No labels left.
		{
			removeReplica: true,
			removeLabels:  []string{"cluster"},
			inputSeries: labels.Labels{
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			expectedSeries: labels.Labels{},
			exemplars: []cortexpbv2.Exemplar{
				{LabelsRefs: []uint32{1, 2}, Value: 1, Timestamp: 0},
				{LabelsRefs: []uint32{1, 2}, Value: 1, Timestamp: 0},
			},
		},
	}

	for _, tc := range cases {
		for _, histogram := range []bool{true, false} {
			var err error
			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.DropLabels = tc.removeLabels
			limits.AcceptHASamples = tc.removeReplica

			expectedDiscardedSamples := 0
			expectedDiscardedExemplars := 0
			if tc.expectedSeries.Len() == 0 {
				expectedDiscardedSamples = 1
				expectedDiscardedExemplars = len(tc.exemplars)
				// Allow series with no labels to ingest
				limits.EnforceMetricName = false
			}

			ds, ingesters, _, _ := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shardByAllLabels: true,
				limits:           &limits,
			})

			// Push the series to the distributor
			req := mockWriteRequestV2([]labels.Labels{tc.inputSeries}, 1, 1, histogram)
			req.Timeseries[0].Exemplars = tc.exemplars
			_, err = ds[0].PushV2(ctx, req)
			require.NoError(t, err)

			actualDiscardedSamples := testutil.ToFloat64(ds[0].validateMetrics.DiscardedSamples.WithLabelValues(validation.DroppedByUserConfigurationOverride, "user"))
			actualDiscardedExemplars := testutil.ToFloat64(ds[0].validateMetrics.DiscardedExemplars.WithLabelValues(validation.DroppedByUserConfigurationOverride, "user"))
			require.Equal(t, float64(expectedDiscardedSamples), actualDiscardedSamples)
			require.Equal(t, float64(expectedDiscardedExemplars), actualDiscardedExemplars)

			// Since each test pushes only 1 series, we do expect the ingester
			// to have received exactly 1 series
			for i := range ingesters {
				timeseries := ingesters[i].series()
				expectedSeries := 1
				if tc.expectedSeries.Len() == 0 {
					expectedSeries = 0
				}
				assert.Equal(t, expectedSeries, len(timeseries))
				for _, v := range timeseries {
					assert.Equal(t, tc.expectedSeries, cortexpb.FromLabelAdaptersToLabels(v.Labels))
				}
			}
		}
	}
}

func TestDistributorPRW2_PushHAInstances(t *testing.T) {
	t.Parallel()
	ctx := user.InjectOrgID(context.Background(), "user")

	for i, tc := range []struct {
		enableTracker    bool
		acceptedReplica  string
		testReplica      string
		cluster          string
		samples          int
		expectedResponse *cortexpbv2.WriteResponse
		expectedCode     int32
	}{
		{
			enableTracker:    true,
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponseV2,
		},
		// The 202 indicates that we didn't accept this sample.
		{
			enableTracker:   true,
			acceptedReplica: "instance2",
			testReplica:     "instance0",
			cluster:         "cluster0",
			samples:         5,
			expectedCode:    202,
		},
		// If the HA tracker is disabled we should still accept samples that have both labels.
		{
			enableTracker:    false,
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponseV2,
		},
		// Using very long replica label value results in validation error.
		{
			enableTracker:    true,
			acceptedReplica:  "instance0",
			testReplica:      "instance1234567890123456789012345678901234567890",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponseV2,
			expectedCode:     400,
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			tc := tc
			shardByAllLabels := shardByAllLabels
			for _, enableHistogram := range []bool{true, false} {
				enableHistogram := enableHistogram
				t.Run(fmt.Sprintf("[%d](shardByAllLabels=%v, histogram=%v)", i, shardByAllLabels, enableHistogram), func(t *testing.T) {
					t.Parallel()
					var limits validation.Limits
					flagext.DefaultValues(&limits)
					limits.AcceptHASamples = true
					limits.MaxLabelValueLength = 15

					ds, _, _, _ := prepare(t, prepConfig{
						numIngesters:     3,
						happyIngesters:   3,
						numDistributors:  1,
						shardByAllLabels: shardByAllLabels,
						limits:           &limits,
						enableTracker:    tc.enableTracker,
					})

					d := ds[0]

					userID, err := tenant.TenantID(ctx)
					assert.NoError(t, err)
					err = d.HATracker.CheckReplica(ctx, userID, tc.cluster, tc.acceptedReplica, time.Now())
					assert.NoError(t, err)

					request := makeWriteRequestHAV2(tc.samples, tc.testReplica, tc.cluster, enableHistogram)
					response, err := d.PushV2(ctx, request)
					assert.Equal(t, tc.expectedResponse, response)

					httpResp, ok := httpgrpc.HTTPResponseFromError(err)
					if ok {
						assert.Equal(t, tc.expectedCode, httpResp.Code)
					} else if tc.expectedCode != 0 {
						assert.Fail(t, "expected HTTP status code", tc.expectedCode)
					}
				})
			}
		}
	}
}

func BenchmarkDistributorPRW2_Push(b *testing.B) {
	const (
		numSeriesPerRequest = 1000
	)
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		prepareConfig func(limits *validation.Limits)
		prepareSeries func() ([]labels.Labels, []cortexpbv2.Sample)
		expectedErr   string
	}{
		"all samples successfully pushed": {
			prepareConfig: func(limits *validation.Limits) {},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "",
		},
		"ingestion rate limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.IngestionRate = 1
				limits.IngestionBurstSize = 1
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "ingestion rate limit",
		},
		"too many labels limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelNamesPerSeries = 30
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 1; i < 31; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "series has too many labels",
		},
		"max label name length limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelNameLength = 1024
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					// Add a label with a very long name.
					lbls.Set(fmt.Sprintf("xxx_%0.2000d", 1), "xxx")

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "label name too long",
		},
		"max label value length limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelValueLength = 1024
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					// Add a label with a very long value.
					lbls.Set("xxx", fmt.Sprintf("xxx_%0.2000d", 1))

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "label value too long",
		},
		"max label size bytes per series limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelsSizeBytes = 1024
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					// Add a label with a very long value.
					lbls.Set("xxx", fmt.Sprintf("xxx_%0.2000d", 1))

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "labels size bytes exceeded",
		},
		"timestamp too old": {
			prepareConfig: func(limits *validation.Limits) {
				limits.RejectOldSamples = true
				limits.RejectOldSamplesMaxAge = model.Duration(time.Hour)
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().Add(-2*time.Hour).UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "timestamp too old",
		},
		"timestamp too new": {
			prepareConfig: func(limits *validation.Limits) {
				limits.CreationGracePeriod = model.Duration(time.Minute)
			},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().Add(time.Hour).UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "timestamp too new",
		},
	}

	tg := ring.NewRandomTokenGenerator()

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {

			// Create an in-memory KV store for the ring with 1 ingester registered.
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			b.Cleanup(func() { assert.NoError(b, closer.Close()) })

			err := kvStore.CAS(context.Background(), ingester.RingKey,
				func(_ interface{}) (interface{}, bool, error) {
					d := &ring.Desc{}
					d.AddIngester("ingester-1", "127.0.0.1", "", tg.GenerateTokens(d, "ingester-1", "", 128, true), ring.ACTIVE, time.Now())
					return d, true, nil
				},
			)
			require.NoError(b, err)

			ingestersRing, err := ring.New(ring.Config{
				KVStore:           kv.Config{Mock: kvStore},
				HeartbeatTimeout:  60 * time.Minute,
				ReplicationFactor: 1,
			}, ingester.RingKey, ingester.RingKey, nil, nil)
			require.NoError(b, err)
			require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingestersRing))
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ingestersRing))
			})

			test.Poll(b, time.Second, 1, func() interface{} {
				return ingestersRing.InstancesCount()
			})

			// Prepare the distributor configuration.
			var distributorCfg Config
			var clientConfig client.Config
			limits := validation.Limits{}
			flagext.DefaultValues(&distributorCfg, &clientConfig, &limits)

			limits.IngestionRate = 10000000 // Unlimited.
			testData.prepareConfig(&limits)

			distributorCfg.ShardByAllLabels = true
			distributorCfg.IngesterClientFactory = func(addr string) (ring_client.PoolClient, error) {
				return &noopIngester{}, nil
			}

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(b, err)

			// Start the distributor.
			distributor, err := New(distributorCfg, clientConfig, overrides, ingestersRing, true, prometheus.NewRegistry(), log.NewNopLogger())
			require.NoError(b, err)
			require.NoError(b, services.StartAndAwaitRunning(context.Background(), distributor))

			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(context.Background(), distributor))
			})

			// Prepare the series to remote write before starting the benchmark.
			metrics, samples := testData.prepareSeries()

			// Run the benchmark.
			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, err := distributor.PushV2(ctx, cortexpbv2.ToWriteRequestV2(metrics, samples, nil, nil, cortexpbv2.API))
				if testData.expectedErr == "" && err != nil {
					b.Fatalf("no error expected but got %v", err)
				}
				if testData.expectedErr != "" && (err == nil || !strings.Contains(err.Error(), testData.expectedErr)) {
					b.Fatalf("expected %v error but got %v", testData.expectedErr, err)
				}
			}
		})
	}
}

func TestDistributorPRW2_Push(t *testing.T) {
	t.Parallel()
	// Metrics to assert on.
	lastSeenTimestamp := "cortex_distributor_latest_seen_sample_timestamp_seconds"
	distributorAppend := "cortex_distributor_ingester_appends_total"
	distributorAppendFailure := "cortex_distributor_ingester_append_failures_total"
	distributorReceivedSamples := "cortex_distributor_received_samples_total"
	ctx := user.InjectOrgID(context.Background(), "userDistributorPush")

	type samplesIn struct {
		num              int
		startTimestampMs int64
	}
	for name, tc := range map[string]struct {
		metricNames      []string
		numIngesters     int
		happyIngesters   int
		samples          samplesIn
		histogramSamples bool
		metadata         int
		expectedResponse *cortexpbv2.WriteResponse
		expectedError    error
		expectedMetrics  string
		ingesterError    error
	}{
		"A push of no samples shouldn't block or return error, even if ingesters are sad": {
			numIngesters:     3,
			happyIngesters:   0,
			expectedResponse: emptyResponseV2,
		},
		"A push to 3 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: emptyResponseV2,
			metricNames:      []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.004
			`,
		},
		"A push to 2 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: emptyResponseV2,
			metricNames:      []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.004
			`,
		},
		"A push to 1 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 1,
			samples:        samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedError:  errFail,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.009
			`,
		},
		"A push to 0 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 0,
			samples:        samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedError:  errFail,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.009
			`,
		},
		"A push exceeding burst size should fail": {
			numIngesters:   3,
			happyIngesters: 3,
			samples:        samplesIn{num: 25, startTimestampMs: 123456789000},
			metadata:       5,
			expectedError:  httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (20) exceeded while adding 25 samples and 5 metadata"),
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.024
			`,
		},
		"A push to ingesters should report the correct metrics with no metadata": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 1, startTimestampMs: 123456789000},
			metadata:         0,
			metricNames:      []string{distributorAppend, distributorAppendFailure},
			expectedResponse: emptyResponseV2,
			expectedMetrics: `
				# HELP cortex_distributor_ingester_append_failures_total The total number of failed batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_append_failures_total counter
				cortex_distributor_ingester_append_failures_total{ingester="ingester-2",status="5xx",type="samples"} 1
				# HELP cortex_distributor_ingester_appends_total The total number of batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_appends_total counter
				cortex_distributor_ingester_appends_total{ingester="ingester-0",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-1",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-2",type="samples"} 1
			`,
		},
		"A push to ingesters should report samples and metadata metrics with no samples": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 0, startTimestampMs: 123456789000},
			metadata:         1,
			metricNames:      []string{distributorAppend, distributorAppendFailure},
			expectedResponse: emptyResponseV2,
			ingesterError:    httpgrpc.Errorf(http.StatusInternalServerError, "Fail"),
			expectedMetrics: `
				# HELP cortex_distributor_ingester_append_failures_total The total number of failed batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_append_failures_total counter
				cortex_distributor_ingester_append_failures_total{ingester="ingester-2",status="5xx",type="metadata"} 1
				cortex_distributor_ingester_append_failures_total{ingester="ingester-2",status="5xx",type="samples"} 1
				# HELP cortex_distributor_ingester_appends_total The total number of batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_appends_total counter
				cortex_distributor_ingester_appends_total{ingester="ingester-0",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-1",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-2",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-0",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-1",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-2",type="samples"} 1
			`,
		},
		"A push to overloaded ingesters should report the correct metrics": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 0, startTimestampMs: 123456789000},
			metadata:         1,
			metricNames:      []string{distributorAppend, distributorAppendFailure},
			expectedResponse: emptyResponseV2,
			ingesterError:    httpgrpc.Errorf(http.StatusTooManyRequests, "Fail"),
			expectedMetrics: `
				# HELP cortex_distributor_ingester_appends_total The total number of batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_appends_total counter
				cortex_distributor_ingester_appends_total{ingester="ingester-0",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-1",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-2",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-0",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-1",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="ingester-2",type="samples"} 1
				# HELP cortex_distributor_ingester_append_failures_total The total number of failed batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_append_failures_total counter
				cortex_distributor_ingester_append_failures_total{ingester="ingester-2",status="4xx",type="metadata"} 1
				cortex_distributor_ingester_append_failures_total{ingester="ingester-2",status="4xx",type="samples"} 1
			`,
		},
		"A push to 3 happy ingesters should succeed, histograms": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			histogramSamples: true,
			metadata:         5,
			expectedResponse: emptyResponseV2,
			metricNames:      []string{lastSeenTimestamp, distributorReceivedSamples},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.004
				# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
				# TYPE cortex_distributor_received_samples_total counter
				cortex_distributor_received_samples_total{type="float",user="userDistributorPush"} 0
				cortex_distributor_received_samples_total{type="histogram",user="userDistributorPush"} 5
			`,
		},
		"A push to 2 happy ingesters should succeed, histograms": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			histogramSamples: true,
			metadata:         5,
			expectedResponse: emptyResponseV2,
			metricNames:      []string{lastSeenTimestamp, distributorReceivedSamples},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.004
				# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
				# TYPE cortex_distributor_received_samples_total counter
				cortex_distributor_received_samples_total{type="float",user="userDistributorPush"} 0
				cortex_distributor_received_samples_total{type="histogram",user="userDistributorPush"} 5
			`,
		},
		"A push to 1 happy ingesters should fail, histograms": {
			numIngesters:     3,
			happyIngesters:   1,
			samples:          samplesIn{num: 10, startTimestampMs: 123456789000},
			histogramSamples: true,
			expectedError:    errFail,
			metricNames:      []string{lastSeenTimestamp, distributorReceivedSamples},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.009
				# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
				# TYPE cortex_distributor_received_samples_total counter
				cortex_distributor_received_samples_total{type="float",user="userDistributorPush"} 0
				cortex_distributor_received_samples_total{type="histogram",user="userDistributorPush"} 10
			`,
		},
		"A push exceeding burst size should fail, histograms": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          samplesIn{num: 25, startTimestampMs: 123456789000},
			histogramSamples: true,
			metadata:         5,
			expectedError:    httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (20) exceeded while adding 25 samples and 5 metadata"),
			metricNames:      []string{lastSeenTimestamp, distributorReceivedSamples},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="userDistributorPush"} 123456789.024
				# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
				# TYPE cortex_distributor_received_samples_total counter
				cortex_distributor_received_samples_total{type="float",user="userDistributorPush"} 0
				cortex_distributor_received_samples_total{type="histogram",user="userDistributorPush"} 25
			`,
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			tc := tc
			name := name
			shardByAllLabels := shardByAllLabels
			t.Run(fmt.Sprintf("[%s](shardByAllLabels=%v)", name, shardByAllLabels), func(t *testing.T) {
				t.Parallel()
				limits := &validation.Limits{}
				flagext.DefaultValues(limits)
				limits.IngestionRate = 20
				limits.IngestionBurstSize = 20

				ds, _, regs, _ := prepare(t, prepConfig{
					numIngesters:     tc.numIngesters,
					happyIngesters:   tc.happyIngesters,
					numDistributors:  1,
					shardByAllLabels: shardByAllLabels,
					limits:           limits,
					errFail:          tc.ingesterError,
				})

				var request *cortexpbv2.WriteRequest
				if !tc.histogramSamples {
					request = makeWriteRequestV2WithSamples(tc.samples.startTimestampMs, tc.samples.num, tc.metadata)
				} else {
					request = makeWriteRequestV2WithHistogram(tc.samples.startTimestampMs, tc.samples.num, tc.metadata)
				}

				response, err := ds[0].PushV2(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)
				assert.Equal(t, status.Code(tc.expectedError), status.Code(err))

				// Check tracked Prometheus metrics. Since the Push() response is sent as soon as the quorum
				// is reached, when we reach this point the 3rd ingester may not have received series/metadata
				// yet. To avoid flaky test we retry metrics assertion until we hit the desired state (no error)
				// within a reasonable timeout.
				if tc.expectedMetrics != "" {
					test.Poll(t, time.Second, nil, func() interface{} {
						return testutil.GatherAndCompare(regs[0], strings.NewReader(tc.expectedMetrics), tc.metricNames...)
					})
				}
			})
		}
	}
}

func TestDistributorPRW2_PushIngestionRateLimiter(t *testing.T) {
	t.Parallel()
	type testPush struct {
		samples       int
		metadata      int
		expectedError error
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		distributors          int
		ingestionRateStrategy string
		ingestionRate         float64
		ingestionBurstSize    int
		pushes                []testPush
	}{
		"local strategy: limit should be set to each distributor": {
			distributors:          2,
			ingestionRateStrategy: validation.LocalIngestionRateStrategy,
			ingestionRate:         10,
			ingestionBurstSize:    10,
			pushes: []testPush{
				{samples: 4, expectedError: nil},
				{metadata: 1, expectedError: nil},
				{samples: 6, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (10) exceeded while adding 6 samples and 0 metadata")},
				{samples: 4, metadata: 1, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (10) exceeded while adding 1 samples and 0 metadata")},
				{metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (10) exceeded while adding 0 samples and 1 metadata")},
			},
		},
		"global strategy: limit should be evenly shared across distributors": {
			distributors:          2,
			ingestionRateStrategy: validation.GlobalIngestionRateStrategy,
			ingestionRate:         10,
			ingestionBurstSize:    5,
			pushes: []testPush{
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: nil},
				{samples: 2, metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 2 samples and 1 metadata")},
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 1 samples and 0 metadata")},
				{metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 0 samples and 1 metadata")},
			},
		},
		"global strategy: burst should set to each distributor": {
			distributors:          2,
			ingestionRateStrategy: validation.GlobalIngestionRateStrategy,
			ingestionRate:         10,
			ingestionBurstSize:    20,
			pushes: []testPush{
				{samples: 10, expectedError: nil},
				{samples: 5, expectedError: nil},
				{samples: 5, metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 5 samples and 1 metadata")},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 1 samples and 0 metadata")},
				{metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 0 samples and 1 metadata")},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		for _, enableHistogram := range []bool{false, true} {
			enableHistogram := enableHistogram
			t.Run(fmt.Sprintf("%s, histogram=%s", testName, strconv.FormatBool(enableHistogram)), func(t *testing.T) {
				t.Parallel()
				limits := &validation.Limits{}
				flagext.DefaultValues(limits)
				limits.IngestionRateStrategy = testData.ingestionRateStrategy
				limits.IngestionRate = testData.ingestionRate
				limits.IngestionBurstSize = testData.ingestionBurstSize

				// Start all expected distributors
				distributors, _, _, _ := prepare(t, prepConfig{
					numIngesters:     3,
					happyIngesters:   3,
					numDistributors:  testData.distributors,
					shardByAllLabels: true,
					limits:           limits,
				})

				// Push samples in multiple requests to the first distributor
				for _, push := range testData.pushes {
					var request *cortexpbv2.WriteRequest
					if !enableHistogram {
						request = makeWriteRequestV2WithSamples(0, push.samples, push.metadata)
					} else {
						request = makeWriteRequestV2WithHistogram(0, push.samples, push.metadata)
					}
					response, err := distributors[0].PushV2(ctx, request)

					if push.expectedError == nil {
						assert.Equal(t, emptyResponseV2, response)
						assert.Nil(t, err)
					} else {
						assert.Nil(t, response)
						assert.Equal(t, push.expectedError, err)
					}
				}
			})
		}
	}
}

func TestPushPRW2_QuorumError(t *testing.T) {
	t.Parallel()

	var limits validation.Limits
	flagext.DefaultValues(&limits)

	limits.IngestionRate = math.MaxFloat64

	dists, ingesters, _, r := prepare(t, prepConfig{
		numDistributors:     1,
		numIngesters:        3,
		happyIngesters:      0,
		shuffleShardSize:    3,
		shardByAllLabels:    true,
		shuffleShardEnabled: true,
		limits:              &limits,
	})

	ctx := user.InjectOrgID(context.Background(), "user")

	d := dists[0]

	// we should run several write request to make sure we dont have any race condition on the batchTracker#record code
	numberOfWrites := 10000

	// Using 429 just to make sure we are not hitting the &limits
	// Simulating 2 4xx and 1 5xx -> Should return 4xx
	ingesters[0].failResp.Store(httpgrpc.Errorf(429, "Throttling"))
	ingesters[1].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))
	ingesters[2].failResp.Store(httpgrpc.Errorf(429, "Throttling"))

	for i := 0; i < numberOfWrites; i++ {
		request := makeWriteRequestV2WithSamples(0, 30, 20)
		_, err := d.PushV2(ctx, request)
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(429), status.Code())
	}

	// Simulating 2 5xx and 1 4xx -> Should return 5xx
	ingesters[0].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))
	ingesters[1].failResp.Store(httpgrpc.Errorf(429, "Throttling"))
	ingesters[2].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))

	for i := 0; i < numberOfWrites; i++ {
		request := makeWriteRequestV2WithSamples(0, 300, 200)
		_, err := d.PushV2(ctx, request)
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(500), status.Code())
	}

	// Simulating 2 different errors and 1 success -> This case we may return any of the errors
	ingesters[0].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))
	ingesters[1].failResp.Store(httpgrpc.Errorf(429, "Throttling"))
	ingesters[2].happy.Store(true)

	for i := 0; i < numberOfWrites; i++ {
		request := makeWriteRequestV2WithSamples(0, 30, 20)
		_, err := d.PushV2(ctx, request)
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(429), status.Code())
	}

	// Simulating 1 error -> Should return 2xx
	ingesters[0].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))
	ingesters[1].happy.Store(true)
	ingesters[2].happy.Store(true)

	for i := 0; i < 1; i++ {
		request := makeWriteRequestV2WithSamples(0, 30, 20)
		_, err := d.PushV2(ctx, request)
		require.NoError(t, err)
	}

	// Simulating an unhealthy ingester (ingester 2)
	ingesters[0].failResp.Store(httpgrpc.Errorf(500, "InternalServerError"))
	ingesters[1].happy.Store(true)
	ingesters[2].happy.Store(true)

	err := r.KVClient.CAS(context.Background(), ingester.RingKey, func(in interface{}) (interface{}, bool, error) {
		r := in.(*ring.Desc)
		ingester2 := r.Ingesters["ingester-2"]
		ingester2.State = ring.LEFT
		ingester2.Timestamp = time.Now().Unix()
		r.Ingesters["ingester-2"] = ingester2
		return in, true, nil
	})

	require.NoError(t, err)

	// Give time to the ring get updated with the KV value
	test.Poll(t, 15*time.Second, true, func() interface{} {
		replicationSet, _ := r.GetAllHealthy(ring.Read)
		return len(replicationSet.Instances) == 2
	})

	for i := 0; i < numberOfWrites; i++ {
		request := makeWriteRequestV2WithSamples(0, 30, 20)
		_, err := d.PushV2(ctx, request)
		require.Error(t, err)
		status, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.Code(500), status.Code())
	}
}

func TestDistributorPRW2_PushInstanceLimits(t *testing.T) {
	t.Parallel()

	type testPush struct {
		samples       int
		metadata      int
		expectedError error
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		preInflight    int
		preRateSamples int        // initial rate before first push
		pushes         []testPush // rate is recomputed after each push

		// limits
		inflightLimit      int
		ingestionRateLimit float64

		metricNames     []string
		expectedMetrics string
	}{
		"no limits limit": {
			preInflight:    100,
			preRateSamples: 1000,

			pushes: []testPush{
				{samples: 100, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric},
			expectedMetrics: `
				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 0
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 0
			`,
		},
		"below inflight limit": {
			preInflight:   100,
			inflightLimit: 101,
			pushes: []testPush{
				{samples: 100, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric, "cortex_distributor_inflight_push_requests"},
			expectedMetrics: `
				# HELP cortex_distributor_inflight_push_requests Current number of inflight push requests in distributor.
				# TYPE cortex_distributor_inflight_push_requests gauge
				cortex_distributor_inflight_push_requests 100

				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 101
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 0
			`,
		},
		"hits inflight limit": {
			preInflight:   101,
			inflightLimit: 101,
			pushes: []testPush{
				{samples: 100, expectedError: errTooManyInflightPushRequests},
			},
		},
		"below ingestion rate limit": {
			preRateSamples:     500,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 1000, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric, "cortex_distributor_ingestion_rate_samples_per_second"},
			expectedMetrics: `
				# HELP cortex_distributor_ingestion_rate_samples_per_second Current ingestion rate in samples/sec that distributor is using to limit access.
				# TYPE cortex_distributor_ingestion_rate_samples_per_second gauge
				cortex_distributor_ingestion_rate_samples_per_second 600

				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 0
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 1000
			`,
		},
		"hits rate limit on first request, but second request can proceed": {
			preRateSamples:     1200,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 100, expectedError: errMaxSamplesPushRateLimitReached},
				{samples: 100, expectedError: nil},
			},
		},
		"below rate limit on first request, but hits the rate limit afterwards": {
			preRateSamples:     500,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 5000, expectedError: nil},                               // after push, rate = 500 + 0.2*(5000-500) = 1400
				{samples: 5000, expectedError: errMaxSamplesPushRateLimitReached}, // after push, rate = 1400 + 0.2*(0 - 1400) = 1120
				{samples: 5000, expectedError: errMaxSamplesPushRateLimitReached}, // after push, rate = 1120 + 0.2*(0 - 1120) = 896
				{samples: 5000, expectedError: nil},                               // 896 is below 1000, so this push succeeds, new rate = 896 + 0.2*(5000-896) = 1716.8
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		for _, enableHistogram := range []bool{true, false} {
			enableHistogram := enableHistogram
			t.Run(fmt.Sprintf("%s, histogram=%s", testName, strconv.FormatBool(enableHistogram)), func(t *testing.T) {
				t.Parallel()
				limits := &validation.Limits{}
				flagext.DefaultValues(limits)

				// Start all expected distributors
				distributors, _, regs, _ := prepare(t, prepConfig{
					numIngesters:        3,
					happyIngesters:      3,
					numDistributors:     1,
					shardByAllLabels:    true,
					limits:              limits,
					maxInflightRequests: testData.inflightLimit,
					maxIngestionRate:    testData.ingestionRateLimit,
				})

				d := distributors[0]
				d.inflightPushRequests.Add(int64(testData.preInflight))
				d.ingestionRate.Add(int64(testData.preRateSamples))

				d.ingestionRate.Tick()

				for _, push := range testData.pushes {
					var request *cortexpbv2.WriteRequest
					if enableHistogram {
						request = makeWriteRequestV2WithHistogram(0, push.samples, push.metadata)
					} else {
						request = makeWriteRequestV2WithSamples(0, push.samples, push.metadata)
					}
					_, err := d.PushV2(ctx, request)

					if push.expectedError == nil {
						assert.Nil(t, err)
					} else {
						assert.Equal(t, push.expectedError, err)
					}

					d.ingestionRate.Tick()

					if testData.expectedMetrics != "" {
						assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(testData.expectedMetrics), testData.metricNames...))
					}
				}
			})
		}
	}
}

func TestDistributorPRW2_PushQuery(t *testing.T) {
	t.Parallel()
	const shuffleShardSize = 5

	ctx := user.InjectOrgID(context.Background(), "user")
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	barMatcher := mustEqualMatcher("bar", "baz")

	type testcase struct {
		name                string
		numIngesters        int
		happyIngesters      int
		samples             int
		metadata            int
		matchers            []*labels.Matcher
		expectedIngesters   int
		expectedResponse    model.Matrix
		expectedError       error
		shardByAllLabels    bool
		shuffleShardEnabled bool
	}

	// We'll programmatically build the test cases now, as we want complete
	// coverage along quite a few different axis.
	testcases := []testcase{}

	// Run every test in both sharding modes.
	for _, shardByAllLabels := range []bool{true, false} {

		// Test with between 2 and 10 ingesters.
		for numIngesters := 2; numIngesters < 10; numIngesters++ {

			// Test with between 0 and numIngesters "happy" ingesters.
			for happyIngesters := 0; happyIngesters <= numIngesters; happyIngesters++ {

				// Test either with shuffle-sharding enabled or disabled.
				for _, shuffleShardEnabled := range []bool{false, true} {
					scenario := fmt.Sprintf("shardByAllLabels=%v, numIngester=%d, happyIngester=%d, shuffleSharding=%v)", shardByAllLabels, numIngesters, happyIngesters, shuffleShardEnabled)

					// The number of ingesters we expect to query depends whether shuffle sharding and/or
					// shard by all labels are enabled.
					var expectedIngesters int
					if shuffleShardEnabled {
						expectedIngesters = min(shuffleShardSize, numIngesters)
					} else if shardByAllLabels {
						expectedIngesters = numIngesters
					} else {
						expectedIngesters = 3 // Replication factor
					}

					// When we're not sharding by metric name, queriers with more than one
					// failed ingester should fail.
					if shardByAllLabels && numIngesters-happyIngesters > 1 {
						testcases = append(testcases, testcase{
							name:                fmt.Sprintf("ExpectFail(%s)", scenario),
							numIngesters:        numIngesters,
							happyIngesters:      happyIngesters,
							matchers:            []*labels.Matcher{nameMatcher, barMatcher},
							expectedError:       errFail,
							shardByAllLabels:    shardByAllLabels,
							shuffleShardEnabled: shuffleShardEnabled,
						})
						continue
					}

					// When we have less ingesters than replication factor, any failed ingester
					// will cause a failure.
					if numIngesters < 3 && happyIngesters < 2 {
						testcases = append(testcases, testcase{
							name:                fmt.Sprintf("ExpectFail(%s)", scenario),
							numIngesters:        numIngesters,
							happyIngesters:      happyIngesters,
							matchers:            []*labels.Matcher{nameMatcher, barMatcher},
							expectedError:       errFail,
							shardByAllLabels:    shardByAllLabels,
							shuffleShardEnabled: shuffleShardEnabled,
						})
						continue
					}

					// If we're sharding by metric name and we have failed ingesters, we can't
					// tell ahead of time if the query will succeed, as we don't know which
					// ingesters will hold the results for the query.
					if !shardByAllLabels && numIngesters-happyIngesters > 1 {
						continue
					}

					// Reading all the samples back should succeed.
					testcases = append(testcases, testcase{
						name:                fmt.Sprintf("ReadAll(%s)", scenario),
						numIngesters:        numIngesters,
						happyIngesters:      happyIngesters,
						samples:             10,
						matchers:            []*labels.Matcher{nameMatcher, barMatcher},
						expectedResponse:    expectedResponse(0, 10),
						expectedIngesters:   expectedIngesters,
						shardByAllLabels:    shardByAllLabels,
						shuffleShardEnabled: shuffleShardEnabled,
					})

					// As should reading none of the samples back.
					testcases = append(testcases, testcase{
						name:                fmt.Sprintf("ReadNone(%s)", scenario),
						numIngesters:        numIngesters,
						happyIngesters:      happyIngesters,
						samples:             10,
						matchers:            []*labels.Matcher{nameMatcher, mustEqualMatcher("not", "found")},
						expectedResponse:    expectedResponse(0, 0),
						expectedIngesters:   expectedIngesters,
						shardByAllLabels:    shardByAllLabels,
						shuffleShardEnabled: shuffleShardEnabled,
					})

					// And reading each sample individually.
					for i := 0; i < 10; i++ {
						testcases = append(testcases, testcase{
							name:                fmt.Sprintf("ReadOne(%s, sample=%d)", scenario, i),
							numIngesters:        numIngesters,
							happyIngesters:      happyIngesters,
							samples:             10,
							matchers:            []*labels.Matcher{nameMatcher, mustEqualMatcher("sample", strconv.Itoa(i))},
							expectedResponse:    expectedResponse(i, i+1),
							expectedIngesters:   expectedIngesters,
							shardByAllLabels:    shardByAllLabels,
							shuffleShardEnabled: shuffleShardEnabled,
						})
					}
				}
			}
		}
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ds, ingesters, _, _ := prepare(t, prepConfig{
				numIngesters:        tc.numIngesters,
				happyIngesters:      tc.happyIngesters,
				numDistributors:     1,
				shardByAllLabels:    tc.shardByAllLabels,
				shuffleShardEnabled: tc.shuffleShardEnabled,
				shuffleShardSize:    shuffleShardSize,
			})

			request := makeWriteRequestV2WithSamples(0, tc.samples, tc.metadata)
			writeResponse, err := ds[0].PushV2(ctx, request)
			assert.Equal(t, &cortexpbv2.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			var response model.Matrix
			series, err := ds[0].QueryStream(ctx, 0, 10, tc.matchers...)
			assert.Equal(t, tc.expectedError, err)

			if series == nil {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, nil)
			} else {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, series.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse.String(), response.String())

			// Check how many ingesters have been queried.
			// Due to the quorum the distributor could cancel the last request towards ingesters
			// if all other ones are successful, so we're good either has been queried X or X-1
			// ingesters.
			if tc.expectedError == nil {
				assert.Contains(t, []int{tc.expectedIngesters, tc.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "QueryStream"))
			}
		})
	}
}

func TestDistributorPRW2_QueryStream_ShouldReturnErrorIfMaxChunksPerQueryLimitIsReached(t *testing.T) {
	t.Parallel()
	const maxChunksLimit = 30 // Chunks are duplicated due to replication factor.

	for _, histogram := range []bool{true, false} {
		ctx := user.InjectOrgID(context.Background(), "user")
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		limits.MaxChunksPerQuery = maxChunksLimit

		// Prepare distributors.
		ds, _, _, _ := prepare(t, prepConfig{
			numIngesters:     3,
			happyIngesters:   3,
			numDistributors:  1,
			shardByAllLabels: true,
			limits:           limits,
		})

		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, maxChunksLimit, 0))

		// Push a number of series below the max chunks limit. Each series has 1 sample,
		// so expect 1 chunk per series when querying back.
		initialSeries := maxChunksLimit / 3
		var writeReqV2 *cortexpbv2.WriteRequest
		if histogram {
			writeReqV2 = makeWriteRequestV2WithHistogram(0, initialSeries, 0)
		} else {
			writeReqV2 = makeWriteRequestV2WithSamples(0, initialSeries, 0)
		}

		writeRes, err := ds[0].PushV2(ctx, writeReqV2)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		allSeriesMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
		}

		// Since the number of series (and thus chunks) is equal to the limit (but doesn't
		// exceed it), we expect a query running on all series to succeed.
		queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)
		assert.Len(t, queryRes.Chunkseries, initialSeries)

		// Push more series to exceed the limit once we'll query back all series.

		for i := 0; i < maxChunksLimit; i++ {
			writeReq := &cortexpbv2.WriteRequest{}
			writeReq.Symbols = []string{"", "__name__", fmt.Sprintf("another_series_%d", i)}
			writeReq.Timeseries = append(writeReq.Timeseries,
				makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: fmt.Sprintf("another_series_%d", i)}}, 0, 0, histogram, false),
			)
			writeRes, err := ds[0].PushV2(ctx, writeReq)
			assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
			assert.Nil(t, err)
		}

		// Since the number of series (and thus chunks) is exceeding to the limit, we expect
		// a query running on all series to fail.
		_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "the query hit the max number of chunks limit")
	}
}

func TestDistributorPRW2_QueryStream_ShouldReturnErrorIfMaxSeriesPerQueryLimitIsReached(t *testing.T) {
	t.Parallel()
	const maxSeriesLimit = 10

	for _, histogram := range []bool{true, false} {
		ctx := user.InjectOrgID(context.Background(), "user")
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(maxSeriesLimit, 0, 0, 0))

		// Prepare distributors.
		ds, _, _, _ := prepare(t, prepConfig{
			numIngesters:     3,
			happyIngesters:   3,
			numDistributors:  1,
			shardByAllLabels: true,
			limits:           limits,
		})

		// Push a number of series below the max series limit.
		initialSeries := maxSeriesLimit
		var writeReqV2 *cortexpbv2.WriteRequest
		if histogram {
			writeReqV2 = makeWriteRequestV2WithHistogram(0, initialSeries, 0)
		} else {
			writeReqV2 = makeWriteRequestV2WithSamples(0, initialSeries, 0)
		}

		writeRes, err := ds[0].PushV2(ctx, writeReqV2)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		allSeriesMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
		}

		// Since the number of series is equal to the limit (but doesn't
		// exceed it), we expect a query running on all series to succeed.
		queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)
		assert.Len(t, queryRes.Chunkseries, initialSeries)

		// Push more series to exceed the limit once we'll query back all series.
		writeReq := &cortexpbv2.WriteRequest{}
		writeReq.Symbols = []string{"", "__name__", "another_series"}
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0, histogram, false),
		)

		writeRes, err = ds[0].PushV2(ctx, writeReq)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		// Since the number of series is exceeding the limit, we expect
		// a query running on all series to fail.
		_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "max number of series limit")
	}
}

func TestDistributorPRW2_QueryStream_ShouldReturnErrorIfMaxChunkBytesPerQueryLimitIsReached(t *testing.T) {
	t.Parallel()
	const seriesToAdd = 10

	for _, histogram := range []bool{true, false} {
		ctx := user.InjectOrgID(context.Background(), "user")
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)

		// Prepare distributors.
		// Use replication factor of 2 to always read all the chunks from both ingesters,
		// this guarantees us to always read the same chunks and have a stable test.
		ds, _, _, _ := prepare(t, prepConfig{
			numIngesters:      2,
			happyIngesters:    2,
			numDistributors:   1,
			shardByAllLabels:  true,
			limits:            limits,
			replicationFactor: 2,
		})

		allSeriesMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
		}
		// Push a single series to allow us to calculate the chunk size to calculate the limit for the test.
		writeReq := &cortexpbv2.WriteRequest{}
		writeReq.Symbols = []string{"", "__name__", "another_series"}
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0, histogram, false),
		)
		writeRes, err := ds[0].PushV2(ctx, writeReq)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)
		chunkSizeResponse, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)

		// Use the resulting chunks size to calculate the limit as (series to add + our test series) * the response chunk size.
		var responseChunkSize = chunkSizeResponse.ChunksSize()
		var maxBytesLimit = (seriesToAdd) * responseChunkSize

		// Update the limiter with the calculated limits.
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, maxBytesLimit, 0, 0))

		// Push a number of series below the max chunk bytes limit. Subtract one for the series added above.
		var writeReqV2 *cortexpbv2.WriteRequest
		if histogram {
			writeReqV2 = makeWriteRequestV2WithHistogram(0, seriesToAdd-1, 0)
		} else {
			writeReqV2 = makeWriteRequestV2WithSamples(0, seriesToAdd-1, 0)
		}

		writeRes, err = ds[0].PushV2(ctx, writeReqV2)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		// Since the number of chunk bytes is equal to the limit (but doesn't
		// exceed it), we expect a query running on all series to succeed.
		queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)
		assert.Len(t, queryRes.Chunkseries, seriesToAdd)

		// Push another series to exceed the chunk bytes limit once we'll query back all series.
		writeReq = &cortexpbv2.WriteRequest{}
		writeReq.Symbols = []string{"", "__name__", "another_series_1"}
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series_1"}}, 0, 0, histogram, false),
		)

		writeRes, err = ds[0].PushV2(ctx, writeReq)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		// Since the aggregated chunk size is exceeding the limit, we expect
		// a query running on all series to fail.
		_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.Error(t, err)
		assert.Equal(t, err, validation.LimitError(fmt.Sprintf(limiter.ErrMaxChunkBytesHit, maxBytesLimit)))
	}
}

func TestDistributorPRW2_QueryStream_ShouldReturnErrorIfMaxDataBytesPerQueryLimitIsReached(t *testing.T) {
	t.Parallel()
	const seriesToAdd = 10

	for _, histogram := range []bool{true, false} {
		ctx := user.InjectOrgID(context.Background(), "user")
		limits := &validation.Limits{}
		flagext.DefaultValues(limits)

		// Prepare distributors.
		// Use replication factor of 2 to always read all the chunks from both ingesters,
		// this guarantees us to always read the same chunks and have a stable test.
		ds, _, _, _ := prepare(t, prepConfig{
			numIngesters:      2,
			happyIngesters:    2,
			numDistributors:   1,
			shardByAllLabels:  true,
			limits:            limits,
			replicationFactor: 2,
		})

		allSeriesMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
		}
		// Push a single series to allow us to calculate the label size to calculate the limit for the test.
		writeReq := &cortexpbv2.WriteRequest{}
		writeReq.Symbols = []string{"", "__name__", "another_series"}
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0, histogram, false),
		)

		writeRes, err := ds[0].PushV2(ctx, writeReq)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)
		dataSizeResponse, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)

		// Use the resulting chunks size to calculate the limit as (series to add + our test series) * the response chunk size.
		var dataSize = dataSizeResponse.Size()
		var maxBytesLimit = (seriesToAdd) * dataSize * 2 // Multiplying by RF because the limit is applied before de-duping.

		// Update the limiter with the calculated limits.
		ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, 0, maxBytesLimit))

		// Push a number of series below the max chunk bytes limit. Subtract one for the series added above.
		var writeReqV2 *cortexpbv2.WriteRequest
		if histogram {
			writeReqV2 = makeWriteRequestV2WithHistogram(0, seriesToAdd-1, 0)
		} else {
			writeReqV2 = makeWriteRequestV2WithSamples(0, seriesToAdd-1, 0)
		}

		writeRes, err = ds[0].PushV2(ctx, writeReqV2)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		// Since the number of chunk bytes is equal to the limit (but doesn't
		// exceed it), we expect a query running on all series to succeed.
		queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.NoError(t, err)
		assert.Len(t, queryRes.Chunkseries, seriesToAdd)

		// Push another series to exceed the chunk bytes limit once we'll query back all series.
		writeReq = &cortexpbv2.WriteRequest{}
		writeReq.Symbols = []string{"", "__name__", "another_series_1"}
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestV2Timeseries([]cortexpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series_1"}}, 0, 0, histogram, false),
		)

		writeRes, err = ds[0].PushV2(ctx, writeReq)
		assert.Equal(t, &cortexpbv2.WriteResponse{}, writeRes)
		assert.Nil(t, err)

		// Since the aggregated chunk size is exceeding the limit, we expect
		// a query running on all series to fail.
		_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
		require.Error(t, err)
		assert.Equal(t, err, validation.LimitError(fmt.Sprintf(limiter.ErrMaxDataBytesHit, maxBytesLimit)))
	}
}

func TestDistributorPRW2_Push_ShouldGuaranteeShardingTokenConsistencyOverTheTime(t *testing.T) {
	t.Parallel()
	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		expectedToken  uint32
	}{
		"metric_1 with value_1": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped label due to config": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
				{Name: "dropped", Value: "unused"}, // will be dropped, doesn't need to be in correct order
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped HA replica label": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
				{Name: "__replica__", Value: "replica_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_2 with value_1": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_2"},
				{Name: "key", Value: "value_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_2"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xa60906f2,
		},
		"metric_1 with value_2": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_2"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_2"},
			},
			expectedToken: 0x18abc8a2,
		},
	}

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.DropLabels = []string{"dropped"}
	limits.AcceptHASamples = true

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			ds, ingesters, _, _ := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shardByAllLabels: true,
				limits:           &limits,
			})

			// Push the series to the distributor
			req := mockWriteRequestV2([]labels.Labels{testData.inputSeries}, 1, 1, false)
			_, err := ds[0].PushV2(ctx, req)
			require.NoError(t, err)

			// Since each test pushes only 1 series, we do expect the ingester
			// to have received exactly 1 series
			for i := range ingesters {
				timeseries := ingesters[i].series()
				assert.Equal(t, 1, len(timeseries))

				series, ok := timeseries[testData.expectedToken]
				require.True(t, ok)
				assert.Equal(t, testData.expectedSeries, cortexpb.FromLabelAdaptersToLabels(series.Labels))
			}
		})
	}
}

func makeWriteRequestV2WithSamples(startTimestampMs int64, samples int, metadata int) *cortexpbv2.WriteRequest {
	request := &cortexpbv2.WriteRequest{}
	st := writev2.NewSymbolTable()
	st.SymbolizeLabels(labels.Labels{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}}, nil)

	for i := 0; i < samples; i++ {
		st.SymbolizeLabels(labels.Labels{{Name: "sample", Value: fmt.Sprintf("%d", i)}, {Name: "bar", Value: "baz"}}, nil)
		request.Timeseries = append(request.Timeseries, makeTimeseriesV2FromST(
			[]cortexpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: fmt.Sprintf("%d", i)},
			}, &st, startTimestampMs+int64(i), i, false, i < metadata))
	}

	for i := 0; i < metadata-samples; i++ {
		request.Timeseries = append(request.Timeseries, makeMetadataV2FromST(i, &st))
	}

	request.Symbols = st.Symbols()

	return request
}

func TestDistributorPRW2_Push_LabelNameValidation(t *testing.T) {
	t.Parallel()
	inputLabels := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
		{Name: "999.illegal", Value: "baz"},
	}
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		inputLabels                labels.Labels
		skipLabelNameValidationCfg bool
		skipLabelNameValidationReq bool
		errExpected                bool
		errMessage                 string
	}{
		"label name validation is on by default": {
			inputLabels: inputLabels,
			errExpected: true,
			errMessage:  `sample invalid label: "999.illegal" metric "foo{999.illegal=\"baz\"}"`,
		},
		"label name validation can be skipped via config": {
			inputLabels:                inputLabels,
			skipLabelNameValidationCfg: true,
			errExpected:                false,
		},
		"label name validation can be skipped via WriteRequest parameter": {
			inputLabels:                inputLabels,
			skipLabelNameValidationReq: true,
			errExpected:                false,
		},
	}

	for testName, tc := range tests {
		tc := tc
		for _, histogram := range []bool{true, false} {
			histogram := histogram
			t.Run(fmt.Sprintf("%s, histogram=%s", testName, strconv.FormatBool(histogram)), func(t *testing.T) {
				t.Parallel()
				ds, _, _, _ := prepare(t, prepConfig{
					numIngesters:            2,
					happyIngesters:          2,
					numDistributors:         1,
					shuffleShardSize:        1,
					skipLabelNameValidation: tc.skipLabelNameValidationCfg,
				})
				req := mockWriteRequestV2([]labels.Labels{tc.inputLabels}, 42, 100000, histogram)
				req.SkipLabelNameValidation = tc.skipLabelNameValidationReq
				_, err := ds[0].PushV2(ctx, req)
				if tc.errExpected {
					fromError, _ := status.FromError(err)
					assert.Equal(t, tc.errMessage, fromError.Message())
				} else {
					assert.Nil(t, err)
				}
			})
		}
	}
}

func TestDistributorPRW2_Push_ExemplarValidation(t *testing.T) {
	t.Parallel()
	ctx := user.InjectOrgID(context.Background(), "user")
	manyLabels := []string{model.MetricNameLabel, "test"}
	for i := 1; i < 31; i++ {
		manyLabels = append(manyLabels, fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
	}

	tests := map[string]struct {
		req    *cortexpbv2.WriteRequest
		errMsg string
	}{
		"valid exemplar": {
			req: makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{"foo", "bar"}),
		},
		"rejects exemplar with no labels": {
			req:    makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{}),
			errMsg: `exemplar missing labels, timestamp: 1000 series: {__name__="test"} labels: {}`,
		},
		"rejects exemplar with no timestamp": {
			req:    makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test"}, 0, []string{"foo", "bar"}),
			errMsg: `exemplar missing timestamp, timestamp: 0 series: {__name__="test"} labels: {foo="bar"}`,
		},
		"rejects exemplar with too long labelset": {
			req:    makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{"foo", strings.Repeat("0", 126)}),
			errMsg: fmt.Sprintf(`exemplar combined labelset exceeds 128 characters, timestamp: 1000 series: {__name__="test"} labels: {foo="%s"}`, strings.Repeat("0", 126)),
		},
		"rejects exemplar with too many series labels": {
			req:    makeWriteRequestV2Exemplar(manyLabels, 0, nil),
			errMsg: "series has too many labels",
		},
		"rejects exemplar with duplicate series labels": {
			req:    makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test", "foo", "bar", "foo", "bar"}, 0, nil),
			errMsg: "duplicate label name",
		},
		"rejects exemplar with empty series label name": {
			req:    makeWriteRequestV2Exemplar([]string{model.MetricNameLabel, "test", "", "bar"}, 0, nil),
			errMsg: "invalid label",
		},
	}

	for testName, tc := range tests {
		tc := tc
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			ds, _, _, _ := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shuffleShardSize: 1,
			})
			_, err := ds[0].PushV2(ctx, tc.req)
			if tc.errMsg != "" {
				fromError, _ := status.FromError(err)
				assert.Contains(t, fromError.Message(), tc.errMsg)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestDistributorPRW2_MetricsForLabelMatchers_SingleSlowIngester(t *testing.T) {
	t.Parallel()
	for _, histogram := range []bool{true, false} {
		// Create distributor
		ds, ing, _, _ := prepare(t, prepConfig{
			numIngesters:        3,
			happyIngesters:      3,
			numDistributors:     1,
			shardByAllLabels:    true,
			shuffleShardEnabled: true,
			shuffleShardSize:    3,
			replicationFactor:   3,
		})

		ing[2].queryDelay = 50 * time.Millisecond

		ctx := user.InjectOrgID(context.Background(), "test")

		now := model.Now()

		for i := 0; i < 100; i++ {
			req := mockWriteRequestV2([]labels.Labels{{{Name: labels.MetricName, Value: "test"}, {Name: "app", Value: "m"}, {Name: "uniq8", Value: strconv.Itoa(i)}}}, 1, now.Unix(), histogram)
			_, err := ds[0].PushV2(ctx, req)
			require.NoError(t, err)
		}

		for i := 0; i < 50; i++ {
			_, err := ds[0].MetricsForLabelMatchers(ctx, now, now, nil, mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test"))
			require.NoError(t, err)
		}
	}
}

func TestDistributorPRW2_MetricsForLabelMatchers(t *testing.T) {
	t.Parallel()
	const numIngesters = 5

	fixtures := []struct {
		lbls      labels.Labels
		value     int64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		shuffleShardEnabled bool
		shuffleShardSize    int
		matchers            []*labels.Matcher
		expectedResult      []model.Metric
		expectedIngesters   int
		queryLimiter        *limiter.QueryLimiter
		expectedErr         error
	}{
		"should return an empty response if no metric match": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown"),
			},
			expectedResult:    []model.Metric{},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should filter metrics by single matcher": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[0].lbls),
				util.LabelsToMetric(fixtures[1].lbls),
			},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should filter metrics by multiple matchers": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "status", "200"),
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[0].lbls),
			},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "fast_fingerprint_collision"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[3].lbls),
				util.LabelsToMetric(fixtures[4].lbls),
			},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should query only ingesters belonging to tenant's subring if shuffle sharding is enabled": {
			shuffleShardEnabled: true,
			shuffleShardSize:    3,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[0].lbls),
				util.LabelsToMetric(fixtures[1].lbls),
			},
			expectedIngesters: 3,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should query all ingesters if shuffle sharding is enabled but shard size is 0": {
			shuffleShardEnabled: true,
			shuffleShardSize:    0,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[0].lbls),
				util.LabelsToMetric(fixtures[1].lbls),
			},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 0),
			expectedErr:       nil,
		},
		"should return err if series limit is exhausted": {
			shuffleShardEnabled: true,
			shuffleShardSize:    0,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult:    nil,
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(1, 0, 0, 0),
			expectedErr:       validation.LimitError(fmt.Sprintf(limiter.ErrMaxSeriesHit, 1)),
		},
		"should return err if data bytes limit is exhausted": {
			shuffleShardEnabled: true,
			shuffleShardSize:    0,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult:    nil,
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(0, 0, 0, 1),
			expectedErr:       validation.LimitError(fmt.Sprintf(limiter.ErrMaxDataBytesHit, 1)),
		},
		"should not exhaust series limit when only one series is fetched": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_2"),
			},
			expectedResult: []model.Metric{
				util.LabelsToMetric(fixtures[2].lbls),
			},
			expectedIngesters: numIngesters,
			queryLimiter:      limiter.NewQueryLimiter(1, 0, 0, 0),
			expectedErr:       nil,
		},
	}

	for testName, testData := range tests {
		testData := testData
		for _, histogram := range []bool{true, false} {
			histogram := histogram
			t.Run(fmt.Sprintf("%s, histogram=%s", testName, strconv.FormatBool(histogram)), func(t *testing.T) {
				t.Parallel()
				now := model.Now()

				// Create distributor
				ds, ingesters, _, _ := prepare(t, prepConfig{
					numIngesters:        numIngesters,
					happyIngesters:      numIngesters,
					numDistributors:     1,
					shardByAllLabels:    true,
					shuffleShardEnabled: testData.shuffleShardEnabled,
					shuffleShardSize:    testData.shuffleShardSize,
				})

				// Push fixtures
				ctx := user.InjectOrgID(context.Background(), "test")
				ctx = limiter.AddQueryLimiterToContext(ctx, testData.queryLimiter)

				for _, series := range fixtures {
					req := mockWriteRequestV2([]labels.Labels{series.lbls}, series.value, series.timestamp, histogram)
					_, err := ds[0].PushV2(ctx, req)
					require.NoError(t, err)
				}

				{
					metrics, err := ds[0].MetricsForLabelMatchers(ctx, now, now, nil, testData.matchers...)

					if testData.expectedErr != nil {
						assert.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, testData.expectedResult, metrics)

					// Check how many ingesters have been queried.
					// Due to the quorum the distributor could cancel the last request towards ingesters
					// if all other ones are successful, so we're good either has been queried X or X-1
					// ingesters.
					assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "MetricsForLabelMatchers"))
				}

				{
					metrics, err := ds[0].MetricsForLabelMatchersStream(ctx, now, now, nil, testData.matchers...)
					if testData.expectedErr != nil {
						assert.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, testData.expectedResult, metrics)

					assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "MetricsForLabelMatchersStream"))
				}
			})
		}
	}
}

func BenchmarkDistributorPRW2_MetricsForLabelMatchers(b *testing.B) {
	const (
		numIngesters        = 100
		numSeriesPerRequest = 100
	)

	tests := map[string]struct {
		prepareConfig func(limits *validation.Limits)
		prepareSeries func() ([]labels.Labels, []cortexpbv2.Sample)
		matchers      []*labels.Matcher
		queryLimiter  *limiter.QueryLimiter
		expectedErr   error
	}{
		"get series within limits": {
			prepareConfig: func(limits *validation.Limits) {},
			prepareSeries: func() ([]labels.Labels, []cortexpbv2.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]cortexpbv2.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: fmt.Sprintf("foo_%d", i)}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = cortexpbv2.Sample{
						Value:     float64(i),
						Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "foo.+"),
			},
			queryLimiter: limiter.NewQueryLimiter(100, 0, 0, 0),
			expectedErr:  nil,
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			// Create distributor
			ds, ingesters, _, _ := prepare(b, prepConfig{
				numIngesters:        numIngesters,
				happyIngesters:      numIngesters,
				numDistributors:     1,
				shardByAllLabels:    true,
				shuffleShardEnabled: false,
				shuffleShardSize:    0,
			})

			// Push fixtures
			ctx := user.InjectOrgID(context.Background(), "test")
			ctx = limiter.AddQueryLimiterToContext(ctx, testData.queryLimiter)

			// Prepare the series to remote write before starting the benchmark.
			metrics, samples := testData.prepareSeries()

			if _, err := ds[0].PushV2(ctx, cortexpbv2.ToWriteRequestV2(metrics, samples, nil, nil, cortexpbv2.API)); err != nil {
				b.Fatalf("error pushing to distributor %v", err)
			}

			// Run the benchmark.
			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				now := model.Now()
				metrics, err := ds[0].MetricsForLabelMatchers(ctx, now, now, nil, testData.matchers...)

				if testData.expectedErr != nil {
					assert.EqualError(b, err, testData.expectedErr.Error())
					return
				}

				require.NoError(b, err)

				// Check how many ingesters have been queried.
				// Due to the quorum the distributor could cancel the last request towards ingesters
				// if all other ones are successful, so we're good either has been queried X or X-1
				// ingesters.
				assert.Contains(b, []int{numIngesters, numIngesters - 1}, countMockIngestersCalls(ingesters, "MetricsForLabelMatchers"))
				assert.Equal(b, numSeriesPerRequest, len(metrics))
			}
		})
	}
}

func TestDistributorPRW2_MetricsMetadata(t *testing.T) {
	t.Parallel()
	const numIngesters = 5

	tests := map[string]struct {
		shuffleShardEnabled bool
		shuffleShardSize    int
		expectedIngesters   int
	}{
		"should query all ingesters if shuffle sharding is disabled": {
			shuffleShardEnabled: false,
			expectedIngesters:   numIngesters,
		},
		"should query all ingesters if shuffle sharding is enabled but shard size is 0": {
			shuffleShardEnabled: true,
			shuffleShardSize:    0,
			expectedIngesters:   numIngesters,
		},
		"should query only ingesters belonging to tenant's subring if shuffle sharding is enabled": {
			shuffleShardEnabled: true,
			shuffleShardSize:    3,
			expectedIngesters:   3,
		},
	}

	for testName, testData := range tests {
		testData := testData
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			// Create distributor
			ds, ingesters, _, _ := prepare(t, prepConfig{
				numIngesters:        numIngesters,
				happyIngesters:      numIngesters,
				numDistributors:     1,
				shardByAllLabels:    true,
				shuffleShardEnabled: testData.shuffleShardEnabled,
				shuffleShardSize:    testData.shuffleShardSize,
				limits:              nil,
			})

			// Push metadata
			ctx := user.InjectOrgID(context.Background(), "test")

			req := makeWriteRequestV2WithSamples(0, 0, 10)
			_, err := ds[0].PushV2(ctx, req)
			require.NoError(t, err)

			// Assert on metric metadata
			metadata, err := ds[0].MetricsMetadata(ctx)
			require.NoError(t, err)
			assert.Equal(t, 10, len(metadata))

			// Check how many ingesters have been queried.
			// Due to the quorum the distributor could cancel the last request towards ingesters
			// if all other ones are successful, so we're good either has been queried X or X-1
			// ingesters.
			assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "MetricsMetadata"))
		})
	}
}

func makeWriteRequestV2WithHistogram(startTimestampMs int64, histogram int, metadata int) *cortexpbv2.WriteRequest {
	request := &cortexpbv2.WriteRequest{}
	st := writev2.NewSymbolTable()
	st.SymbolizeLabels(labels.Labels{{Name: "__name__", Value: "foo"}, {Name: "bar", Value: "baz"}}, nil)

	for i := 0; i < histogram; i++ {
		st.SymbolizeLabels(labels.Labels{{Name: "histogram", Value: fmt.Sprintf("%d", i)}}, nil)
		request.Timeseries = append(request.Timeseries, makeTimeseriesV2FromST(
			[]cortexpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "histogram", Value: fmt.Sprintf("%d", i)},
			}, &st, startTimestampMs+int64(i), i, true, i < metadata))
	}

	for i := 0; i < metadata-histogram; i++ {
		request.Timeseries = append(request.Timeseries, makeMetadataV2FromST(i, &st))
	}

	request.Symbols = st.Symbols()

	return request
}

func makeMetadataV2FromST(value int, st *writev2.SymbolsTable) cortexpbv2.PreallocTimeseriesV2 {
	t := cortexpbv2.PreallocTimeseriesV2{
		TimeSeries: &cortexpbv2.TimeSeries{
			LabelsRefs: []uint32{1, 2},
		},
	}
	helpRef := st.Symbolize(fmt.Sprintf("a help for metric_%d", value))
	t.Metadata.Type = cortexpbv2.METRIC_TYPE_COUNTER
	t.Metadata.HelpRef = helpRef

	return t
}

func makeTimeseriesV2FromST(labels []cortexpb.LabelAdapter, st *writev2.SymbolsTable, ts int64, value int, histogram bool, metadata bool) cortexpbv2.PreallocTimeseriesV2 {
	var helpRef uint32
	if metadata {
		helpRef = st.Symbolize(fmt.Sprintf("a help for metric_%d", value))
	}

	t := cortexpbv2.PreallocTimeseriesV2{
		TimeSeries: &cortexpbv2.TimeSeries{
			LabelsRefs: st.SymbolizeLabels(cortexpb.FromLabelAdaptersToLabels(labels), nil),
		},
	}
	if metadata {
		t.Metadata.Type = cortexpbv2.METRIC_TYPE_COUNTER
		t.Metadata.HelpRef = helpRef
	}

	if histogram {
		t.Histograms = append(t.Histograms, cortexpbv2.HistogramToHistogramProto(ts, tsdbutil.GenerateTestHistogram(value)))
	} else {
		t.Samples = append(t.Samples, cortexpbv2.Sample{
			Timestamp: ts,
			Value:     float64(value),
		})
	}

	return t
}

func makeWriteRequestV2Timeseries(labels []cortexpb.LabelAdapter, ts int64, value int, histogram bool, metadata bool) cortexpbv2.PreallocTimeseriesV2 {
	st := writev2.NewSymbolTable()
	st.SymbolizeLabels(cortexpb.FromLabelAdaptersToLabels(labels), nil)

	var helpRef uint32
	if metadata {
		helpRef = st.Symbolize(fmt.Sprintf("a help for metric_%d", value))
	}

	t := cortexpbv2.PreallocTimeseriesV2{
		TimeSeries: &cortexpbv2.TimeSeries{
			LabelsRefs: st.SymbolizeLabels(cortexpb.FromLabelAdaptersToLabels(labels), nil),
		},
	}
	if metadata {
		t.Metadata.Type = cortexpbv2.METRIC_TYPE_COUNTER
		t.Metadata.HelpRef = helpRef
	}

	if histogram {
		t.Histograms = append(t.Histograms, cortexpbv2.HistogramToHistogramProto(ts, tsdbutil.GenerateTestHistogram(value)))
	} else {
		t.Samples = append(t.Samples, cortexpbv2.Sample{
			Timestamp: ts,
			Value:     float64(value),
		})
	}

	return t
}

func makeWriteRequestV2Exemplar(seriesLabels []string, timestamp int64, exemplarLabels []string) *cortexpbv2.WriteRequest {
	st := writev2.NewSymbolTable()
	for _, l := range seriesLabels {
		st.Symbolize(l)
	}
	for _, l := range exemplarLabels {
		st.Symbolize(l)
	}

	return &cortexpbv2.WriteRequest{
		Symbols: st.Symbols(),
		Timeseries: []cortexpbv2.PreallocTimeseriesV2{
			{
				TimeSeries: &cortexpbv2.TimeSeries{
					LabelsRefs: cortexpbv2.GetLabelRefsFromLabelAdapters(st.Symbols(), cortexpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...))),
					Exemplars: []cortexpbv2.Exemplar{
						{
							LabelsRefs: cortexpbv2.GetLabelRefsFromLabelAdapters(st.Symbols(), cortexpb.FromLabelsToLabelAdapters(labels.FromStrings(exemplarLabels...))),
							Timestamp:  timestamp,
						},
					},
				},
			},
		},
	}
}

func mockWriteRequestV2(lbls []labels.Labels, value int64, timestamp int64, histogram bool) *cortexpbv2.WriteRequest {
	var (
		samples    []cortexpbv2.Sample
		histograms []cortexpbv2.Histogram
	)
	if histogram {
		histograms = make([]cortexpbv2.Histogram, len(lbls))
		for i := range lbls {
			histograms[i] = cortexpbv2.HistogramToHistogramProto(timestamp, tsdbutil.GenerateTestHistogram(int(value)))
		}
	} else {
		samples = make([]cortexpbv2.Sample, len(lbls))
		for i := range lbls {
			samples[i] = cortexpbv2.Sample{
				Timestamp: timestamp,
				Value:     float64(value),
			}
		}
	}

	return cortexpbv2.ToWriteRequestV2(lbls, samples, histograms, nil, cortexpbv2.API)
}

func makeWriteRequestHAV2(samples int, replica, cluster string, histogram bool) *cortexpbv2.WriteRequest {
	request := &cortexpbv2.WriteRequest{}
	st := writev2.NewSymbolTable()
	for i := 0; i < samples; i++ {
		ts := cortexpbv2.PreallocTimeseriesV2{
			TimeSeries: &cortexpbv2.TimeSeries{
				LabelsRefs: st.SymbolizeLabels(labels.Labels{{Name: "__name__", Value: "foo"}, {Name: "__replica__", Value: replica}, {Name: "bar", Value: "baz"}, {Name: "cluster", Value: cluster}, {Name: "sample", Value: fmt.Sprintf("%d", i)}}, nil),
			},
		}
		if histogram {
			ts.Histograms = []cortexpbv2.Histogram{
				cortexpbv2.HistogramToHistogramProto(int64(i), tsdbutil.GenerateTestHistogram(i)),
			}
		} else {
			ts.Samples = []cortexpbv2.Sample{
				{
					Value:     float64(i),
					Timestamp: int64(i),
				},
			}
		}
		request.Timeseries = append(request.Timeseries, ts)
	}
	request.Symbols = st.Symbols()
	return request
}
