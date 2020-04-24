package distributor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/ring"
	ring_client "github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	errFail = fmt.Errorf("Fail")
	success = &client.WriteResponse{}
	ctx     = user.InjectOrgID(context.Background(), "user")
)

func TestDistributor_Push(t *testing.T) {
	// Metrics to assert on.
	lastSeenTimestamp := "cortex_distributor_latest_seen_sample_timestamp_seconds"
	distributorAppend := "cortex_distributor_ingester_appends_total"
	distributorAppendFailure := "cortex_distributor_ingester_append_failures_total"

	type samplesIn struct {
		num              int
		startTimestampMs int64
	}
	for name, tc := range map[string]struct {
		metricNames      []string
		numIngesters     int
		happyIngesters   int
		samples          samplesIn
		metadata         int
		expectedResponse *client.WriteResponse
		expectedError    error
		expectedMetrics  string
	}{
		"A push of no samples shouldn't block or return error, even if ingesters are sad": {
			numIngesters:     3,
			happyIngesters:   0,
			expectedResponse: success,
		},
		"A push to 3 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: success,
			metricNames:      []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.004
			`,
		},
		"A push to 2 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: success,
			metricNames:      []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.004
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
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
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
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
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
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.024
			`,
		},
		"A push to ingesters should report the correct metrics with no metadata": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 1, startTimestampMs: 123456789000},
			metadata:         0,
			metricNames:      []string{distributorAppend, distributorAppendFailure},
			expectedResponse: success,
			expectedMetrics: `
				# HELP cortex_distributor_ingester_append_failures_total The total number of failed batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_append_failures_total counter
				cortex_distributor_ingester_append_failures_total{ingester="2",type="samples"} 1
				# HELP cortex_distributor_ingester_appends_total The total number of batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_appends_total counter
				cortex_distributor_ingester_appends_total{ingester="0",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="1",type="samples"} 1
				cortex_distributor_ingester_appends_total{ingester="2",type="samples"} 1
			`,
		},
		"A push to ingesters should report the correct metrics with no samples": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 0, startTimestampMs: 123456789000},
			metadata:         1,
			metricNames:      []string{distributorAppend, distributorAppendFailure},
			expectedResponse: success,
			expectedMetrics: `
				# HELP cortex_distributor_ingester_append_failures_total The total number of failed batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_append_failures_total counter
				cortex_distributor_ingester_append_failures_total{ingester="2",type="metadata"} 1
				# HELP cortex_distributor_ingester_appends_total The total number of batch appends sent to ingesters.
				# TYPE cortex_distributor_ingester_appends_total counter
				cortex_distributor_ingester_appends_total{ingester="0",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="1",type="metadata"} 1
				cortex_distributor_ingester_appends_total{ingester="2",type="metadata"} 1
			`,
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%s](shardByAllLabels=%v)", name, shardByAllLabels), func(t *testing.T) {
				latestSeenSampleTimestampPerUser.Reset()
				ingesterAppends.Reset()
				ingesterAppendFailures.Reset()

				limits := &validation.Limits{}
				flagext.DefaultValues(limits)
				limits.IngestionRate = 20
				limits.IngestionBurstSize = 20

				ds, _, r := prepare(t, prepConfig{
					numIngesters:     tc.numIngesters,
					happyIngesters:   tc.happyIngesters,
					numDistributors:  1,
					shardByAllLabels: shardByAllLabels,
					limits:           limits,
				})
				defer stopAll(ds, r)

				request := makeWriteRequest(tc.samples.startTimestampMs, tc.samples.num, tc.metadata)
				response, err := ds[0].Push(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)
				assert.Equal(t, tc.expectedError, err)

				// Check tracked Prometheus metrics.
				if tc.expectedMetrics != "" {
					err = testutil.GatherAndCompare(prometheus.DefaultGatherer, strings.NewReader(tc.expectedMetrics), tc.metricNames...)
					assert.NoError(t, err)
				}
			})
		}
	}
}

func TestDistributor_PushIngestionRateLimiter(t *testing.T) {
	type testPush struct {
		samples       int
		metadata      int
		expectedError error
	}

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

		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.IngestionRateStrategy = testData.ingestionRateStrategy
			limits.IngestionRate = testData.ingestionRate
			limits.IngestionBurstSize = testData.ingestionBurstSize

			// Start all expected distributors
			distributors, _, r := prepare(t, prepConfig{
				numIngesters:     3,
				happyIngesters:   3,
				numDistributors:  testData.distributors,
				shardByAllLabels: true,
				limits:           limits,
			})
			defer stopAll(distributors, r)

			// Push samples in multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, push.samples, push.metadata)
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, success, response)
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response)
					assert.Equal(t, push.expectedError, err)
				}
			}
		})
	}
}

func TestDistributor_PushHAInstances(t *testing.T) {
	ctx = user.InjectOrgID(context.Background(), "user")

	for i, tc := range []struct {
		enableTracker    bool
		acceptedReplica  string
		testReplica      string
		cluster          string
		samples          int
		expectedResponse *client.WriteResponse
		expectedCode     int32
	}{
		{
			enableTracker:    true,
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: success,
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
			expectedResponse: success,
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%d](shardByAllLabels=%v)", i, shardByAllLabels), func(t *testing.T) {
				var limits validation.Limits
				flagext.DefaultValues(&limits)
				limits.AcceptHASamples = true

				ds, _, r := prepare(t, prepConfig{
					numIngesters:     3,
					happyIngesters:   3,
					numDistributors:  1,
					shardByAllLabels: shardByAllLabels,
					limits:           &limits,
				})
				defer stopAll(ds, r)
				codec := GetReplicaDescCodec()
				mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")
				d := ds[0]

				if tc.enableTracker {
					r, err := newClusterTracker(HATrackerConfig{
						EnableHATracker: true,
						KVStore:         kv.Config{Mock: mock},
						UpdateTimeout:   100 * time.Millisecond,
						FailoverTimeout: time.Second,
					})
					require.NoError(t, err)
					require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
					d.HATracker = r
				}

				userID, err := user.ExtractOrgID(ctx)
				assert.NoError(t, err)
				err = d.HATracker.checkReplica(ctx, userID, tc.cluster, tc.acceptedReplica)
				assert.NoError(t, err)

				request := makeWriteRequestHA(tc.samples, tc.testReplica, tc.cluster)
				response, err := d.Push(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)

				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				if ok {
					assert.Equal(t, tc.expectedCode, httpResp.Code)
				}
			})
		}
	}
}

func TestDistributor_PushQuery(t *testing.T) {
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	barMatcher := mustEqualMatcher("bar", "baz")

	type testcase struct {
		name             string
		numIngesters     int
		happyIngesters   int
		samples          int
		metadata         int
		matchers         []*labels.Matcher
		expectedResponse model.Matrix
		expectedError    error
		shardByAllLabels bool
	}

	// We'll programatically build the test cases now, as we want complete
	// coverage along quite a few different axis.
	testcases := []testcase{}

	// Run every test in both sharding modes.
	for _, shardByAllLabels := range []bool{true, false} {

		// Test with between 2 and 10 ingesters.
		for numIngesters := 2; numIngesters < 10; numIngesters++ {

			// Test with between 0 and numIngesters "happy" ingesters.
			for happyIngesters := 0; happyIngesters <= numIngesters; happyIngesters++ {

				// When we're not sharding by metric name, queriers with more than one
				// failed ingester should fail.
				if shardByAllLabels && numIngesters-happyIngesters > 1 {
					testcases = append(testcases, testcase{
						name:             fmt.Sprintf("ExpectFail(shardByAllLabels=%v,numIngester=%d,happyIngester=%d)", shardByAllLabels, numIngesters, happyIngesters),
						numIngesters:     numIngesters,
						happyIngesters:   happyIngesters,
						matchers:         []*labels.Matcher{nameMatcher, barMatcher},
						expectedError:    promql.ErrStorage{Err: errFail},
						shardByAllLabels: shardByAllLabels,
					})
					continue
				}

				// When we have less ingesters than replication factor, any failed ingester
				// will cause a failure.
				if numIngesters < 3 && happyIngesters < 2 {
					testcases = append(testcases, testcase{
						name:             fmt.Sprintf("ExpectFail(shardByAllLabels=%v,numIngester=%d,happyIngester=%d)", shardByAllLabels, numIngesters, happyIngesters),
						numIngesters:     numIngesters,
						happyIngesters:   happyIngesters,
						matchers:         []*labels.Matcher{nameMatcher, barMatcher},
						expectedError:    promql.ErrStorage{Err: errFail},
						shardByAllLabels: shardByAllLabels,
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
					name:             fmt.Sprintf("ReadAll(shardByAllLabels=%v,numIngester=%d,happyIngester=%d)", shardByAllLabels, numIngesters, happyIngesters),
					numIngesters:     numIngesters,
					happyIngesters:   happyIngesters,
					samples:          10,
					matchers:         []*labels.Matcher{nameMatcher, barMatcher},
					expectedResponse: expectedResponse(0, 10),
					shardByAllLabels: shardByAllLabels,
				})

				// As should reading none of the samples back.
				testcases = append(testcases, testcase{
					name:             fmt.Sprintf("ReadNone(shardByAllLabels=%v,numIngester=%d,happyIngester=%d)", shardByAllLabels, numIngesters, happyIngesters),
					numIngesters:     numIngesters,
					happyIngesters:   happyIngesters,
					samples:          10,
					matchers:         []*labels.Matcher{nameMatcher, mustEqualMatcher("not", "found")},
					expectedResponse: expectedResponse(0, 0),
					shardByAllLabels: shardByAllLabels,
				})

				// And reading each sample individually.
				for i := 0; i < 10; i++ {
					testcases = append(testcases, testcase{
						name:             fmt.Sprintf("ReadOne(shardByAllLabels=%v, sample=%d,numIngester=%d,happyIngester=%d)", shardByAllLabels, i, numIngesters, happyIngesters),
						numIngesters:     numIngesters,
						happyIngesters:   happyIngesters,
						samples:          10,
						matchers:         []*labels.Matcher{nameMatcher, mustEqualMatcher("sample", strconv.Itoa(i))},
						expectedResponse: expectedResponse(i, i+1),
						shardByAllLabels: shardByAllLabels,
					})
				}
			}
		}
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ds, _, r := prepare(t, prepConfig{
				numIngesters:     tc.numIngesters,
				happyIngesters:   tc.happyIngesters,
				numDistributors:  1,
				shardByAllLabels: tc.shardByAllLabels,
			})
			defer stopAll(ds, r)

			request := makeWriteRequest(0, tc.samples, tc.metadata)
			writeResponse, err := ds[0].Push(ctx, request)
			assert.Equal(t, &client.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			response, err := ds[0].Query(ctx, 0, 10, tc.matchers...)
			sort.Sort(response)
			assert.Equal(t, tc.expectedResponse, response)
			assert.Equal(t, tc.expectedError, err)

			series, err := ds[0].QueryStream(ctx, 0, 10, tc.matchers...)
			assert.Equal(t, tc.expectedError, err)

			if series == nil {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, nil)
			} else {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, series.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse.String(), response.String())
		})
	}
}

func TestDistributor_Push_LabelRemoval(t *testing.T) {
	ctx = user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		removeReplica  bool
		removeLabels   []string
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
	}

	for _, tc := range cases {
		var err error
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.DropLabels = tc.removeLabels
		limits.AcceptHASamples = tc.removeReplica

		ds, ingesters, r := prepare(t, prepConfig{
			numIngesters:     2,
			happyIngesters:   2,
			numDistributors:  1,
			shardByAllLabels: true,
			limits:           &limits,
		})
		defer stopAll(ds, r)

		// Push the series to the distributor
		req := mockWriteRequest(tc.inputSeries, 1, 1)
		_, err = ds[0].Push(ctx, req)
		require.NoError(t, err)

		// Since each test pushes only 1 series, we do expect the ingester
		// to have received exactly 1 series
		for i := range ingesters {
			timeseries := ingesters[i].series()
			assert.Equal(t, 1, len(timeseries))
			for _, v := range timeseries {
				assert.Equal(t, tc.expectedSeries, client.FromLabelAdaptersToLabels(v.Labels))
			}
		}
	}
}

func TestDistributor_Push_ShouldGuaranteeShardingTokenConsistencyOverTheTime(t *testing.T) {
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

	ctx = user.InjectOrgID(context.Background(), "user")

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ds, ingesters, r := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shardByAllLabels: true,
				limits:           &limits,
			})
			defer stopAll(ds, r)

			// Push the series to the distributor
			req := mockWriteRequest(testData.inputSeries, 1, 1)
			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)

			// Since each test pushes only 1 series, we do expect the ingester
			// to have received exactly 1 series
			for i := range ingesters {
				timeseries := ingesters[i].series()
				assert.Equal(t, 1, len(timeseries))

				series, ok := timeseries[testData.expectedToken]
				require.True(t, ok)
				assert.Equal(t, testData.expectedSeries, client.FromLabelAdaptersToLabels(series.Labels))
			}
		})
	}
}

func TestSlowQueries(t *testing.T) {
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	nIngesters := 3
	for _, shardByAllLabels := range []bool{true, false} {
		for happy := 0; happy <= nIngesters; happy++ {
			t.Run(fmt.Sprintf("%t/%d", shardByAllLabels, happy), func(t *testing.T) {
				var expectedErr error
				if nIngesters-happy > 1 {
					expectedErr = promql.ErrStorage{Err: errFail}
				}

				ds, _, r := prepare(t, prepConfig{
					numIngesters:     nIngesters,
					happyIngesters:   happy,
					numDistributors:  1,
					queryDelay:       100 * time.Millisecond,
					shardByAllLabels: shardByAllLabels,
				})
				defer stopAll(ds, r)

				_, err := ds[0].Query(ctx, 0, 10, nameMatcher)
				assert.Equal(t, expectedErr, err)

				_, err = ds[0].QueryStream(ctx, 0, 10, nameMatcher)
				assert.Equal(t, expectedErr, err)
			})
		}
	}
}

func TestDistributor_MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
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
		matchers []*labels.Matcher
		expected []metric.Metric
	}{
		"should return an empty response if no metric match": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown"),
			},
			expected: []metric.Metric{},
		},
		"should filter metrics by single matcher": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expected: []metric.Metric{
				{Metric: util.LabelsToMetric(fixtures[0].lbls)},
				{Metric: util.LabelsToMetric(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "status", "200"),
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expected: []metric.Metric{
				{Metric: util.LabelsToMetric(fixtures[0].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "fast_fingerprint_collision"),
			},
			expected: []metric.Metric{
				{Metric: util.LabelsToMetric(fixtures[3].lbls)},
				{Metric: util.LabelsToMetric(fixtures[4].lbls)},
			},
		},
	}

	// Create distributor
	ds, _, r := prepare(t, prepConfig{
		numIngesters:     3,
		happyIngesters:   3,
		numDistributors:  1,
		shardByAllLabels: true,
	})
	defer stopAll(ds, r)

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := ds[0].Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			now := model.Now()

			metrics, err := ds[0].MetricsForLabelMatchers(ctx, now, now, testData.matchers...)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, metrics)
		})
	}
}

func TestDistributor_MetricsMetadata(t *testing.T) {
	// Create distributor
	ds, _, r := prepare(t, prepConfig{
		numIngesters:     3,
		happyIngesters:   3,
		numDistributors:  1,
		shardByAllLabels: true,
		limits:           nil,
	})
	defer stopAll(ds, r)

	// Push metadata
	ctx := user.InjectOrgID(context.Background(), "test")

	req := makeWriteRequest(0, 0, 10)
	_, err := ds[0].Push(ctx, req)
	require.NoError(t, err)

	// Asert on metric metadata
	metadata, err := ds[0].MetricsMetadata(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, len(metadata))
}

func mustNewMatcher(t labels.MatchType, n, v string) *labels.Matcher {
	m, err := labels.NewMatcher(t, n, v)
	if err != nil {
		panic(err)
	}

	return m
}

func mockWriteRequest(lbls labels.Labels, value float64, timestampMs int64) *client.WriteRequest {
	samples := []client.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	return client.ToWriteRequest([]labels.Labels{lbls}, samples, nil, client.API)
}

type prepConfig struct {
	numIngesters, happyIngesters int
	queryDelay                   time.Duration
	shardByAllLabels             bool
	limits                       *validation.Limits
	numDistributors              int
}

func prepare(t *testing.T, cfg prepConfig) ([]*Distributor, []mockIngester, *ring.Ring) {
	ingesters := []mockIngester{}
	for i := 0; i < cfg.happyIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			happy:      true,
			queryDelay: cfg.queryDelay,
		})
	}
	for i := cfg.happyIngesters; i < cfg.numIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			queryDelay: cfg.queryDelay,
		})
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	ingesterDescs := map[string]ring.IngesterDesc{}
	ingestersByAddr := map[string]*mockIngester{}
	for i := range ingesters {
		addr := fmt.Sprintf("%d", i)
		ingesterDescs[addr] = ring.IngesterDesc{
			Addr:      addr,
			Zone:      addr,
			State:     ring.ACTIVE,
			Timestamp: time.Now().Unix(),
			Tokens:    []uint32{uint32((math.MaxUint32 / cfg.numIngesters) * i)},
		}
		ingestersByAddr[addr] = &ingesters[i]
	}

	kvStore := consul.NewInMemoryClient(ring.GetCodec())
	err := kvStore.CAS(context.Background(), ring.IngesterRingKey,
		func(_ interface{}) (interface{}, bool, error) {
			return &ring.Desc{
				Ingesters: ingesterDescs,
			}, true, nil
		},
	)
	require.NoError(t, err)

	ingestersRing, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvStore,
		},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: 3,
	}, ring.IngesterRingKey, ring.IngesterRingKey)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingestersRing))

	test.Poll(t, time.Second, cfg.numIngesters, func() interface{} {
		return ingestersRing.IngesterCount()
	})

	factory := func(addr string) (ring_client.PoolClient, error) {
		return ingestersByAddr[addr], nil
	}

	distributors := make([]*Distributor, 0, cfg.numDistributors)
	for i := 0; i < cfg.numDistributors; i++ {
		var distributorCfg Config
		var clientConfig client.Config
		flagext.DefaultValues(&distributorCfg, &clientConfig)

		distributorCfg.ingesterClientFactory = factory
		distributorCfg.ShardByAllLabels = cfg.shardByAllLabels
		distributorCfg.ExtraQueryDelay = 50 * time.Millisecond
		distributorCfg.DistributorRing.HeartbeatPeriod = 100 * time.Millisecond
		distributorCfg.DistributorRing.InstanceID = strconv.Itoa(i)
		distributorCfg.DistributorRing.KVStore.Mock = kvStore
		distributorCfg.DistributorRing.InstanceAddr = "127.0.0.1"

		if cfg.limits == nil {
			cfg.limits = &validation.Limits{}
			flagext.DefaultValues(cfg.limits)
		}
		overrides, err := validation.NewOverrides(*cfg.limits, nil)
		require.NoError(t, err)

		d, err := New(distributorCfg, clientConfig, overrides, ingestersRing, true)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), d))

		distributors = append(distributors, d)
	}

	// If the distributors ring is setup, wait until the first distributor
	// updates to the expected size
	if distributors[0].distributorsRing != nil {
		test.Poll(t, time.Second, cfg.numDistributors, func() interface{} {
			return distributors[0].distributorsRing.HealthyInstancesCount()
		})
	}

	return distributors, ingesters, ingestersRing
}

func stopAll(ds []*Distributor, r *ring.Ring) {
	for _, d := range ds {
		services.StopAndAwaitTerminated(context.Background(), d) //nolint:errcheck
	}

	// Mock consul doesn't stop quickly, so don't wait.
	r.StopAsync()
}

func makeWriteRequest(startTimestampMs int64, samples int, metadata int) *client.WriteRequest {
	request := &client.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := client.PreallocTimeseries{
			TimeSeries: &client.TimeSeries{
				Labels: []client.LabelAdapter{
					{Name: model.MetricNameLabel, Value: "foo"},
					{Name: "bar", Value: "baz"},
					{Name: "sample", Value: fmt.Sprintf("%d", i)},
				},
			},
		}
		ts.Samples = []client.Sample{
			{
				Value:       float64(i),
				TimestampMs: startTimestampMs + int64(i),
			},
		}
		request.Timeseries = append(request.Timeseries, ts)
	}

	for i := 0; i < metadata; i++ {
		m := &client.MetricMetadata{
			MetricName: fmt.Sprintf("metric_%d", i),
			Type:       client.COUNTER,
			Help:       fmt.Sprintf("a help for metric_%d", i),
		}
		request.Metadata = append(request.Metadata, m)
	}

	return request
}

func makeWriteRequestHA(samples int, replica, cluster string) *client.WriteRequest {
	request := &client.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := client.PreallocTimeseries{
			TimeSeries: &client.TimeSeries{
				Labels: []client.LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "__replica__", Value: replica},
					{Name: "bar", Value: "baz"},
					{Name: "cluster", Value: cluster},
					{Name: "sample", Value: fmt.Sprintf("%d", i)},
				},
			},
		}
		ts.Samples = []client.Sample{
			{
				Value:       float64(i),
				TimestampMs: int64(i),
			},
		}
		request.Timeseries = append(request.Timeseries, ts)
	}
	return request
}

func expectedResponse(start, end int) model.Matrix {
	result := model.Matrix{}
	for i := start; i < end; i++ {
		result = append(result, &model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: "foo",
				"bar":                 "baz",
				"sample":              model.LabelValue(fmt.Sprintf("%d", i)),
			},
			Values: []model.SamplePair{
				{
					Value:     model.SampleValue(i),
					Timestamp: model.Time(i),
				},
			},
		})
	}
	return result
}

func mustEqualMatcher(k, v string) *labels.Matcher {
	m, err := labels.NewMatcher(labels.MatchEqual, k, v)
	if err != nil {
		panic(err)
	}
	return m
}

type mockIngester struct {
	sync.Mutex
	client.IngesterClient
	grpc_health_v1.HealthClient
	happy      bool
	stats      client.UsersStatsResponse
	timeseries map[uint32]*client.PreallocTimeseries
	metadata   map[uint32]map[client.MetricMetadata]struct{}
	queryDelay time.Duration
}

func (i *mockIngester) series() map[uint32]*client.PreallocTimeseries {
	i.Lock()
	defer i.Unlock()

	result := map[uint32]*client.PreallocTimeseries{}
	for k, v := range i.timeseries {
		result[k] = v
	}
	return result
}

func (i *mockIngester) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func (i *mockIngester) Close() error {
	return nil
}

func (i *mockIngester) Push(ctx context.Context, req *client.WriteRequest, opts ...grpc.CallOption) (*client.WriteResponse, error) {
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	if i.timeseries == nil {
		i.timeseries = map[uint32]*client.PreallocTimeseries{}
	}

	if i.metadata == nil {
		i.metadata = map[uint32]map[client.MetricMetadata]struct{}{}
	}

	orgid, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	for j := range req.Timeseries {
		series := req.Timeseries[j]
		hash := shardByAllLabels(orgid, series.Labels)
		existing, ok := i.timeseries[hash]
		if !ok {
			// Make a copy because the request Timeseries are reused
			item := client.TimeSeries{
				Labels:  make([]client.LabelAdapter, len(series.TimeSeries.Labels)),
				Samples: make([]client.Sample, len(series.TimeSeries.Samples)),
			}

			copy(item.Labels, series.TimeSeries.Labels)
			copy(item.Samples, series.TimeSeries.Samples)

			i.timeseries[hash] = &client.PreallocTimeseries{TimeSeries: &item}
		} else {
			existing.Samples = append(existing.Samples, series.Samples...)
		}
	}

	for _, m := range req.Metadata {
		hash := shardByMetricName(orgid, m.MetricName)
		set, ok := i.metadata[hash]
		if !ok {
			set = map[client.MetricMetadata]struct{}{}
			i.metadata[hash] = set
		}
		set[*m] = struct{}{}
	}

	return &client.WriteResponse{}, nil
}

func (i *mockIngester) Query(ctx context.Context, req *client.QueryRequest, opts ...grpc.CallOption) (*client.QueryResponse, error) {
	time.Sleep(i.queryDelay)
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	_, _, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	response := client.QueryResponse{}
	for _, ts := range i.timeseries {
		if match(ts.Labels, matchers) {
			response.Timeseries = append(response.Timeseries, *ts.TimeSeries)
		}
	}
	return &response, nil
}

func (i *mockIngester) QueryStream(ctx context.Context, req *client.QueryRequest, opts ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
	time.Sleep(i.queryDelay)
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	_, _, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	results := []*client.QueryStreamResponse{}
	for _, ts := range i.timeseries {
		if !match(ts.Labels, matchers) {
			continue
		}

		c := encoding.New()
		chunks := []encoding.Chunk{c}
		for _, sample := range ts.Samples {
			newChunk, err := c.Add(model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
			if err != nil {
				panic(err)
			}
			if newChunk != nil {
				c = newChunk
				chunks = append(chunks, newChunk)
			}
		}

		wireChunks := []client.Chunk{}
		for _, c := range chunks {
			var buf bytes.Buffer
			chunk := client.Chunk{
				Encoding: int32(c.Encoding()),
			}
			if err := c.Marshal(&buf); err != nil {
				panic(err)
			}
			chunk.Data = buf.Bytes()
			wireChunks = append(wireChunks, chunk)
		}

		results = append(results, &client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: ts.Labels,
					Chunks: wireChunks,
				},
			},
		})
	}
	return &stream{
		results: results,
	}, nil
}

func (i *mockIngester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	_, _, multiMatchers, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	response := client.MetricsForLabelMatchersResponse{}
	for _, matchers := range multiMatchers {
		for _, ts := range i.timeseries {
			if match(ts.Labels, matchers) {
				response.Metric = append(response.Metric, &client.Metric{Labels: ts.Labels})
			}
		}
	}
	return &response, nil
}

func (i *mockIngester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest, opts ...grpc.CallOption) (*client.MetricsMetadataResponse, error) {
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	resp := &client.MetricsMetadataResponse{}
	for _, sets := range i.metadata {
		for m := range sets {
			resp.Metadata = append(resp.Metadata, &m)
		}
	}

	return resp, nil
}

type stream struct {
	grpc.ClientStream
	i       int
	results []*client.QueryStreamResponse
}

func (*stream) CloseSend() error {
	return nil
}

func (s *stream) Recv() (*client.QueryStreamResponse, error) {
	if s.i >= len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.i]
	s.i++
	return result, nil
}

func (i *mockIngester) AllUserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}

func match(labels []client.LabelAdapter, matchers []*labels.Matcher) bool {
outer:
	for _, matcher := range matchers {
		for _, labels := range labels {
			if matcher.Name == labels.Name && matcher.Matches(labels.Value) {
				continue outer
			}
		}
		return false
	}
	return true
}

func TestDistributorValidation(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "1")
	now := model.Now()
	future, past := now.Add(5*time.Hour), now.Add(-25*time.Hour)

	for i, tc := range []struct {
		metadata []*client.MetricMetadata
		labels   []labels.Labels
		samples  []client.Sample
		err      error
	}{
		// Test validation passes.
		{
			metadata: []*client.MetricMetadata{{MetricName: "testmetric", Help: "a test metric.", Unit: "", Type: client.COUNTER}},
			labels:   []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []client.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
		},
		// Test validation fails for very old samples.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []client.Sample{{
				TimestampMs: int64(past),
				Value:       2,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, "sample for 'testmetric' has timestamp too old: %d", past),
		},

		// Test validation fails for samples from the future.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []client.Sample{{
				TimestampMs: int64(future),
				Value:       4,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, "sample for 'testmetric' has timestamp too new: %d", future),
		},

		// Test maximum labels names per series.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}}},
			samples: []client.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, `sample for 'testmetric{foo2="bar2", foo="bar"}' has 3 label names; limit 2`),
		},
		// Test multiple validation fails return the first one.
		{
			labels: []labels.Labels{
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}},
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}},
			},
			samples: []client.Sample{
				{TimestampMs: int64(now), Value: 2},
				{TimestampMs: int64(past), Value: 2},
			},
			err: httpgrpc.Errorf(http.StatusBadRequest, `sample for 'testmetric{foo2="bar2", foo="bar"}' has 3 label names; limit 2`),
		},
		// Test metadata validation fails
		{
			metadata: []*client.MetricMetadata{{MetricName: "", Help: "a test metric.", Unit: "", Type: client.COUNTER}},
			labels:   []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []client.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, `metadata missing metric name`),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)

			limits.CreationGracePeriod = 2 * time.Hour
			limits.RejectOldSamples = true
			limits.RejectOldSamplesMaxAge = 24 * time.Hour
			limits.MaxLabelNamesPerSeries = 2

			ds, _, r := prepare(t, prepConfig{
				numIngesters:     3,
				happyIngesters:   3,
				numDistributors:  1,
				shardByAllLabels: true,
				limits:           &limits,
			})
			defer stopAll(ds, r)

			_, err := ds[0].Push(ctx, client.ToWriteRequest(tc.labels, tc.samples, tc.metadata, client.API))
			require.Equal(t, tc.err, err)
		})
	}
}

func TestRemoveReplicaLabel(t *testing.T) {
	replicaLabel := "replica"
	clusterLabel := "cluster"
	cases := []struct {
		labelsIn  []client.LabelAdapter
		labelsOut []client.LabelAdapter
	}{
		// Replica label is present
		{
			labelsIn: []client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "replica", Value: replicaLabel},
			},
			labelsOut: []client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
			},
		},
		// Replica label is not present
		{
			labelsIn: []client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "cluster", Value: clusterLabel},
			},
			labelsOut: []client.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "cluster", Value: clusterLabel},
			},
		},
	}

	for _, c := range cases {
		removeLabel(replicaLabel, &c.labelsIn)
		assert.Equal(t, c.labelsOut, c.labelsIn)
	}
}

// This is not great, but we deal with unsorted labels when validating labels.
func TestShardByAllLabelsReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := shardByAllLabels("test", []client.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	})

	val2 := shardByAllLabels("test", []client.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	})

	assert.NotEqual(t, val1, val2)
}

func TestSortLabels(t *testing.T) {
	sorted := []client.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "cluster", Value: "cluster"},
		{Name: "sample", Value: "1"},
	}

	// no allocations if input is already sorted
	require.Equal(t, 0.0, testing.AllocsPerRun(100, func() {
		sortLabelsIfNeeded(sorted)
	}))

	unsorted := []client.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "cluster", Value: "cluster"},
		{Name: "bar", Value: "baz"},
	}

	sortLabelsIfNeeded(unsorted)

	sort.SliceIsSorted(unsorted, func(i, j int) bool {
		return strings.Compare(unsorted[i].Name, unsorted[j].Name) < 0
	})
}
