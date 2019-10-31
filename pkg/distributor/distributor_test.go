package distributor

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

var (
	errFail = fmt.Errorf("Fail")
	success = &client.WriteResponse{}
	ctx     = user.InjectOrgID(context.Background(), "user")
)

func TestDistributor_Push(t *testing.T) {
	for name, tc := range map[string]struct {
		numIngesters     int
		happyIngesters   int
		samples          int
		expectedResponse *client.WriteResponse
		expectedError    error
	}{
		"A push of no samples shouldn't block or return error, even if ingesters are sad": {
			numIngesters:     3,
			happyIngesters:   0,
			expectedResponse: success,
		},
		"A push to 3 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          10,
			expectedResponse: success,
		},
		"A push to 2 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          10,
			expectedResponse: success,
		},
		"A push to 1 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 1,
			samples:        10,
			expectedError:  errFail,
		},
		"A push to 0 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 0,
			samples:        10,
			expectedError:  errFail,
		},
		"A push exceeding burst size should fail": {
			numIngesters:   3,
			happyIngesters: 3,
			samples:        30,
			expectedError:  httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (20) exceeded while adding 30 samples"),
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%s](shardByAllLabels=%v)", name, shardByAllLabels), func(t *testing.T) {
				limits := &validation.Limits{}
				flagext.DefaultValues(limits)
				limits.IngestionRate = 20
				limits.IngestionBurstSize = 20

				d, _ := prepare(t, tc.numIngesters, tc.happyIngesters, 0, shardByAllLabels, limits, nil)
				defer d.Stop()

				request := makeWriteRequest(tc.samples)
				response, err := d.Push(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)
				assert.Equal(t, tc.expectedError, err)
			})
		}
	}
}

func TestDistributor_PushIngestionRateLimiter(t *testing.T) {
	type testPush struct {
		samples       int
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
				{samples: 5, expectedError: nil},
				{samples: 6, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (10) exceeded while adding 6 samples")},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (10) exceeded while adding 1 samples")},
			},
		},
		"global strategy: limit should be evenly shared across distributors": {
			distributors:          2,
			ingestionRateStrategy: validation.GlobalIngestionRateStrategy,
			ingestionRate:         10,
			ingestionBurstSize:    5,
			pushes: []testPush{
				{samples: 3, expectedError: nil},
				{samples: 3, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 3 samples")},
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 1 samples")},
			},
		},
		"global strategy: burst should set to each distributor": {
			distributors:          2,
			ingestionRateStrategy: validation.GlobalIngestionRateStrategy,
			ingestionRate:         10,
			ingestionBurstSize:    20,
			pushes: []testPush{
				{samples: 15, expectedError: nil},
				{samples: 6, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 6 samples")},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (5) exceeded while adding 1 samples")},
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

			// Init a shared KVStore
			kvStore := consul.NewInMemoryClient(ring.GetCodec())

			// Start all expected distributors
			distributors := make([]*Distributor, testData.distributors)
			for i := 0; i < testData.distributors; i++ {
				distributors[i], _ = prepare(t, 1, 1, 0, true, limits, kvStore)
				defer distributors[i].Stop()
			}

			// If the distributors ring is setup, wait until the first distributor
			// updates to the expected size
			if distributors[0].distributorsRing != nil {
				test.Poll(t, time.Second, testData.distributors, func() interface{} {
					return distributors[0].distributorsRing.HealthyInstancesCount()
				})
			}

			// Push samples in multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(push.samples)
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

				d, _ := prepare(t, 1, 1, 0, shardByAllLabels, &limits, nil)
				codec := codec.Proto{Factory: ProtoReplicaDescFactory}
				mock := kv.PrefixClient(consul.NewInMemoryClient(codec), "prefix")

				if tc.enableTracker {
					r, err := newClusterTracker(HATrackerConfig{
						EnableHATracker: true,
						KVStore:         kv.Config{Mock: mock},
						UpdateTimeout:   100 * time.Millisecond,
						FailoverTimeout: time.Second,
					})
					assert.NoError(t, err)
					d.Replicas = r
				}

				userID, err := user.ExtractOrgID(ctx)
				assert.NoError(t, err)
				err = d.Replicas.checkReplica(ctx, userID, tc.cluster, tc.acceptedReplica)
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

		// Test with between 3 and 10 ingesters.
		for numIngesters := 3; numIngesters < 10; numIngesters++ {

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
			d, _ := prepare(t, tc.numIngesters, tc.happyIngesters, 0, tc.shardByAllLabels, nil, nil)
			defer d.Stop()

			request := makeWriteRequest(tc.samples)
			writeResponse, err := d.Push(ctx, request)
			assert.Equal(t, &client.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			response, err := d.Query(ctx, 0, 10, tc.matchers...)
			sort.Sort(response)
			assert.Equal(t, tc.expectedResponse, response)
			assert.Equal(t, tc.expectedError, err)

			series, err := d.QueryStream(ctx, 0, 10, tc.matchers...)
			assert.Equal(t, tc.expectedError, err)

			response, err = chunkcompat.SeriesChunksToMatrix(0, 10, series)
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
		{ // Remove both cluster and replica label.
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
			},
			removeReplica: true,
			removeLabels:  []string{"cluster"},
		},
		{ // Remove multiple labels and replica.
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
			removeReplica: true,
			removeLabels:  []string{"foo", "some"},
		},
		{ // Don't remove any labels.
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			removeReplica: false,
		},
	}

	for _, tc := range cases {
		var err error
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.DropLabels = tc.removeLabels
		limits.AcceptHASamples = tc.removeReplica

		d, ingesters := prepare(t, 1, 1, 0, true, &limits, nil)
		defer d.Stop()

		// Push the series to the distributor
		req := mockWriteRequest(tc.inputSeries, 1, 1)
		_, err = d.Push(ctx, req)
		require.NoError(t, err)

		// Since each test pushes only 1 series, we do expect the ingester
		// to have received exactly 1 series
		assert.Equal(t, 1, len(ingesters))
		actualSeries := []*client.PreallocTimeseries{}

		for _, ts := range ingesters[0].timeseries {
			actualSeries = append(actualSeries, ts)
		}

		assert.Equal(t, 1, len(actualSeries))
		assert.Equal(t, tc.expectedSeries, client.FromLabelAdaptersToLabels(actualSeries[0].Labels))
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
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
			},
			expectedToken: 0x58b1e325,
		},
		"metric_1 with value_1 and dropped label due to config": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "dropped", Value: "unused"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
			},
			expectedToken: 0x58b1e325,
		},
		"metric_1 with value_1 and dropped HA replica label": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "__replica__", Value: "replica_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_1"},
				{Name: "cluster", Value: "cluster_1"},
			},
			expectedToken: 0x58b1e325,
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
			d, ingesters := prepare(t, 1, 1, 0, true, &limits, nil)
			defer d.Stop()

			// Push the series to the distributor
			req := mockWriteRequest(testData.inputSeries, 1, 1)
			_, err := d.Push(ctx, req)
			require.NoError(t, err)

			// Since each test pushes only 1 series, we do expect the ingester
			// to have received exactly 1 series
			require.Equal(t, 1, len(ingesters))
			require.Equal(t, 1, len(ingesters[0].timeseries))

			var actualSeries *client.PreallocTimeseries
			var actualToken uint32

			for token, ts := range ingesters[0].timeseries {
				actualSeries = ts
				actualToken = token
			}

			// Ensure the series and the sharding token is the expected one
			assert.Equal(t, testData.expectedSeries, client.FromLabelAdaptersToLabels(actualSeries.Labels))
			assert.Equal(t, testData.expectedToken, actualToken)
		})
	}
}

func TestSlowQueries(t *testing.T) {
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	nIngesters := 3
	for _, shardByAllLabels := range []bool{true, false} {
		for happy := 0; happy <= nIngesters; happy++ {
			var expectedErr error
			if nIngesters-happy > 1 {
				expectedErr = promql.ErrStorage{Err: errFail}
			}
			d, _ := prepare(t, nIngesters, happy, 100*time.Millisecond, shardByAllLabels, nil, nil)
			defer d.Stop()

			_, err := d.Query(ctx, 0, 10, nameMatcher)
			assert.Equal(t, expectedErr, err)

			_, err = d.QueryStream(ctx, 0, 10, nameMatcher)
			assert.Equal(t, expectedErr, err)
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
	d, _ := prepare(t, 3, 3, time.Duration(0), true, nil, nil)
	defer d.Stop()

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := d.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			now := model.Now()

			metrics, err := d.MetricsForLabelMatchers(ctx, now, now, testData.matchers...)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, metrics)
		})
	}
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

	return client.ToWriteRequest([]labels.Labels{lbls}, samples, client.API)
}

func prepare(t *testing.T, numIngesters, happyIngesters int, queryDelay time.Duration, shardByAllLabels bool, limits *validation.Limits, kvStore kv.Client) (*Distributor, []mockIngester) {
	ingesters := []mockIngester{}
	for i := 0; i < happyIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			happy:      true,
			queryDelay: queryDelay,
		})
	}
	for i := happyIngesters; i < numIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			queryDelay: queryDelay,
		})
	}

	// Mock the ingesters ring
	ingesterDescs := []ring.IngesterDesc{}
	ingestersByAddr := map[string]*mockIngester{}
	for i := range ingesters {
		addr := fmt.Sprintf("%d", i)
		ingesterDescs = append(ingesterDescs, ring.IngesterDesc{
			Addr:      addr,
			Timestamp: time.Now().Unix(),
		})
		ingestersByAddr[addr] = &ingesters[i]
	}

	ingestersRing := mockRing{
		Counter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "foo",
		}),
		ingesters:         ingesterDescs,
		replicationFactor: 3,
	}

	factory := func(addr string) (grpc_health_v1.HealthClient, error) {
		return ingestersByAddr[addr], nil
	}

	var cfg Config
	var clientConfig client.Config
	flagext.DefaultValues(&cfg, &clientConfig)

	if limits == nil {
		limits = &validation.Limits{}
		flagext.DefaultValues(limits)
	}
	cfg.ingesterClientFactory = factory
	cfg.ShardByAllLabels = shardByAllLabels
	cfg.ExtraQueryDelay = 50 * time.Millisecond
	cfg.DistributorRing.HeartbeatPeriod = 100 * time.Millisecond
	cfg.DistributorRing.InstanceID = strconv.Itoa(rand.Int())
	cfg.DistributorRing.KVStore.Mock = kvStore

	overrides, err := validation.NewOverrides(*limits)
	require.NoError(t, err)

	d, err := New(cfg, clientConfig, overrides, ingestersRing, true)
	require.NoError(t, err)

	return d, ingesters
}

func makeWriteRequest(samples int) *client.WriteRequest {
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
				TimestampMs: int64(i),
			},
		}
		request.Timeseries = append(request.Timeseries, ts)
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
					{Name: "bar", Value: "baz"},
					{Name: "sample", Value: fmt.Sprintf("%d", i)},
					{Name: "__replica__", Value: replica},
					{Name: "cluster", Value: cluster},
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

// mockRing doesn't do virtual nodes, just returns mod(key) + replicationFactor
// ingesters.
type mockRing struct {
	prometheus.Counter
	ingesters         []ring.IngesterDesc
	replicationFactor uint32
}

func (r mockRing) Get(key uint32, op ring.Operation, buf []ring.IngesterDesc) (ring.ReplicationSet, error) {
	result := ring.ReplicationSet{
		MaxErrors: 1,
		Ingesters: buf[:0],
	}
	for i := uint32(0); i < r.replicationFactor; i++ {
		n := (key + i) % uint32(len(r.ingesters))
		result.Ingesters = append(result.Ingesters, r.ingesters[n])
	}
	return result, nil
}

func (r mockRing) GetAll() (ring.ReplicationSet, error) {
	return ring.ReplicationSet{
		Ingesters: r.ingesters,
		MaxErrors: 1,
	}, nil
}

func (r mockRing) ReplicationFactor() int {
	return int(r.replicationFactor)
}

func (r mockRing) IngesterCount() int {
	return len(r.ingesters)
}

type mockIngester struct {
	sync.Mutex
	client.IngesterClient
	grpc_health_v1.HealthClient
	happy      bool
	stats      client.UsersStatsResponse
	timeseries map[uint32]*client.PreallocTimeseries
	queryDelay time.Duration
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

	orgid, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	for j := range req.Timeseries {
		series := req.Timeseries[j]
		hash, _ := shardByAllLabels(orgid, series.Labels)
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
			Timeseries: []client.TimeSeriesChunk{
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
		labels  []labels.Labels
		samples []client.Sample
		err     error
	}{
		// Test validation passes.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
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
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)

			limits.CreationGracePeriod = 2 * time.Hour
			limits.RejectOldSamples = true
			limits.RejectOldSamplesMaxAge = 24 * time.Hour
			limits.MaxLabelNamesPerSeries = 2

			d, _ := prepare(t, 3, 3, 0, true, &limits, nil)
			defer d.Stop()

			_, err := d.Push(ctx, client.ToWriteRequest(tc.labels, tc.samples, client.API))
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
