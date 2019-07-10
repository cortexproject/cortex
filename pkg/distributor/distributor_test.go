package distributor

import (
	"bytes"
	"fmt"
	"io"
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
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util/chunkcompat"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

var (
	errFail = fmt.Errorf("Fail")
	success = &client.WriteResponse{}
	ctx     = user.InjectOrgID(context.Background(), "user")
)

func TestDistributorPush(t *testing.T) {
	for i, tc := range []struct {
		numIngesters     int
		happyIngesters   int
		samples          int
		expectedResponse *client.WriteResponse
		expectedError    error
	}{
		// A push of no samples shouldn't block or return error, even if ingesters are sad
		{
			numIngesters:     3,
			happyIngesters:   0,
			expectedResponse: success,
		},

		// A push to 3 happy ingesters should succeed
		{
			numIngesters:     3,
			happyIngesters:   3,
			samples:          10,
			expectedResponse: success,
		},

		// A push to 2 happy ingesters should succeed
		{
			numIngesters:     3,
			happyIngesters:   2,
			samples:          10,
			expectedResponse: success,
		},

		// A push to 1 happy ingesters should fail
		{
			numIngesters:   3,
			happyIngesters: 1,
			samples:        10,
			expectedError:  errFail,
		},

		// A push to 0 happy ingesters should fail
		{
			numIngesters:   3,
			happyIngesters: 0,
			samples:        10,
			expectedError:  errFail,
		},

		// A push exceeding burst size should fail
		{
			numIngesters:   3,
			happyIngesters: 3,
			samples:        30,
			expectedError:  httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (20) exceeded while adding 30 samples"),
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%d](shardByAllLabels=%v)", i, shardByAllLabels), func(t *testing.T) {
				d := prepare(t, tc.numIngesters, tc.happyIngesters, 0, shardByAllLabels)
				defer d.Stop()

				request := makeWriteRequest(tc.samples)
				response, err := d.Push(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)
				assert.Equal(t, tc.expectedError, err)
			})
		}
	}
}

func TestDistributorPushHAInstances(t *testing.T) {
	ctx = user.InjectOrgID(context.Background(), "user")

	for i, tc := range []struct {
		acceptedReplica  string
		testReplica      string
		cluster          string
		samples          int
		expectedResponse *client.WriteResponse
		expectedCode     int32
	}{
		{
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: success,
		},
		// The 202 indicates that we didn't accept this sample.
		{
			acceptedReplica: "instance2",
			testReplica:     "instance0",
			cluster:         "cluster0",
			samples:         5,
			expectedCode:    202,
		},
	} {
		for _, shardByAllLabels := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%d](shardByAllLabels=%v)", i, shardByAllLabels), func(t *testing.T) {
				d := prepare(t, 1, 1, 0, shardByAllLabels)
				d.cfg.EnableHAReplicas = true
				d.limits.Defaults.AcceptHASamples = true
				codec := ring.ProtoCodec{Factory: ProtoReplicaDescFactory}
				mock := ring.PrefixClient(ring.NewInMemoryKVClient(codec), "prefix")

				r, err := newClusterTracker(HATrackerConfig{
					KVStore:         ring.KVConfig{Mock: mock},
					UpdateTimeout:   100 * time.Millisecond,
					FailoverTimeout: time.Second,
				})
				assert.NoError(t, err)
				d.replicas = r

				userID, err := user.ExtractOrgID(ctx)
				assert.NoError(t, err)
				err = d.replicas.checkReplica(ctx, userID, tc.cluster, tc.acceptedReplica)
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

func TestDistributorPushQuery(t *testing.T) {
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
			d := prepare(t, tc.numIngesters, tc.happyIngesters, 0, tc.shardByAllLabels)
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

func TestSlowQueries(t *testing.T) {
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	nIngesters := 3
	for _, shardByAllLabels := range []bool{true, false} {
		for happy := 0; happy <= nIngesters; happy++ {
			var expectedErr error
			if nIngesters-happy > 1 {
				expectedErr = promql.ErrStorage{Err: errFail}
			}
			d := prepare(t, nIngesters, happy, 100*time.Millisecond, shardByAllLabels)
			defer d.Stop()

			_, err := d.Query(ctx, 0, 10, nameMatcher)
			assert.Equal(t, expectedErr, err)

			_, err = d.QueryStream(ctx, 0, 10, nameMatcher)
			assert.Equal(t, expectedErr, err)
		}
	}
}

func prepare(t *testing.T, numIngesters, happyIngesters int, queryDelay time.Duration, shardByAllLabels bool) *Distributor {
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

	ring := mockRing{
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
	var limits validation.Limits
	var clientConfig client.Config
	flagext.DefaultValues(&cfg, &limits, &clientConfig)
	limits.IngestionRate = 20
	limits.IngestionBurstSize = 20
	cfg.ingesterClientFactory = factory
	cfg.ShardByAllLabels = shardByAllLabels
	cfg.ExtraQueryDelay = 50 * time.Millisecond

	overrides, err := validation.NewOverrides(limits)
	require.NoError(t, err)

	d, err := New(cfg, clientConfig, overrides, ring)
	require.NoError(t, err)

	return d
}

func makeWriteRequest(samples int) *client.WriteRequest {
	request := &client.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := client.PreallocTimeseries{
			TimeSeries: client.TimeSeries{
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
			TimeSeries: client.TimeSeries{
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

func (r mockRing) Get(key uint32, op ring.Operation) (ring.ReplicationSet, error) {
	result := ring.ReplicationSet{
		MaxErrors: 1,
	}
	for i := uint32(0); i < r.replicationFactor; i++ {
		n := (key + i) % uint32(len(r.ingesters))
		result.Ingesters = append(result.Ingesters, r.ingesters[n])
	}
	return result, nil
}

func (r mockRing) BatchGet(keys []uint32, op ring.Operation) ([]ring.ReplicationSet, error) {
	result := []ring.ReplicationSet{}
	for i := 0; i < len(keys); i++ {
		rs, err := r.Get(keys[i], op)
		if err != nil {
			return nil, err
		}
		result = append(result, rs)
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
		hash, _ := shardByAllLabels(orgid, req.Timeseries[j].Labels)
		existing, ok := i.timeseries[hash]
		if !ok {
			i.timeseries[hash] = &req.Timeseries[j]
		} else {
			existing.Samples = append(existing.Samples, req.Timeseries[j].Samples...)
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
			response.Timeseries = append(response.Timeseries, ts.TimeSeries)
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
		for _, sample := range ts.Samples {
			cs, err := c.Add(model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
			if err != nil {
				panic(err)
			}
			c = cs[0]
		}

		var buf bytes.Buffer
		chunk := client.Chunk{
			Encoding: int32(c.Encoding()),
		}
		if err := c.Marshal(&buf); err != nil {
			panic(err)
		}
		chunk.Data = buf.Bytes()

		results = append(results, &client.QueryStreamResponse{
			Timeseries: []client.TimeSeriesChunk{
				{
					Labels: ts.Labels,
					Chunks: []client.Chunk{chunk},
				},
			},
		})
	}
	return &stream{
		results: results,
	}, nil
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
		samples []model.Sample
		err     error
	}{
		// Test validation passes.
		{
			samples: []model.Sample{{
				Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
				Timestamp: now,
				Value:     1,
			}},
		},

		// Test validation fails for very old samples.
		{
			samples: []model.Sample{{
				Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
				Timestamp: past,
				Value:     2,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, "sample for 'testmetric' has timestamp too old: %d", past),
		},

		// Test validation fails for samples from the future.
		{
			samples: []model.Sample{{
				Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar"},
				Timestamp: future,
				Value:     4,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, "sample for 'testmetric' has timestamp too new: %d", future),
		},

		// Test maximum labels names per series.
		{
			samples: []model.Sample{{
				Metric:    model.Metric{model.MetricNameLabel: "testmetric", "foo": "bar", "foo2": "bar2"},
				Timestamp: now,
				Value:     2,
			}},
			err: httpgrpc.Errorf(http.StatusBadRequest, `sample for 'testmetric{foo2="bar2", foo="bar"}' has 3 label names; limit 2`),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			d := prepare(t, 3, 3, 0, true)
			defer d.Stop()

			d.limits.Defaults.CreationGracePeriod = 2 * time.Hour
			d.limits.Defaults.RejectOldSamples = true
			d.limits.Defaults.RejectOldSamplesMaxAge = 24 * time.Hour
			d.limits.Defaults.MaxLabelNamesPerSeries = 2

			_, err := d.Push(ctx, client.ToWriteRequest(tc.samples, client.API))
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
		removeReplicaLabel(replicaLabel, &c.labelsIn)
		assert.Equal(t, c.labelsOut, c.labelsIn)
	}
}
