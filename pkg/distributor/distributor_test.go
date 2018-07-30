package distributor

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
	"github.com/weaveworks/cortex/pkg/util"
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
				d := prepare(t, tc.numIngesters, tc.happyIngesters, shardByAllLabels)
				defer d.Stop()

				request := makeWriteRequest(tc.samples)
				response, err := d.Push(ctx, request)
				assert.Equal(t, tc.expectedResponse, response)
				assert.Equal(t, tc.expectedError, err)
			})
		}
	}
}

func TestDistributorPushQuery(t *testing.T) {
	nameMatcher := mustEqualMatcher("__name__", "foo")
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
						expectedError:    errFail,
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
			d := prepare(t, tc.numIngesters, tc.happyIngesters, tc.shardByAllLabels)
			defer d.Stop()

			request := makeWriteRequest(tc.samples)
			writeResponse, err := d.Push(ctx, request)
			assert.Equal(t, &client.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			response, err := d.Query(ctx, 0, 10, tc.matchers...)
			sort.Sort(response)
			assert.Equal(t, tc.expectedResponse, response)
			assert.Equal(t, tc.expectedError, err)
		})
	}
}

func prepare(t *testing.T, numIngesters, happyIngesters int, shardByAllLabels bool) *Distributor {
	ingesters := []mockIngester{}
	for i := 0; i < happyIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			happy: true,
		})
	}
	for i := happyIngesters; i < numIngesters; i++ {
		ingesters = append(ingesters, mockIngester{})
	}

	ingesterDescs := []*ring.IngesterDesc{}
	ingestersByAddr := map[string]*mockIngester{}
	for i := range ingesters {
		addr := fmt.Sprintf("%d", i)
		ingesterDescs = append(ingesterDescs, &ring.IngesterDesc{
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
	util.DefaultValues(&cfg)
	cfg.IngestionRateLimit = 20
	cfg.IngestionBurstSize = 20
	cfg.ingesterClientFactory = factory
	cfg.ShardByAllLabels = shardByAllLabels

	d, err := New(cfg, ring)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func makeWriteRequest(samples int) *client.WriteRequest {
	request := &client.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := client.PreallocTimeseries{
			TimeSeries: client.TimeSeries{
				Labels: []client.LabelPair{
					{Name: []byte("__name__"), Value: []byte("foo")},
					{Name: []byte("bar"), Value: []byte("baz")},
					{Name: []byte("sample"), Value: []byte(fmt.Sprintf("%d", i))},
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
				"__name__": "foo",
				"bar":      "baz",
				"sample":   model.LabelValue(fmt.Sprintf("%d", i)),
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
	ingesters         []*ring.IngesterDesc
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
	happy      bool
	stats      client.UsersStatsResponse
	timeseries map[uint32]*client.PreallocTimeseries
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

func (i *mockIngester) AllUserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}

func match(labels []client.LabelPair, matchers []*labels.Matcher) bool {
outer:
	for _, matcher := range matchers {
		for _, labels := range labels {
			if matcher.Name == string(labels.Name) && matcher.Matches(string(labels.Value)) {
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
			err: httpgrpc.Errorf(http.StatusBadRequest, "sample for 'testmetric' has 3 label names; limit 2"),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			d := prepare(t, 3, 3, true)
			defer d.Stop()

			d.cfg.validationConfig.CreationGracePeriod = 2 * time.Hour
			d.cfg.validationConfig.RejectOldSamples = true
			d.cfg.validationConfig.RejectOldSamplesMaxAge = 24 * time.Hour
			d.cfg.validationConfig.MaxLabelNamesPerSeries = 2

			_, err := d.Push(ctx, client.ToWriteRequest(tc.samples))
			require.Equal(t, tc.err, err)
		})
	}
}
