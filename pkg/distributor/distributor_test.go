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
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
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
		for _, shardByMetricName := range []bool{true, false} {
			t.Run(fmt.Sprintf("[%d](shardByMetricName=%v)", i, shardByMetricName), func(t *testing.T) {
				d := prepare(t, tc.numIngesters, tc.happyIngesters, shardByMetricName)
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
		name              string
		numIngesters      int
		happyIngesters    int
		samples           int
		matchers          []*labels.Matcher
		expectedResponse  model.Matrix
		expectedError     error
		shardByMetricName bool
	}

	// We'll programatically build the test cases now, as we want complete
	// coverage along quite a few different axis.
	testcases := []testcase{}

	// Run every test in both sharding modes.
	for _, shardByMetricName := range []bool{true, false} {

		// Test with between 3 and 10 ingesters.
		for numIngesters := 3; numIngesters < 10; numIngesters++ {

			// Test with between 0 and numIngesters "happy" ingesters.
			for happyIngesters := 0; happyIngesters <= numIngesters; happyIngesters++ {

				// Queriers with more than one failed ingester should fail.
				if numIngesters-happyIngesters > 1 {
					testcases = append(testcases, testcase{
						name:              fmt.Sprintf("ExpectFail(shardByMetricName=%v,numIngester=%d,happyIngester=%d)", shardByMetricName, numIngesters, happyIngesters),
						numIngesters:      numIngesters,
						happyIngesters:    happyIngesters,
						matchers:          []*labels.Matcher{nameMatcher, barMatcher},
						expectedError:     errFail,
						shardByMetricName: shardByMetricName,
					})
					continue
				}

				// Reading all the samples back should succeed.
				testcases = append(testcases, testcase{
					name:              fmt.Sprintf("ReadAll(shardByMetricName=%v,numIngester=%d,happyIngester=%d)", shardByMetricName, numIngesters, happyIngesters),
					numIngesters:      numIngesters,
					happyIngesters:    happyIngesters,
					samples:           10,
					matchers:          []*labels.Matcher{nameMatcher, barMatcher},
					expectedResponse:  expectedResponse(0, 10),
					shardByMetricName: shardByMetricName,
				})

				// As should reading none of the samples back.
				testcases = append(testcases, testcase{
					name:              fmt.Sprintf("ReadNone(shardByMetricName=%v,numIngester=%d,happyIngester=%d)", shardByMetricName, numIngesters, happyIngesters),
					numIngesters:      numIngesters,
					happyIngesters:    happyIngesters,
					samples:           10,
					matchers:          []*labels.Matcher{nameMatcher, mustEqualMatcher("not", "found")},
					expectedResponse:  expectedResponse(0, 0),
					shardByMetricName: shardByMetricName,
				})

				// And reading each sample individually.
				for i := 0; i < 10; i++ {
					testcases = append(testcases, testcase{
						name:              fmt.Sprintf("ReadOne(shardByMetricName=%v, sample=%d,numIngester=%d,happyIngester=%d)", shardByMetricName, i, numIngesters, happyIngesters),
						numIngesters:      numIngesters,
						happyIngesters:    happyIngesters,
						samples:           10,
						matchers:          []*labels.Matcher{nameMatcher, mustEqualMatcher("sample", strconv.Itoa(i))},
						expectedResponse:  expectedResponse(i, i+1),
						shardByMetricName: shardByMetricName,
					})
				}
			}
		}
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			d := prepare(t, tc.numIngesters, tc.happyIngesters, tc.shardByMetricName)
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

func prepare(t *testing.T, numIngesters, happyIngesters int, shardByMetricName bool) *Distributor {
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

	factory := func(addr string, _ client.Config) (client.IngesterClient, error) {
		return ingestersByAddr[addr], nil
	}

	d, err := New(Config{
		RemoteTimeout:         1 * time.Minute,
		ClientCleanupPeriod:   1 * time.Minute,
		IngestionRateLimit:    20,
		IngestionBurstSize:    20,
		ingesterClientFactory: factory,
		ShardByMetricName:     true,
	}, ring)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func makeWriteRequest(samples int) *client.WriteRequest {
	request := &client.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := client.TimeSeries{
			Labels: []client.LabelPair{
				{Name: []byte("__name__"), Value: []byte("foo")},
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("sample"), Value: []byte(fmt.Sprintf("%d", i))},
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
	timeseries map[uint32]*client.TimeSeries
}

func (i *mockIngester) Push(ctx context.Context, req *client.WriteRequest, opts ...grpc.CallOption) (*client.WriteResponse, error) {
	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	if i.timeseries == nil {
		i.timeseries = map[uint32]*client.TimeSeries{}
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
			response.Timeseries = append(response.Timeseries, *ts)
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
