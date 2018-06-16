package distributor

import (
	"fmt"
	"net/http"
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
)

func TestDistributorPush(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	for i, tc := range []struct {
		ingesters        []mockIngester
		samples          int
		expectedResponse *client.WriteResponse
		expectedError    error
	}{
		// A push of no samples shouldn't block or return error, even if ingesters are sad
		{
			ingesters:        []mockIngester{{}, {}, {}},
			expectedResponse: success,
		},

		// A push to 3 happy ingesters should succeed
		{
			samples: 10,
			ingesters: []mockIngester{
				{happy: true},
				{happy: true},
				{happy: true},
			},
			expectedResponse: success,
		},

		// A push to 2 happy ingesters should succeed
		{
			samples: 10,
			ingesters: []mockIngester{
				{},
				{happy: true},
				{happy: true},
			},
			expectedResponse: success,
		},

		// A push to 1 happy ingesters should fail
		{
			samples: 10,
			ingesters: []mockIngester{
				{},
				{},
				{happy: true},
			},
			expectedError: errFail,
		},

		// A push to 0 happy ingesters should fail
		{
			samples:       10,
			ingesters:     []mockIngester{{}, {}, {}},
			expectedError: errFail,
		},

		// A push exceeding burst size should fail
		{
			samples: 30,
			ingesters: []mockIngester{
				{happy: true},
				{happy: true},
				{happy: true},
			},
			expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, "ingestion rate limit (20) exceeded while adding 30 samples"),
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			d := prepare(t, tc.ingesters)
			defer d.Stop()

			request := makeWriteRequest(tc.samples)
			response, err := d.Push(ctx, request)
			assert.Equal(t, tc.expectedResponse, response, "Wrong response")
			assert.Equal(t, tc.expectedError, err, "Wrong error")
		})
	}
}

func TestDistributorQuery(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	nameMatcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "foo")
	if err != nil {
		t.Fatal(err)
	}

	jobMatcher, err := labels.NewMatcher(labels.MatchEqual, "job", "foo")
	if err != nil {
		t.Fatal(err)
	}

	matchers := []*labels.Matcher{
		nameMatcher,
		jobMatcher,
	}

	for i, tc := range []struct {
		ingesters        []mockIngester
		expectedResponse model.Matrix
		expectedError    error
	}{
		// A query to 3 happy ingesters should succeed
		{
			ingesters: []mockIngester{
				{happy: true},
				{happy: true},
				{happy: true},
			},
			expectedResponse: expectedResponse(0, 2),
		},

		// A query to 2 happy ingesters should succeed
		{
			ingesters: []mockIngester{
				{happy: false},
				{happy: true},
				{happy: true},
			},
			expectedResponse: expectedResponse(0, 2),
		},

		// A query to 1 happy ingesters should fail
		{
			ingesters: []mockIngester{
				{happy: false},
				{happy: false},
				{happy: true},
			},
			expectedError: errFail,
		},

		// A query to 0 happy ingesters should succeed
		{
			ingesters: []mockIngester{
				{happy: false},
				{happy: false},
				{happy: false},
			},
			expectedError: errFail,
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			d := prepare(t, tc.ingesters)
			defer d.Stop()

			for _, matcher := range matchers {
				response, err := d.Query(ctx, 0, 10, matcher)
				assert.Equal(t, tc.expectedResponse, response, "Wrong response")
				assert.Equal(t, tc.expectedError, err, "Wrong error")
			}
		})
	}
}

func prepare(t *testing.T, ingesters []mockIngester) *Distributor {
	ingesterDescs := []*ring.IngesterDesc{}
	ingestersByAddr := map[string]mockIngester{}
	for i, ingester := range ingesters {
		addr := fmt.Sprintf("%d", i)
		ingesterDescs = append(ingesterDescs, &ring.IngesterDesc{
			Addr:      addr,
			Timestamp: time.Now().Unix(),
		})
		ingestersByAddr[addr] = ingester
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
	result := model.Matrix{
		&model.SampleStream{
			Metric: model.Metric{"__name__": "foo"},
		},
	}
	for i := start; i < end; i++ {
		result[0].Values = append(result[0].Values,
			model.SamplePair{
				Value:     model.SampleValue(i),
				Timestamp: model.Time(i),
			},
		)
	}
	return result
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
	client.IngesterClient
	happy bool
	stats client.UsersStatsResponse
}

func (i mockIngester) Push(ctx context.Context, in *client.WriteRequest, opts ...grpc.CallOption) (*client.WriteResponse, error) {
	if !i.happy {
		return nil, errFail
	}
	return &client.WriteResponse{}, nil
}

func (i mockIngester) Query(ctx context.Context, in *client.QueryRequest, opts ...grpc.CallOption) (*client.QueryResponse, error) {
	if !i.happy {
		return nil, errFail
	}
	return &client.QueryResponse{
		Timeseries: []client.TimeSeries{
			{
				Labels: []client.LabelPair{
					{
						Name:  []byte("__name__"),
						Value: []byte("foo"),
					},
				},
				Samples: []client.Sample{
					{
						Value:       0,
						TimestampMs: 0,
					},
					{
						Value:       1,
						TimestampMs: 1,
					},
				},
			},
		},
	}, nil
}

func (i mockIngester) AllUserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}
