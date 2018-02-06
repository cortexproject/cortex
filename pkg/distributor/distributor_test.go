package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/ring"
)

// mockRing doesn't do any consistent hashing, just returns same ingesters for every query.
type mockRing struct {
	prometheus.Counter
	ingesters        []*ring.IngesterDesc
	heartbeatTimeout time.Duration
}

func (r mockRing) Get(key uint32, op ring.Operation) (ring.ReplicationSet, error) {
	return ring.ReplicationSet{
		Ingesters: r.ingesters[:3],
		MaxErrors: 1,
	}, nil
}

func (r mockRing) BatchGet(keys []uint32, op ring.Operation) ([]ring.ReplicationSet, error) {
	result := []ring.ReplicationSet{}
	for i := 0; i < len(keys); i++ {
		result = append(result, ring.ReplicationSet{
			Ingesters: r.ingesters[:3],
			MaxErrors: 1,
		})
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
	return 3
}

type mockIngester struct {
	client.IngesterClient
	happy bool
	stats client.UsersStatsResponse
}

func (i mockIngester) Push(ctx context.Context, in *client.WriteRequest, opts ...grpc.CallOption) (*client.WriteResponse, error) {
	if !i.happy {
		return nil, fmt.Errorf("Fail")
	}
	return &client.WriteResponse{}, nil
}

func (i mockIngester) Query(ctx context.Context, in *client.QueryRequest, opts ...grpc.CallOption) (*client.QueryResponse, error) {
	if !i.happy {
		return nil, fmt.Errorf("Fail")
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
			expectedResponse: &client.WriteResponse{},
		},

		// A push to 3 happy ingesters should succeed
		{
			samples: 10,
			ingesters: []mockIngester{
				{happy: true},
				{happy: true},
				{happy: true},
			},
			expectedResponse: &client.WriteResponse{},
		},

		// A push to 2 happy ingesters should succeed
		{
			samples: 10,
			ingesters: []mockIngester{
				{},
				{happy: true},
				{happy: true},
			},
			expectedResponse: &client.WriteResponse{},
		},

		// A push to 1 happy ingesters should fail
		{
			samples: 10,
			ingesters: []mockIngester{
				{},
				{},
				{happy: true},
			},
			expectedError: fmt.Errorf("Fail"),
		},

		// A push to 0 happy ingesters should fail
		{
			samples:       10,
			ingesters:     []mockIngester{{}, {}, {}},
			expectedError: fmt.Errorf("Fail"),
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			ingesterDescs := []*ring.IngesterDesc{}
			ingesters := map[string]mockIngester{}
			for i, ingester := range tc.ingesters {
				addr := fmt.Sprintf("%d", i)
				ingesterDescs = append(ingesterDescs, &ring.IngesterDesc{
					Addr:      addr,
					Timestamp: time.Now().Unix(),
				})
				ingesters[addr] = ingester
			}

			ring := mockRing{
				Counter: prometheus.NewCounter(prometheus.CounterOpts{
					Name: "foo",
				}),
				ingesters:        ingesterDescs,
				heartbeatTimeout: 1 * time.Minute,
			}

			d, err := New(Config{
				RemoteTimeout:       1 * time.Minute,
				ClientCleanupPeriod: 1 * time.Minute,
				IngestionRateLimit:  10000,
				IngestionBurstSize:  10000,

				ingesterClientFactory: func(addr string, _ client.Config) (client.IngesterClient, error) {
					return ingesters[addr], nil
				},
			}, ring)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Stop()

			request := &client.WriteRequest{}
			for i := 0; i < tc.samples; i++ {
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

	expectedResponse := func(start, end int) model.Matrix {
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
			expectedError: fmt.Errorf("Fail"),
		},

		// A query to 0 happy ingesters should succeed
		{
			ingesters: []mockIngester{
				{happy: false},
				{happy: false},
				{happy: false},
			},
			expectedError: fmt.Errorf("Fail"),
		},
	} {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			ingesterDescs := []*ring.IngesterDesc{}
			ingesters := map[string]mockIngester{}
			for i, ingester := range tc.ingesters {
				addr := fmt.Sprintf("%d", i)
				ingesterDescs = append(ingesterDescs, &ring.IngesterDesc{
					Addr:      addr,
					Timestamp: time.Now().Unix(),
				})
				ingesters[addr] = ingester
			}

			ring := mockRing{
				Counter: prometheus.NewCounter(prometheus.CounterOpts{
					Name: "foo",
				}),
				ingesters:        ingesterDescs,
				heartbeatTimeout: 1 * time.Minute,
			}

			d, err := New(Config{
				RemoteTimeout:       1 * time.Minute,
				ClientCleanupPeriod: 1 * time.Minute,
				IngestionRateLimit:  10000,
				IngestionBurstSize:  10000,

				ingesterClientFactory: func(addr string, _ client.Config) (client.IngesterClient, error) {
					return ingesters[addr], nil
				},
			}, ring)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Stop()

			for _, matcher := range matchers {
				response, err := d.Query(ctx, 0, 10, matcher)
				assert.Equal(t, tc.expectedResponse, response, "Wrong response")
				assert.Equal(t, tc.expectedError, err, "Wrong error")
			}
		})
	}
}
