package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/ring"
)

// mockRing doesn't do any consistent hashing, just returns same ingesters for every query.
type mockRing struct {
	prometheus.Counter
	ingesters []*ring.IngesterDesc
}

func (r mockRing) Get(key uint32, n int, op ring.Operation) ([]*ring.IngesterDesc, error) {
	return r.ingesters[:n], nil
}

func (r mockRing) BatchGet(keys []uint32, n int, op ring.Operation) ([][]*ring.IngesterDesc, error) {
	result := [][]*ring.IngesterDesc{}
	for i := 0; i < len(keys); i++ {
		result = append(result, r.ingesters[:n])
	}
	return result, nil
}

func (r mockRing) GetAll() []*ring.IngesterDesc {
	return r.ingesters
}

type mockIngester struct {
	happy bool
}

func (i mockIngester) Push(ctx context.Context, in *remote.WriteRequest, opts ...grpc.CallOption) (*cortex.WriteResponse, error) {
	if !i.happy {
		return nil, fmt.Errorf("Fail")
	}
	return &cortex.WriteResponse{}, nil
}

func (i mockIngester) Query(ctx context.Context, in *cortex.QueryRequest, opts ...grpc.CallOption) (*cortex.QueryResponse, error) {
	return nil, nil
}

func (i mockIngester) LabelValues(ctx context.Context, in *cortex.LabelValuesRequest, opts ...grpc.CallOption) (*cortex.LabelValuesResponse, error) {
	return nil, nil
}

func (i mockIngester) UserStats(ctx context.Context, in *cortex.UserStatsRequest, opts ...grpc.CallOption) (*cortex.UserStatsResponse, error) {
	return nil, nil
}

func (i mockIngester) MetricsForLabelMatchers(ctx context.Context, in *cortex.MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*cortex.MetricsForLabelMatchersResponse, error) {
	return nil, nil
}

func TestDistributor(t *testing.T) {
	ctx := user.WithID(context.Background(), "user")
	for i, tc := range []struct {
		ingesters        []mockIngester
		samples          int
		expectedResponse *cortex.WriteResponse
		expectedError    error
	}{
		// A push of no samples shouldn't block or return error, even if ingesters are sad
		{
			ingesters:        []mockIngester{{}, {}, {}},
			expectedResponse: &cortex.WriteResponse{},
		},

		// A push to 3 happy ingesters should succeed
		{
			samples:          10,
			ingesters:        []mockIngester{{true}, {true}, {true}},
			expectedResponse: &cortex.WriteResponse{},
		},

		// A push to 2 happy ingesters should succeed
		{
			samples:          10,
			ingesters:        []mockIngester{{}, {true}, {true}},
			expectedResponse: &cortex.WriteResponse{},
		},

		// A push to 1 happy ingesters should fail
		{
			samples:       10,
			ingesters:     []mockIngester{{}, {}, {true}},
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
				ingesters: ingesterDescs,
			}

			d, err := New(Config{
				ReplicationFactor:   3,
				MinReadSuccesses:    2,
				HeartbeatTimeout:    1 * time.Minute,
				RemoteTimeout:       1 * time.Minute,
				ClientCleanupPeriod: 1 * time.Minute,
				IngestionRateLimit:  10000,
				IngestionBurstSize:  10000,

				ingesterClientFactory: func(addr string) cortex.IngesterClient {
					return ingesters[addr]
				},
			}, ring)
			if err != nil {
				t.Fatal(err)
			}
			defer d.Stop()

			request := &remote.WriteRequest{}
			for i := 0; i < tc.samples; i++ {
				ts := &remote.TimeSeries{
					Labels: []*remote.LabelPair{
						{"__name__", "foo"},
						{"bar", "baz"},
						{"sample", fmt.Sprintf("%d", i)},
					},
				}
				ts.Samples = []*remote.Sample{
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
