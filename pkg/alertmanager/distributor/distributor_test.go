package distributor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	am_client "github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"
	"github.com/cortexproject/cortex/pkg/ring"
	ring_client "github.com/cortexproject/cortex/pkg/ring/client"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Copied constants from alertmanager package to avoid circular imports.
const (
	RingKey           = "alertmanager"
	RingNameForServer = "alertmanager"
)

func TestDistributor_DistributeRequest(t *testing.T) {
	cases := []struct {
		name               string
		numAM, numHappyAM  int
		replicationFactor  int
		metricNames        []string
		isRead             bool
		expStatusCode      int
		expectedTotalCalls int
		expectedMetrics    string
	}{
		{
			name:               "Simple AM request, all AM healthy",
			numAM:              4,
			numHappyAM:         4,
			replicationFactor:  3,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			metricNames:        []string{"alertmanager_distributor_received_requests_total"},
			expectedMetrics: `
				# HELP alertmanager_distributor_received_requests_total The total number of requests received.
				# TYPE alertmanager_distributor_received_requests_total counter
				alertmanager_distributor_received_requests_total{user="1"} 3
			`,
		}, {
			name:               "Less than quorum AM available",
			numAM:              1,
			numHappyAM:         1,
			replicationFactor:  3,
			expStatusCode:      http.StatusInternalServerError,
			expectedTotalCalls: 0,
		}, {
			name:               "Less than quorum AM succeed",
			numAM:              5,
			numHappyAM:         2, // Though we have 2 happy, it will hit 2 unhappy AM.
			replicationFactor:  3,
			expStatusCode:      http.StatusInternalServerError,
			expectedTotalCalls: 3,
		}, {
			name:               "Read is sent to only 1 AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 1,
		},
	}

	route := "/alertmanager/api/v1/alerts"
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ams, _, cleanup := prepare(t, c.numAM, c.numHappyAM, c.replicationFactor)
			t.Cleanup(cleanup)

			ctx := user.InjectOrgID(context.Background(), "1")

			url := "http://127.0.0.1:9999" + route
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte{1, 2, 3, 4}))
			require.NoError(t, err)
			if c.isRead {
				req.Method = http.MethodGet
			}
			req.RequestURI = url

			w := httptest.NewRecorder()
			d.DistributeRequest(w, req)
			resp := w.Result()

			require.Equal(t, c.expStatusCode, resp.StatusCode)

			// Since the response is sent as soon as the quorum is reached, when we
			// reach this point the 3rd AM may not have received the request yet.
			// To avoid flaky test we retry until we hit the desired state within a reasonable timeout.
			test.Poll(t, time.Second, true, func() interface{} {
				totalReqCount := 0
				for _, a := range ams {
					routesReceived := len(a.receivedRequests)
					if routesReceived == 0 {
						// This AM did not receive any requests.
						continue
					}

					require.Equal(t, 1, routesReceived)
					routeMap, ok := a.receivedRequests[route]
					require.True(t, ok)

					// The status could be something other than overall
					// expected status because of quorum logic.
					reqCount := 0
					for _, count := range routeMap {
						reqCount += count
					}
					// AM should not get duplicate requests.
					require.Equal(t, 1, reqCount)
					totalReqCount += reqCount
				}

				return c.expectedTotalCalls == totalReqCount
			})

			// TODO(codesome): Not getting any metrics here. Investigate.
			//if c.expectedMetrics != "" {
			//	test.Poll(t, time.Second, nil, func() interface{} {
			//		return testutil.GatherAndCompare(reg, strings.NewReader(c.expectedMetrics), c.metricNames...)
			//	})
			//}
		})
	}

}

func prepare(t *testing.T, numAM, numHappyAM, replicationFactor int) (*Distributor, []*mockAlertmanager, *prometheus.Registry, func()) {
	ams := []*mockAlertmanager{}
	for i := 0; i < numHappyAM; i++ {
		ams = append(ams, newMockAlertmanager(t, i, true))
	}
	for i := numHappyAM; i < numAM; i++ {
		ams = append(ams, newMockAlertmanager(t, i, false))
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	amDescs := map[string]ring.IngesterDesc{}
	amByAddr := map[string]*mockAlertmanager{}
	for i, a := range ams {
		amDescs[a.myAddr] = ring.IngesterDesc{
			Addr:                a.myAddr,
			Zone:                "",
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              []uint32{uint32((math.MaxUint32 / numAM) * i)},
		}
		amByAddr[a.myAddr] = ams[i]
	}

	kvStore := consul.NewInMemoryClient(ring.GetCodec())
	err := kvStore.CAS(context.Background(), RingKey,
		func(_ interface{}) (interface{}, bool, error) {
			return &ring.Desc{
				Ingesters: amDescs,
			}, true, nil
		},
	)
	require.NoError(t, err)

	amRing, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvStore,
		},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: replicationFactor,
	}, RingNameForServer, RingKey, nil)
	require.NoError(t, err)

	var distributorCfg Config
	flagext.DefaultValues(&distributorCfg)

	factory := func(addr string) (ring_client.PoolClient, error) {
		return amByAddr[addr], nil
	}
	distributorCfg.AlertmanagerClientFactory = factory

	reg := prometheus.NewRegistry()
	d, err := New(distributorCfg, amRing, util.Logger, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), d))

	test.Poll(t, time.Second, numAM, func() interface{} {
		return amRing.IngesterCount()
	})

	return d, ams, reg, func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), d))
	}
}

type mockAlertmanager struct {
	am_client.AlertmanagerClient
	grpc_health_v1.HealthClient
	t *testing.T
	// receivedRequests is map of route -> statusCode -> number of requests.
	receivedRequests map[string]map[int]int
	myAddr           string
	happy            bool
}

func newMockAlertmanager(t *testing.T, idx int, happy bool) *mockAlertmanager {
	return &mockAlertmanager{
		t:                t,
		receivedRequests: make(map[string]map[int]int),
		myAddr:           fmt.Sprintf("127.0.0.1:%05d", 10000+idx),
		happy:            happy,
	}
}

func (am *mockAlertmanager) HandleRequest(_ context.Context, in *am_client.Request, _ ...grpc.CallOption) (*am_client.Response, error) {
	u, err := url.Parse(in.HttpRequest.Url)
	require.NoError(am.t, err)
	path := u.Path
	m, ok := am.receivedRequests[path]
	if !ok {
		m = make(map[int]int)
		am.receivedRequests[path] = m
	}

	if am.happy {
		m[http.StatusOK]++
		return &am_client.Response{
			Status: am_client.OK,
			HttpResponse: &httpgrpc.HTTPResponse{
				Code: http.StatusOK,
			},
		}, nil
	}

	m[http.StatusInternalServerError]++
	return nil, errors.New("StatusInternalServerError")
}

func (am *mockAlertmanager) Close() error {
	return nil
}

func (am *mockAlertmanager) RemoteAddress() string {
	return am.myAddr
}
