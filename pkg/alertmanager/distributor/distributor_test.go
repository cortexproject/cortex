package distributor

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cortexproject/cortex/pkg/alertmanager"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestDistributor_ServeHTTP(t *testing.T) {
	amPrefix := "/alertmanager"
	cases := []struct {
		name               string
		numAM, numHappyAM  int
		replicationFactor  int
		route              string
		metricNames        []string
		expStatusCode      int
		expectedTotalCalls int
		expectedMetrics    string
	}{
		{
			name:               "Simple AM request, all AM healthy",
			numAM:              4,
			numHappyAM:         4,
			replicationFactor:  3,
			route:              amPrefix + "/api/v1/alerts",
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
			route:              amPrefix + "/api/v1/alerts",
			expStatusCode:      http.StatusInternalServerError,
			expectedTotalCalls: 0,
		}, {
			name:               "Less than quorum AM succeed",
			numAM:              5,
			numHappyAM:         2, // Though we have 2 happy, it will hit 2 unhappy AM.
			replicationFactor:  3,
			route:              amPrefix + "/api/v1/alerts",
			expStatusCode:      http.StatusInternalServerError,
			expectedTotalCalls: 3,
		}, {
			name:               "Request without AM prefix is sent to all AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			route:              "/not_am_prefix/foo",
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 5,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, ams, _, cleanup := prepare(t, c.numAM, c.numHappyAM, c.replicationFactor, amPrefix)
			t.Cleanup(cleanup)

			ctx := user.InjectOrgID(context.Background(), "1")

			url := "http://127.0.0.1:9999" + c.route
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
			require.NoError(t, err)

			w := httptest.NewRecorder()
			d.ServeHTTP(w, req)
			resp := w.Result()

			require.Equal(t, c.expStatusCode, resp.StatusCode)

			totalReqCount := 0
			for _, a := range ams {
				routesReceived := len(a.receivedRequests)
				if routesReceived == 0 {
					// This AM did not receive any requests.
					continue
				}

				require.Equal(t, 1, routesReceived)
				routeMap, ok := a.receivedRequests[c.route]
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

			require.Equal(t, c.expectedTotalCalls, totalReqCount)

			// TODO(codesome): Not getting any metrics here. Investigate.
			//if c.expectedMetrics != "" {
			//	test.Poll(t, time.Second, nil, func() interface{} {
			//		return testutil.GatherAndCompare(reg, strings.NewReader(c.expectedMetrics), c.metricNames...)
			//	})
			//}
		})
	}

}

func prepare(t *testing.T, numAM, numHappyAM, replicationFactor int, amPrefix string) (*Distributor, []*mockAlertmanager, *prometheus.Registry, func()) {
	ams := []*mockAlertmanager{}
	for i := 0; i < numHappyAM; i++ {
		ams = append(ams, newMockAlertmanager(t, i, true))
	}
	for i := numHappyAM; i < numAM; i++ {
		ams = append(ams, newMockAlertmanager(t, i, false))
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	amDescs := map[string]ring.IngesterDesc{}
	for i, a := range ams {
		amDescs[a.myAddr] = ring.IngesterDesc{
			Addr:                a.myAddr,
			Zone:                "",
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              []uint32{uint32((math.MaxUint32 / numAM) * i)},
		}
	}

	kvStore := consul.NewInMemoryClient(ring.GetCodec())
	err := kvStore.CAS(context.Background(), alertmanager.RingKey,
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
	}, alertmanager.RingNameForServer, alertmanager.RingKey, nil)
	require.NoError(t, err)

	var distributorCfg Config
	flagext.DefaultValues(&distributorCfg)

	reg := prometheus.NewRegistry()
	d, err := New(distributorCfg, amPrefix, amRing, amRing, reg, util.Logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), d))

	test.Poll(t, time.Second, numAM, func() interface{} {
		return amRing.IngesterCount()
	})

	return d, ams, reg, func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), d))
		for _, a := range ams {
			a.close()
		}
	}
}

type mockAlertmanager struct {
	t *testing.T
	// receivedRequests is map of route -> statusCode -> number of requests.
	receivedRequests map[string]map[int]int
	myAddr           string
	happy            bool
	server           *http.Server
}

func newMockAlertmanager(t *testing.T, idx int, happy bool) *mockAlertmanager {
	am := &mockAlertmanager{
		t:                t,
		receivedRequests: make(map[string]map[int]int),
		myAddr:           fmt.Sprintf("127.0.0.1:%05d", 10000+idx),
		happy:            happy,
	}
	am.server = &http.Server{Addr: am.myAddr, Handler: am}
	go func() {
		require.Equal(t, http.ErrServerClosed, am.server.ListenAndServe())
	}()
	return am
}

func (am *mockAlertmanager) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	m, ok := am.receivedRequests[req.URL.Path]
	if !ok {
		m = make(map[int]int)
		am.receivedRequests[req.URL.Path] = m
	}

	if am.happy {
		m[http.StatusOK]++
		w.WriteHeader(http.StatusOK)
		return
	}

	m[http.StatusInternalServerError]++
	w.WriteHeader(http.StatusInternalServerError)
}

func (am *mockAlertmanager) close() {
	require.NoError(am.t, am.server.Close())
}
