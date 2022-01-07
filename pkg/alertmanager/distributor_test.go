package alertmanager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cortexproject/cortex/pkg/alertmanager/alertmanagerpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	util_log "github.com/cortexproject/cortex/pkg/util/log"
	"github.com/cortexproject/cortex/pkg/util/test"
)

func TestDistributor_DistributeRequest(t *testing.T) {
	cases := []struct {
		name                string
		numAM, numHappyAM   int
		replicationFactor   int
		isRead              bool
		isDelete            bool
		expStatusCode       int
		expectedTotalCalls  int
		headersNotPreserved bool
		route               string
		// Paths where responses are merged, we need to supply a valid response body.
		// Note that the actual merging logic is tested elsewhere (merger_test.go).
		responseBody []byte
	}{
		{
			name:               "Write /alerts, Simple AM request, all AM healthy",
			numAM:              4,
			numHappyAM:         4,
			replicationFactor:  3,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/alerts",
		}, {
			name:                "Write /alerts, Less than quorum AM available",
			numAM:               1,
			numHappyAM:          1,
			replicationFactor:   3,
			expStatusCode:       http.StatusInternalServerError,
			expectedTotalCalls:  0,
			headersNotPreserved: true, // There is nothing to preserve since it does not hit any AM.
			route:               "/alerts",
		}, {
			name:               "Write /alerts, Less than quorum AM succeed",
			numAM:              5,
			numHappyAM:         3, // Though we have 3 happy, it will hit >1 unhappy AM.
			replicationFactor:  3,
			expStatusCode:      http.StatusInternalServerError,
			expectedTotalCalls: 3,
			route:              "/alerts",
		}, {
			name:               "Read /v1/alerts is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v1/alerts",
			responseBody:       []byte(`{"status":"success","data":[]}`),
		}, {
			name:               "Read /v2/alerts is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v2/alerts",
			responseBody:       []byte(`[]`),
		}, {
			name:               "Read /v2/alerts/groups is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v2/alerts/groups",
			responseBody:       []byte(`[]`),
		}, {
			name:                "Read /v1/alerts/groups not supported",
			numAM:               5,
			numHappyAM:          5,
			replicationFactor:   3,
			expStatusCode:       http.StatusNotFound,
			expectedTotalCalls:  0,
			headersNotPreserved: true,
			route:               "/v1/alerts/groups",
		}, {
			name:                "Write /alerts/groups not supported",
			numAM:               5,
			numHappyAM:          5,
			replicationFactor:   3,
			expStatusCode:       http.StatusNotFound,
			expectedTotalCalls:  0,
			headersNotPreserved: true,
			route:               "/alerts/groups",
		}, {
			name:               "Read /v1/silences is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v1/silences",
			responseBody:       []byte(`{"status":"success","data":[]}`),
		}, {
			name:               "Read /v2/silences is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v2/silences",
			responseBody:       []byte(`[]`),
		}, {
			name:               "Write /silences is sent to only 1 AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 1,
			route:              "/silences",
		}, {
			name:               "Read /v1/silence/id is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v1/silence/id",
			responseBody:       []byte(`{"status":"success","data":{"id":"aaa","updatedAt":"2020-01-01T00:00:00Z"}}`),
		}, {
			name:               "Read /v2/silence/id is sent to 3 AMs",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 3,
			route:              "/v2/silence/id",
			responseBody:       []byte(`{"id":"aaa","updatedAt":"2020-01-01T00:00:00Z"}`),
		},
		{
			name:                "Write /silence/id not supported",
			numAM:               5,
			numHappyAM:          5,
			replicationFactor:   3,
			expStatusCode:       http.StatusNotFound,
			expectedTotalCalls:  0,
			headersNotPreserved: true,
			route:               "/silence/id",
		}, {
			name:               "Delete /silence/id is sent to only 1 AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isDelete:           true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 1,
			route:              "/silence/id",
		}, {
			name:               "Read /status is sent to only 1 AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 1,
			route:              "/status",
		}, {
			name:                "Write /status not supported",
			numAM:               5,
			numHappyAM:          5,
			replicationFactor:   3,
			expStatusCode:       http.StatusNotFound,
			expectedTotalCalls:  0,
			headersNotPreserved: true,
			route:               "/status",
		}, {
			name:               "Read /receivers is sent to only 1 AM",
			numAM:              5,
			numHappyAM:         5,
			replicationFactor:  3,
			isRead:             true,
			expStatusCode:      http.StatusOK,
			expectedTotalCalls: 1,
			route:              "/receivers",
		}, {
			name:                "Write /receivers not supported",
			numAM:               5,
			numHappyAM:          5,
			replicationFactor:   3,
			expStatusCode:       http.StatusNotFound,
			expectedTotalCalls:  0,
			headersNotPreserved: true,
			route:               "/receivers",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			route := "/alertmanager/api/v1" + c.route
			d, ams, cleanup := prepare(t, c.numAM, c.numHappyAM, c.replicationFactor, c.responseBody)
			t.Cleanup(cleanup)

			ctx := user.InjectOrgID(context.Background(), "1")

			url := "http://127.0.0.1:9999" + route
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader([]byte{1, 2, 3, 4}))
			require.NoError(t, err)
			if c.isRead {
				req.Method = http.MethodGet
			} else if c.isDelete {
				req.Method = http.MethodDelete
			}
			req.RequestURI = url

			w := httptest.NewRecorder()
			d.DistributeRequest(w, req)
			resp := w.Result()
			require.Equal(t, c.expStatusCode, resp.StatusCode)

			if !c.headersNotPreserved {
				// Making sure the headers are not altered.
				contentType := []string{"it-is-ok"}
				contentTypeOptions := []string{"ok-option-1", "ok-option-2"}
				if resp.StatusCode != http.StatusOK {
					contentType = []string{"it-is-not-ok"}
					contentTypeOptions = []string{"not-ok-option-1", "not-ok-option-2"}
				}
				require.Equal(t, contentType, resp.Header.Values("Content-Type"))
				require.Equal(t, contentTypeOptions, resp.Header.Values("X-Content-Type-Options"))
			}

			// Since the response is sent as soon as the quorum is reached, when we
			// reach this point the 3rd AM may not have received the request yet.
			// To avoid flaky test we retry until we hit the desired state within a reasonable timeout.
			test.Poll(t, time.Second, c.expectedTotalCalls, func() interface{} {
				totalReqCount := 0
				for _, a := range ams {
					reqCount := a.requestsCount(route)
					// AM should not get duplicate requests.
					require.True(t, reqCount <= 1, "duplicate requests %d", reqCount)
					totalReqCount += reqCount
				}

				return totalReqCount
			})
		})
	}

}

func TestDistributor_IsPathSupported(t *testing.T) {
	supported := map[string]bool{
		"/alertmanager/api/v1/alerts":           true,
		"/alertmanager/api/v1/alerts/groups":    false,
		"/alertmanager/api/v1/silences":         true,
		"/alertmanager/api/v1/silence/id":       true,
		"/alertmanager/api/v1/silence/anything": true,
		"/alertmanager/api/v1/silence/really":   true,
		"/alertmanager/api/v1/status":           true,
		"/alertmanager/api/v1/receivers":        true,
		"/alertmanager/api/v1/other":            false,
		"/alertmanager/api/v2/alerts":           true,
		"/alertmanager/api/v2/alerts/groups":    true,
		"/alertmanager/api/v2/silences":         true,
		"/alertmanager/api/v2/silence/id":       true,
		"/alertmanager/api/v2/silence/anything": true,
		"/alertmanager/api/v2/silence/really":   true,
		"/alertmanager/api/v2/status":           true,
		"/alertmanager/api/v2/receivers":        true,
		"/alertmanager/api/v2/other":            false,
		"/alertmanager/other":                   false,
		"/other":                                false,
	}

	for path, isSupported := range supported {
		t.Run(path, func(t *testing.T) {
			d, _, cleanup := prepare(t, 1, 1, 1, []byte{})
			t.Cleanup(cleanup)
			require.Equal(t, isSupported, d.IsPathSupported(path))
		})
	}
}

func prepare(t *testing.T, numAM, numHappyAM, replicationFactor int, responseBody []byte) (*Distributor, []*mockAlertmanager, func()) {
	ams := []*mockAlertmanager{}
	for i := 0; i < numHappyAM; i++ {
		ams = append(ams, newMockAlertmanager(i, true, responseBody))
	}
	for i := numHappyAM; i < numAM; i++ {
		ams = append(ams, newMockAlertmanager(i, false, responseBody))
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	amDescs := map[string]ring.InstanceDesc{}
	amByAddr := map[string]*mockAlertmanager{}
	for i, a := range ams {
		amDescs[a.myAddr] = ring.InstanceDesc{
			Addr:                a.myAddr,
			Zone:                "",
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              []uint32{uint32((math.MaxUint32 / numAM) * i)},
		}
		amByAddr[a.myAddr] = ams[i]
	}

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

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
	}, RingNameForServer, RingKey, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), amRing))
	test.Poll(t, time.Second, numAM, func() interface{} {
		return amRing.InstancesCount()
	})

	cfg := &MultitenantAlertmanagerConfig{}
	flagext.DefaultValues(cfg)

	d, err := NewDistributor(cfg.AlertmanagerClient, cfg.MaxRecvMsgSize, amRing, newMockAlertmanagerClientFactory(amByAddr), util_log.Logger, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), d))

	return d, ams, func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), d))
	}
}

type mockAlertmanager struct {
	alertmanagerpb.AlertmanagerClient
	grpc_health_v1.HealthClient
	// receivedRequests is map of route -> statusCode -> number of requests.
	receivedRequests map[string]map[int]int
	mtx              sync.Mutex
	myAddr           string
	happy            bool
	responseBody     []byte
}

func newMockAlertmanager(idx int, happy bool, responseBody []byte) *mockAlertmanager {
	return &mockAlertmanager{
		receivedRequests: make(map[string]map[int]int),
		myAddr:           fmt.Sprintf("127.0.0.1:%05d", 10000+idx),
		happy:            happy,
		responseBody:     responseBody,
	}
}

func (am *mockAlertmanager) HandleRequest(_ context.Context, in *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	am.mtx.Lock()
	defer am.mtx.Unlock()

	u, err := url.Parse(in.Url)
	if err != nil {
		return nil, err
	}
	path := u.Path
	m, ok := am.receivedRequests[path]
	if !ok {
		m = make(map[int]int)
		am.receivedRequests[path] = m
	}

	if am.happy {
		m[http.StatusOK]++
		return &httpgrpc.HTTPResponse{
			Code: http.StatusOK,
			Headers: []*httpgrpc.Header{
				{
					Key:    "Content-Type",
					Values: []string{"it-is-ok"},
				}, {
					Key:    "X-Content-Type-Options",
					Values: []string{"ok-option-1", "ok-option-2"},
				},
			},
			Body: am.responseBody,
		}, nil
	}

	m[http.StatusInternalServerError]++
	return nil, httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: http.StatusInternalServerError,
		Headers: []*httpgrpc.Header{
			{
				Key:    "Content-Type",
				Values: []string{"it-is-not-ok"},
			}, {
				Key:    "X-Content-Type-Options",
				Values: []string{"not-ok-option-1", "not-ok-option-2"},
			},
		},
	})
}

func (am *mockAlertmanager) requestsCount(route string) int {
	am.mtx.Lock()
	defer am.mtx.Unlock()

	routeMap, ok := am.receivedRequests[route]
	if !ok {
		return 0
	}

	// The status could be something other than overall
	// expected status because of quorum logic.
	reqCount := 0
	for _, count := range routeMap {
		reqCount += count
	}
	return reqCount
}

func (am *mockAlertmanager) Close() error {
	return nil
}

func (am *mockAlertmanager) RemoteAddress() string {
	return am.myAddr
}

type mockAlertmanagerClientFactory struct {
	alertmanagerByAddr map[string]*mockAlertmanager
}

func newMockAlertmanagerClientFactory(alertmanagerByAddr map[string]*mockAlertmanager) ClientsPool {
	return &mockAlertmanagerClientFactory{alertmanagerByAddr: alertmanagerByAddr}
}

func (f *mockAlertmanagerClientFactory) GetClientFor(addr string) (Client, error) {
	c, ok := f.alertmanagerByAddr[addr]
	if !ok {
		return nil, errors.New("client not found")
	}
	return Client(c), nil
}
