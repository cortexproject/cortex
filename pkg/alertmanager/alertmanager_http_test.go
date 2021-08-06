package alertmanager

import (
	"context"
	"io/ioutil"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/cluster"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMultitenantAlertmanager_GetStatusHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var peer *cluster.Peer
	{
		logger := log.NewNopLogger()
		createPeer := func(peers []string) (*cluster.Peer, error) {
			return cluster.Create(
				logger,
				prometheus.NewRegistry(),
				"127.0.0.1:0",
				"",
				peers,
				true,
				cluster.DefaultPushPullInterval,
				cluster.DefaultGossipInterval,
				cluster.DefaultTcpTimeout,
				cluster.DefaultProbeTimeout,
				cluster.DefaultProbeInterval,
			)
		}

		peer1, err := createPeer(nil)
		require.NoError(t, err)
		require.NotNil(t, peer1)
		err = peer1.Join(cluster.DefaultReconnectInterval, cluster.DefaultReconnectTimeout)
		require.NoError(t, err)
		go peer1.Settle(ctx, 0*time.Second)
		require.NoError(t, peer1.WaitReady(ctx))
		require.Equal(t, peer1.Status(), "ready")

		peer2, err := createPeer([]string{peer1.Self().Address()})
		require.NoError(t, err)
		require.NotNil(t, peer2)
		err = peer2.Join(cluster.DefaultReconnectInterval, cluster.DefaultReconnectTimeout)
		require.NoError(t, err)
		go peer2.Settle(ctx, 0*time.Second)
		peer = peer2
	}

	for _, tt := range []struct {
		am        *MultitenantAlertmanager
		content   string
		nocontent string
	}{
		{
			am:        &MultitenantAlertmanager{peer: nil},
			content:   "Alertmanager gossip-based clustering is disabled.",
			nocontent: "Node",
		},
		{
			am:        &MultitenantAlertmanager{peer: peer},
			content:   "Members",
			nocontent: "No peers",
		},
	} {
		req := httptest.NewRequest("GET", "http://alertmanager.cortex/status", nil)
		w := httptest.NewRecorder()
		tt.am.GetStatusHandler().ServeHTTP(w, req)

		resp := w.Result()
		require.Equal(t, 200, w.Code)
		body, _ := ioutil.ReadAll(resp.Body)
		content := string(body)
		require.Contains(t, content, tt.content)
		require.NotContains(t, content, tt.nocontent)
	}
}
