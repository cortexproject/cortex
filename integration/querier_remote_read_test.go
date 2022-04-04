// +build requires_docker

package integration

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestQuerierRemoteRead(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{})

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components for the write path.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Cortex.
	now := time.Now()

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series, expectedVectors := generateSeries("series_1", now)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(storeGateway))
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), BlocksStorageFlags(), "")
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))

	matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "series_1")
	require.NoError(t, err)

	startMs := now.Add(-1*time.Minute).Unix() * 1000
	endMs := now.Add(time.Minute).Unix() * 1000

	q, err := remote.ToQuery(startMs, endMs, []*labels.Matcher{matcher}, &storage.SelectHints{
		Step:  1,
		Start: startMs,
		End:   endMs,
	})
	require.NoError(t, err)

	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{q},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
	}

	data, err := proto.Marshal(req)
	require.NoError(t, err)
	compressed := snappy.Encode(nil, data)

	// Call the remote read API endpoint with a timeout.
	httpReqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(httpReqCtx, "POST", "http://"+querier.HTTPEndpoint()+"/prometheus/api/v1/read", bytes.NewReader(compressed))
	require.NoError(t, err)
	httpReq.Header.Set("X-Scope-OrgID", "user-1")
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "Prometheus/1.8.2")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	httpResp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	compressed, err = ioutil.ReadAll(httpResp.Body)
	require.NoError(t, err)

	uncompressed, err := snappy.Decode(nil, compressed)
	require.NoError(t, err)

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	require.NoError(t, err)

	// Validate the returned remote read data matches what was written
	require.Len(t, resp.Results, 1)
	require.Len(t, resp.Results[0].Timeseries, 1)
	require.Len(t, resp.Results[0].Timeseries[0].Labels, 1)
	require.Equal(t, "series_1", resp.Results[0].Timeseries[0].Labels[0].GetValue())
	require.Len(t, resp.Results[0].Timeseries[0].Samples, 1)
	require.Equal(t, int64(expectedVectors[0].Timestamp), resp.Results[0].Timeseries[0].Samples[0].Timestamp)
	require.Equal(t, float64(expectedVectors[0].Value), resp.Results[0].Timeseries[0].Samples[0].Value)
}
