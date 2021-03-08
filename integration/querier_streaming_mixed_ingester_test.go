// +build requires_docker

package integration

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	client2 "github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestQuerierWithStreamingBlocksAndChunksIngesters(t *testing.T) {
	for _, streamChunks := range []bool{false, true} {
		t.Run(fmt.Sprintf("%v", streamChunks), func(t *testing.T) {
			testQuerierWithStreamingBlocksAndChunksIngesters(t, streamChunks)
		})
	}
}

func testQuerierWithStreamingBlocksAndChunksIngesters(t *testing.T, streamChunks bool) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
	chunksFlags := ChunksStorageFlags()
	blockFlags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":      "1h",
		"-blocks-storage.tsdb.head-compaction-interval": "1m",
		"-store-gateway.sharding-enabled":               "false",
		"-querier.ingester-streaming":                   "true",
	})
	blockFlags["-ingester.stream-chunks-when-using-blocks"] = fmt.Sprintf("%v", streamChunks)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blockFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components.
	ingesterBlocks := e2ecortex.NewIngester("ingester-blocks", consul.NetworkHTTPEndpoint(), blockFlags, "")
	ingesterChunks := e2ecortex.NewIngester("ingester-chunks", consul.NetworkHTTPEndpoint(), chunksFlags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), blockFlags, "")
	require.NoError(t, s.StartAndWaitReady(ingesterBlocks, ingesterChunks, storeGateway))

	// Sharding is disabled, pass gateway address.
	querierFlags := mergeFlags(blockFlags, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
		"-distributor.shard-by-all-labels": "true",
	})
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), querierFlags, "")
	require.NoError(t, s.StartAndWaitReady(querier))

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(1024), "cortex_ring_tokens_total"))

	s1 := []client2.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2, TimestampMs: 2000},
		{Value: 3, TimestampMs: 3000},
		{Value: 4, TimestampMs: 4000},
		{Value: 5, TimestampMs: 5000},
	}

	s2 := []client2.Sample{
		{Value: 1, TimestampMs: 1000},
		{Value: 2.5, TimestampMs: 2500},
		{Value: 3, TimestampMs: 3000},
		{Value: 5.5, TimestampMs: 5500},
	}

	clientConfig := client2.Config{}
	clientConfig.RegisterFlags(flag.NewFlagSet("unused", flag.ContinueOnError)) // registers default values

	// Push data to chunks ingester.
	{
		ingesterChunksClient, err := client2.MakeIngesterClient(ingesterChunks.GRPCEndpoint(), clientConfig)
		require.NoError(t, err)
		defer ingesterChunksClient.Close()

		_, err = ingesterChunksClient.Push(user.InjectOrgID(context.Background(), "user"), &client2.WriteRequest{
			Timeseries: []client2.PreallocTimeseries{
				{TimeSeries: &client2.TimeSeries{Labels: []client2.LabelAdapter{{Name: labels.MetricName, Value: "s"}, {Name: "l", Value: "1"}}, Samples: s1}},
				{TimeSeries: &client2.TimeSeries{Labels: []client2.LabelAdapter{{Name: labels.MetricName, Value: "s"}, {Name: "l", Value: "2"}}, Samples: s1}}},
			Source: client2.API,
		})
		require.NoError(t, err)
	}

	// Push data to blocks ingester.
	{
		ingesterBlocksClient, err := client2.MakeIngesterClient(ingesterBlocks.GRPCEndpoint(), clientConfig)
		require.NoError(t, err)
		defer ingesterBlocksClient.Close()

		_, err = ingesterBlocksClient.Push(user.InjectOrgID(context.Background(), "user"), &client2.WriteRequest{
			Timeseries: []client2.PreallocTimeseries{
				{TimeSeries: &client2.TimeSeries{Labels: []client2.LabelAdapter{{Name: labels.MetricName, Value: "s"}, {Name: "l", Value: "2"}}, Samples: s2}},
				{TimeSeries: &client2.TimeSeries{Labels: []client2.LabelAdapter{{Name: labels.MetricName, Value: "s"}, {Name: "l", Value: "3"}}, Samples: s1}}},
			Source: client2.API,
		})
		require.NoError(t, err)
	}

	c, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", "user")
	require.NoError(t, err)

	// Query back the series (1 only in the storage, 1 only in the ingesters, 1 on both).
	result, err := c.Query("s[1m]", time.Unix(10, 0))
	require.NoError(t, err)

	s1Values := []model.SamplePair{
		{Value: 1, Timestamp: 1000},
		{Value: 2, Timestamp: 2000},
		{Value: 3, Timestamp: 3000},
		{Value: 4, Timestamp: 4000},
		{Value: 5, Timestamp: 5000},
	}

	s1AndS2ValuesMerged := []model.SamplePair{
		{Value: 1, Timestamp: 1000},
		{Value: 2, Timestamp: 2000},
		{Value: 2.5, Timestamp: 2500},
		{Value: 3, Timestamp: 3000},
		{Value: 4, Timestamp: 4000},
		{Value: 5, Timestamp: 5000},
		{Value: 5.5, Timestamp: 5500},
	}

	expectedMatrix := model.Matrix{
		// From chunks ingester only.
		&model.SampleStream{
			Metric: model.Metric{labels.MetricName: "s", "l": "1"},
			Values: s1Values,
		},

		// From blocks ingester only.
		&model.SampleStream{
			Metric: model.Metric{labels.MetricName: "s", "l": "3"},
			Values: s1Values,
		},

		// Merged from both ingesters.
		&model.SampleStream{
			Metric: model.Metric{labels.MetricName: "s", "l": "2"},
			Values: s1AndS2ValuesMerged,
		},
	}

	require.Equal(t, model.ValMatrix, result.Type())
	require.ElementsMatch(t, expectedMatrix, result.(model.Matrix))
}
