// +build requires_docker

package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIngesterFlushWithChunksStorage(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(dynamo, consul))

	// Start Cortex components.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	tableManager := e2ecortex.NewTableManager("table-manager", ChunksStorageFlags, "")
	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), mergeFlags(ChunksStorageFlags, map[string]string{
		"-ingester.max-transfer-retries": "0",
	}), "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), ChunksStorageFlags, "")
	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), ChunksStorageFlags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, querier, ingester, tableManager))

	// Wait until the first table-manager sync has completed, so that we're
	// sure the tables have been created.
	require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex.
	now := time.Now()
	series1, expectedVector1 := generateSeries("series_1", now)
	series2, expectedVector2 := generateSeries("series_2", now)

	for _, series := range [][]prompb.TimeSeries{series1, series2} {
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Ensure ingester metrics are tracked correctly.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_chunks_created_total"))

	// Query the series.
	result, err := c.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector1, result.(model.Vector))

	result, err = c.Query("series_2", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector2, result.(model.Vector))

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Ingester, ingester)

	// Stop ingester-1, so that it will flush all chunks to the storage. This function will return
	// once the ingester-1 is successfully stopped, which means the flushing is completed.
	require.NoError(t, s.Stop(ingester))

	// Ensure chunks have been uploaded to the storage (DynamoDB).
	dynamoURL := "dynamodb://u:p@" + dynamo.Endpoint(8000)
	dynamoClient, err := newDynamoClient(dynamoURL)
	require.NoError(t, err)

	// We have pushed 2 series, so we do expect 2 chunks.
	period := now.Unix() / (168 * 3600)
	indexTable := fmt.Sprintf("cortex_%d", period)
	chunksTable := fmt.Sprintf("cortex_chunks_%d", period)

	out, err := dynamoClient.Scan(&dynamodb.ScanInput{TableName: aws.String(indexTable)})
	require.NoError(t, err)
	assert.Equal(t, int64(2*2), *out.Count)

	out, err = dynamoClient.Scan(&dynamodb.ScanInput{TableName: aws.String(chunksTable)})
	require.NoError(t, err)
	assert.Equal(t, int64(2), *out.Count)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, TableManager, tableManager)
}
