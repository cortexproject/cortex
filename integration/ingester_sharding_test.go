//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIngesterSharding(t *testing.T) {
	const numSeriesToPush = 1000

	tests := map[string]struct {
		shardingStrategy            string
		tenantShardSize             int
		expectedIngestersWithSeries int
	}{
		//Default Sharding Strategy
		"default sharding strategy should be ignored and spread across all ingesters": {
			shardingStrategy:            "default",
			tenantShardSize:             2, // Ignored by default strategy.
			expectedIngestersWithSeries: 3,
		},
		"default sharding strategy should spread series across all ingesters": {
			shardingStrategy:            "default",
			tenantShardSize:             0, // Ignored by default strategy.
			expectedIngestersWithSeries: 3,
		},
		//Shuffle Sharding Strategy
		"shuffle-sharding strategy should spread series across the configured shard size number of ingesters": {
			shardingStrategy:            "shuffle-sharding",
			tenantShardSize:             2,
			expectedIngestersWithSeries: 2,
		},
		"Tenant Shard Size of 0 should leverage all ingesters": {
			shardingStrategy:            "shuffle-sharding",
			tenantShardSize:             0,
			expectedIngestersWithSeries: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := BlocksStorageFlags()
			flags["-distributor.shard-by-all-labels"] = "true"
			flags["-distributor.sharding-strategy"] = testData.shardingStrategy
			flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(testData.tenantShardSize)

			if testData.shardingStrategy == "shuffle-sharding" {
				// Enable shuffle sharding on read path but not lookback, otherwise all ingesters would be
				// queried being just registered.
				flags["-querier.shuffle-sharding-ingesters-lookback-period"] = "1ns"
			}

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingesters := e2ecortex.NewCompositeCortexService(ingester1, ingester2, ingester3)
			querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))

			// Wait until distributor and queriers have updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Push series.
			now := time.Now()
			expectedVectors := map[string]model.Vector{}

			client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			for i := 1; i <= numSeriesToPush; i++ {
				metricName := fmt.Sprintf("series_%d", i)
				series, expectedVector := generateSeries(metricName, now)
				res, err := client.Push(series)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				expectedVectors[metricName] = expectedVector
			}

			// Extract metrics from ingesters.
			numIngestersWithSeries := 0
			totalIngestedSeries := 0

			for _, ing := range []*e2ecortex.CortexService{ingester1, ingester2, ingester3} {
				values, err := ing.SumMetrics([]string{"cortex_ingester_memory_series"})
				require.NoError(t, err)

				numMemorySeries := e2e.SumValues(values)
				totalIngestedSeries += int(numMemorySeries)
				if numMemorySeries > 0 {
					numIngestersWithSeries++
				}
			}

			require.Equal(t, testData.expectedIngestersWithSeries, numIngestersWithSeries)
			require.Equal(t, numSeriesToPush, totalIngestedSeries)

			// Query back series.
			for metricName, expectedVector := range expectedVectors {
				result, err := client.Query(metricName, now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector, result.(model.Vector))
			}

			// We expect that only ingesters belonging to tenant's shard have been queried if
			// shuffle sharding is enabled.
			expectedIngesters := ingesters.NumInstances()
			if testData.shardingStrategy == "shuffle-sharding" && testData.tenantShardSize > 0 {
				expectedIngesters = testData.tenantShardSize
			}

			expectedCalls := expectedIngesters * len(expectedVectors)
			require.NoError(t, ingesters.WaitSumMetricsWithOptions(
				e2e.Equals(float64(expectedCalls)),
				[]string{"cortex_request_duration_seconds"},
				e2e.WithMetricCount,
				e2e.SkipMissingMetrics, // Some ingesters may have received no request at all.
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/QueryStream"))))

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester1)
			assertServiceMetricsPrefixes(t, Ingester, ingester2)
			assertServiceMetricsPrefixes(t, Ingester, ingester3)
			assertServiceMetricsPrefixes(t, Querier, querier)
		})
	}
}
