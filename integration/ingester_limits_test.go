//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIngesterGlobalLimits(t *testing.T) {
	tests := map[string]struct {
		shardingStrategy         string
		tenantShardSize          int
		maxGlobalSeriesPerTenant int
		maxGlobalSeriesPerMetric int
	}{
		"default sharding strategy": {
			shardingStrategy:         "default",
			tenantShardSize:          1, // Ignored by default strategy.
			maxGlobalSeriesPerTenant: 1000,
			maxGlobalSeriesPerMetric: 300,
		},
		"shuffle sharding strategy": {
			shardingStrategy:         "shuffle-sharding",
			tenantShardSize:          1,
			maxGlobalSeriesPerTenant: 1000,
			maxGlobalSeriesPerMetric: 300,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := BlocksStorageFlags()
			flags["-distributor.replication-factor"] = "1"
			flags["-distributor.shard-by-all-labels"] = "true"
			flags["-distributor.sharding-strategy"] = testData.shardingStrategy
			flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(testData.tenantShardSize)
			flags["-ingester.max-series-per-user"] = "0"
			flags["-ingester.max-series-per-metric"] = "0"
			flags["-ingester.max-global-series-per-user"] = strconv.Itoa(testData.maxGlobalSeriesPerTenant)
			flags["-ingester.max-global-series-per-metric"] = strconv.Itoa(testData.maxGlobalSeriesPerMetric)
			flags["-ingester.heartbeat-period"] = "1s"

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Cortex components.
			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

			// Wait until distributor has updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Wait until ingesters have heartbeated the ring after all ingesters were active,
			// in order to update the number of instances. Since we have no metric, we have to
			// rely on a ugly sleep.
			time.Sleep(2 * time.Second)

			now := time.Now()
			client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
			require.NoError(t, err)

			numSeriesWithSameMetricName := 0
			numSeriesTotal := 0
			maxErrorsBeforeStop := 100

			// Try to push as many series with the same metric name as we can.
			for i, errs := 0, 0; i < 10000; i++ {
				series, _ := generateSeries("test_limit_per_metric", now, prompb.Label{
					Name:  "cardinality",
					Value: strconv.Itoa(rand.Int()),
				})

				res, err := client.Push(series)
				require.NoError(t, err)

				if res.StatusCode == 200 {
					numSeriesTotal++
					numSeriesWithSameMetricName++
				} else if errs++; errs >= maxErrorsBeforeStop {
					break
				}
			}

			// Try to push as many series with the different metric name as we can.
			for i, errs := 0, 0; i < 10000; i++ {
				series, _ := generateSeries(fmt.Sprintf("test_limit_per_tenant_%d", rand.Int()), now)
				res, err := client.Push(series)
				require.NoError(t, err)

				if res.StatusCode == 200 {
					numSeriesTotal++
				} else if errs++; errs >= maxErrorsBeforeStop {
					break
				}
			}

			// We expect the number of series we've been successfully pushed to be around
			// the limit. Due to how the global limit implementation works (lack of centralised
			// coordination) the actual number of written series could be slightly different
			// than the global limit, so we allow a 10% difference.
			delta := 0.1
			assert.InDelta(t, testData.maxGlobalSeriesPerMetric, numSeriesWithSameMetricName, float64(testData.maxGlobalSeriesPerMetric)*delta)
			assert.InDelta(t, testData.maxGlobalSeriesPerTenant, numSeriesTotal, float64(testData.maxGlobalSeriesPerTenant)*delta)

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester1)
			assertServiceMetricsPrefixes(t, Ingester, ingester2)
			assertServiceMetricsPrefixes(t, Ingester, ingester3)
		})
	}
}
