//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestNativeHistogramIngestionAndQuery(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	configs := []map[string]string{
		{
			"-api.querier-default-codec": "json",
		},
		{
			"-api.querier-default-codec": "protobuf",
		},
	}

	for _, config := range configs {
		t.Run(fmt.Sprintf("native histograms with %s codec", config["-api.querier-default-codec"]), func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(BlocksStorageFlags(), map[string]string{
				"-blocks-storage.tsdb.block-ranges-period":      blockRangePeriod.String(),
				"-blocks-storage.tsdb.ship-interval":            "1s",
				"-blocks-storage.tsdb.retention-period":         ((blockRangePeriod * 2) - 1).String(),
				"-blocks-storage.tsdb.enable-native-histograms": "true",
			})

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Cortex components for the write path.
			distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
			require.NoError(t, s.StartAndWaitReady(distributor, ingester))

			// Wait until the distributor has updated the ring.
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

			// Push some series to Cortex.
			c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
			require.NoError(t, err)

			seriesTimestamp := time.Now()
			series2Timestamp := seriesTimestamp.Add(blockRangePeriod * 2)
			histogramIdx1 := rand.Uint32()
			series1 := e2e.GenerateHistogramSeries("series_1", seriesTimestamp, histogramIdx1, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
			series1Float := e2e.GenerateHistogramSeries("series_1", seriesTimestamp, histogramIdx1, true, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
			res, err := c.Push(append(series1, series1Float...))
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			histogramIdx2 := rand.Uint32()
			series2 := e2e.GenerateHistogramSeries("series_2", series2Timestamp, histogramIdx2, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
			series2Float := e2e.GenerateHistogramSeries("series_2", series2Timestamp, histogramIdx2, true, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
			res, err = c.Push(append(series2, series2Float...))
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Wait until the TSDB head is compacted and shipped to the storage.
			// The shipped block contains the 2 series from `series_1` and `series_2` will be in head.
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(4), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))
			require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series"))

			queryFrontend := e2ecortex.NewQueryFrontendWithConfigFile("query-frontend", "", mergeFlags(flags, config), "")
			require.NoError(t, s.Start(queryFrontend))

			// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check.
			storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
				"-blocks-storage.bucket-store.sync-interval": "5s",
			}), "")
			querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
				"-blocks-storage.bucket-store.sync-interval": "1s",
				"-querier.frontend-address":                  queryFrontend.NetworkGRPCEndpoint(),
			}), "")
			require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

			// Wait until the querier and store-gateway have updated the ring, and wait until the blocks are old enough for consistency check
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
			require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(4), []string{"cortex_querier_blocks_scan_duration_seconds"}, e2e.WithMetricCount))

			// Sleep 3 * bucket sync interval to make sure consistency checker
			// doesn't consider block is uploaded recently.
			time.Sleep(3 * time.Second)

			// Query back the series.
			c, err = e2ecortex.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)

			expectedHistogram1 := tsdbutil.GenerateTestHistogram(int64(histogramIdx1))
			expectedHistogram2 := tsdbutil.GenerateTestHistogram(int64(histogramIdx2))
			result, err := c.QueryRange(`series_1`, series2Timestamp.Add(-time.Minute*10), series2Timestamp, time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())
			m := result.(model.Matrix)
			require.Equal(t, 2, m.Len())
			for _, ss := range m {
				require.Empty(t, ss.Values)
				require.NotEmpty(t, ss.Histograms)
				for _, h := range ss.Histograms {
					require.NotEmpty(t, h)
					require.Equal(t, float64(expectedHistogram1.Count), float64(h.Histogram.Count))
					require.Equal(t, float64(expectedHistogram1.Sum), float64(h.Histogram.Sum))
				}
			}

			result, err = c.QueryRange(`series_2`, series2Timestamp.Add(-time.Minute*10), series2Timestamp, time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())
			m = result.(model.Matrix)
			require.Equal(t, 2, m.Len())
			for _, ss := range m {
				require.Empty(t, ss.Values)
				require.NotEmpty(t, ss.Histograms)
				for _, h := range ss.Histograms {
					require.NotEmpty(t, h)
					require.Equal(t, float64(expectedHistogram2.Count), float64(h.Histogram.Count))
					require.Equal(t, float64(expectedHistogram2.Sum), float64(h.Histogram.Sum))
				}
			}

			result, err = c.Query(`series_1`, series2Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			v := result.(model.Vector)
			require.Equal(t, 2, v.Len())
			for _, s := range v {
				require.NotNil(t, s.Histogram)
				require.Equal(t, float64(expectedHistogram1.Count), float64(s.Histogram.Count))
				require.Equal(t, float64(expectedHistogram1.Sum), float64(s.Histogram.Sum))
			}

			result, err = c.Query(`series_2`, series2Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			v = result.(model.Vector)
			require.Equal(t, 2, v.Len())
			for _, s := range v {
				require.NotNil(t, s.Histogram)
				require.Equal(t, float64(expectedHistogram2.Count), float64(s.Histogram.Count))
				require.Equal(t, float64(expectedHistogram2.Sum), float64(s.Histogram.Sum))
			}
		})
	}
}
