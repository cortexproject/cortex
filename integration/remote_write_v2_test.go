//go:build integration_remote_write_v2
// +build integration_remote_write_v2

package integration

import (
	"math/rand"
	"net/http"
	"path"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestIngest(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                     blocksStorageEngine,
			"-blocks-storage.backend":                           "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":     "4m",
			"-blocks-storage.bucket-store.sync-interval":        "15m",
			"-blocks-storage.bucket-store.index-cache.backend":  tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.bucket-store.bucket-index.enabled": "true",
			"-querier.query-store-for-labels-enabled":           "true",
			"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.retention-period":             ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":     "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path := path.Join(s.SharedDir(), "cortex-1")

	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})
	// Start Cortex replicas.
	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	// series push
	symbols1, series, expectedVector := e2e.GenerateSeriesV2("test_series", now, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "foo", Value: "bar"})
	res, err := c.PushV2(symbols1, series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	testPushHeader(t, res.Header, "1", "0", "0")

	// sample
	result, err := c.Query("test_series", now)
	require.NoError(t, err)
	assert.Equal(t, expectedVector, result.(model.Vector))

	// metadata
	metadata, err := c.Metadata("test_series", "")
	require.NoError(t, err)
	require.Equal(t, 1, len(metadata["test_series"]))

	// histogram
	histogramIdx := rand.Uint32()
	symbols2, histogramSeries := e2e.GenerateHistogramSeriesV2("test_histogram", now, histogramIdx, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
	res, err = c.PushV2(symbols2, histogramSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	testPushHeader(t, res.Header, "1", "1", "0")

	symbols3, histogramFloatSeries := e2e.GenerateHistogramSeriesV2("test_histogram", now, histogramIdx, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
	res, err = c.PushV2(symbols3, histogramFloatSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	testPushHeader(t, res.Header, "1", "1", "0")

	testHistogramTimestamp := now.Add(blockRangePeriod * 2)
	expectedHistogram := tsdbutil.GenerateTestHistogram(int(histogramIdx))
	result, err = c.Query(`test_histogram`, testHistogramTimestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	v := result.(model.Vector)
	require.Equal(t, 2, v.Len())
	for _, s := range v {
		require.NotNil(t, s.Histogram)
		require.Equal(t, float64(expectedHistogram.Count), float64(s.Histogram.Count))
		require.Equal(t, float64(expectedHistogram.Sum), float64(s.Histogram.Sum))
	}
}

func TestExemplar(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                     blocksStorageEngine,
			"-blocks-storage.backend":                           "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":     "4m",
			"-blocks-storage.bucket-store.sync-interval":        "15m",
			"-blocks-storage.bucket-store.index-cache.backend":  tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.bucket-store.bucket-index.enabled": "true",
			"-querier.query-store-for-labels-enabled":           "true",
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.enable-native-histograms":     "true",
			// Ingester.
			"-ring.store":             "consul",
			"-consul.hostname":        consul.NetworkHTTPEndpoint(),
			"-ingester.max-exemplars": "100",
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path := path.Join(s.SharedDir(), "cortex-1")

	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})
	// Start Cortex replicas.
	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	tsMillis := e2e.TimeToMilliseconds(now)

	symbols := []string{"", "__name__", "test_metric", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i", "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes"}
	timeseries := []writev2.TimeSeries{
		{
			LabelsRefs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // Symbolized writeRequestFixture.Timeseries[0].Labels
			Metadata: writev2.Metadata{
				Type: writev2.Metadata_METRIC_TYPE_COUNTER, // writeV2RequestSeries1Metadata.Type.

				HelpRef: 15, // Symbolized writeV2RequestSeries1Metadata.Help.
				UnitRef: 16, // Symbolized writeV2RequestSeries1Metadata.Unit.
			},
			Samples:   []writev2.Sample{{Value: 1, Timestamp: tsMillis}},
			Exemplars: []writev2.Exemplar{{LabelsRefs: []uint32{11, 12}, Value: 1, Timestamp: tsMillis}},
		},
	}

	res, err := c.PushV2(symbols, timeseries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	testPushHeader(t, res.Header, "1", "0", "1")

	start := time.Now().Add(-time.Minute)
	end := now.Add(time.Minute)

	exemplars, err := c.QueryExemplars("test_metric", start, end)
	require.NoError(t, err)
	require.Equal(t, 1, len(exemplars))
}

func testPushHeader(t *testing.T, header http.Header, expectedSamples, expectedHistogram, expectedExemplars string) {
	require.Equal(t, expectedSamples, header.Get("X-Prometheus-Remote-Write-Samples-Written"))
	require.Equal(t, expectedHistogram, header.Get("X-Prometheus-Remote-Write-Histograms-Written"))
	require.Equal(t, expectedExemplars, header.Get("X-Prometheus-Remote-Write-Exemplars-Written"))
}
