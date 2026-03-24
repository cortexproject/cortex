//go:build integration_remote_write_v2

package integration

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	remoteapi "github.com/prometheus/client_golang/exp/api/remote"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestIngesterRollingUpdate(t *testing.T) {
	// Test ingester rolling update situation: when -distributor.remote-writev2-enabled is true, and ingester uses the v1.19.0 image.
	// Expected: remote write 2.0 push success
	const blockRangePeriod = 5 * time.Second
	ingesterImage := "quay.io/cortexproject/cortex:v1.19.0"

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
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

	distributorFlag := mergeFlags(flags, map[string]string{
		"-distributor.remote-writev2-enabled": "true",
	})

	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path := path.Join(s.SharedDir(), "cortex-1")

	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})
	// Start Cortex replicas.
	// Start all other services.
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, ingesterImage)
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), distributorFlag, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-querier.store-gateway-addresses": storeGateway.NetworkGRPCEndpoint()}), "")

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor, storeGateway))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	// series push
	symbols1, series, expectedVector := e2e.GenerateSeriesV2("test_series", now, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "foo", Value: "bar"})
	stats, err := c.PushV2(symbols1, series)
	require.NoError(t, err)
	testPushHeader(t, stats, 1, 0, 0)

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
	symbols2, histogramSeries := e2e.GenerateHistogramSeriesV2("test_histogram", now, histogramIdx, false, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
	writeStats, err := c.PushV2(symbols2, histogramSeries)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	symbols3, histogramFloatSeries := e2e.GenerateHistogramSeriesV2("test_histogram", now, histogramIdx, false, true, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
	writeStats, err = c.PushV2(symbols3, histogramFloatSeries)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	testHistogramTimestamp := now.Add(blockRangePeriod * 2)
	expectedHistogram := tsdbutil.GenerateTestHistogram(int64(histogramIdx))
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

func TestIngest_SenderSendPRW2_DistributorNotAllowPRW2(t *testing.T) {
	// Test `-distributor.remote-writev2-enabled=false` but the Sender pushes PRW2
	// Expected: status code is 200, but samples are not written.
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
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "false",
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
	symbols1, series, _ := e2e.GenerateSeriesV2("test_series", now, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "foo", Value: "bar"})
	_, err = c.PushV2(symbols1, series)
	require.Error(t, err)
	require.Contains(t, err.Error(), "io.prometheus.write.v2.Request protobuf message is not accepted by this server; only accepts prometheus.WriteRequest")

	// sample
	result, err := c.Query("test_series", now)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestIngest_EnableTypeAndUnitLabels(t *testing.T) {
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
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor":          "1",
			"-distributor.remote-writev2-enabled":      "true",
			"-distributor.enable-type-and-unit-labels": "true",
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
	symbols1, series, _ := e2e.GenerateSeriesV2("test_series", now, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "foo", Value: "bar"})
	writeStats, err := c.PushV2(symbols1, series)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 1, 0, 0)

	value, err := c.Query("test_series", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, value.Type())
	vec := value.(model.Vector)
	require.True(t, vec[0].Metric["__unit__"] != "")
	require.True(t, vec[0].Metric["__type__"] != "")
}

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
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "true",
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
	writeStats, err := c.PushV2(symbols1, series)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 1, 0, 0)

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
	symbols2, intNH := e2e.GenerateHistogramSeriesV2("test_nh", now, histogramIdx, false, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
	writeStats, err = c.PushV2(symbols2, intNH)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	// float histogram
	symbols3, floatNH := e2e.GenerateHistogramSeriesV2("test_nh", now, histogramIdx, false, true, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
	writeStats, err = c.PushV2(symbols3, floatNH)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	// histogram with Custom Bucket
	symbols4, intNHCB := e2e.GenerateHistogramSeriesV2("test_nhcb", now, histogramIdx, true, false, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "false"})
	writeStats, err = c.PushV2(symbols4, intNHCB)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	// float histogram with Custom Bucket
	symbols5, floatNHCB := e2e.GenerateHistogramSeriesV2("test_nhcb", now, histogramIdx, true, true, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "float", Value: "true"})
	writeStats, err = c.PushV2(symbols5, floatNHCB)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 0, 1, 0)

	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(5), []string{"cortex_distributor_push_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "type", "prw2"))))

	testHistogramTimestamp := now.Add(blockRangePeriod * 2)
	expectedNH := tsdbutil.GenerateTestHistogram(int64(histogramIdx))
	result, err = c.Query(`test_nh`, testHistogramTimestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	v := result.(model.Vector)
	require.Equal(t, 2, v.Len())
	for _, s := range v {
		require.NotNil(t, s.Histogram)
		require.Equal(t, float64(expectedNH.Count), float64(s.Histogram.Count))
		require.Equal(t, float64(expectedNH.Sum), float64(s.Histogram.Sum))
	}

	expectedNHCB := tsdbutil.GenerateTestCustomBucketsHistogram(int64(histogramIdx))
	result, err = c.Query(`test_nhcb`, testHistogramTimestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	v = result.(model.Vector)
	require.Equal(t, 2, v.Len())
	for _, s := range v {
		require.NotNil(t, s.Histogram)
		require.Equal(t, float64(expectedNHCB.Count), float64(s.Histogram.Count))
		require.Equal(t, float64(expectedNHCB.Sum), float64(s.Histogram.Sum))
	}
}

func TestIngest_StartTimestamp(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "true",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path := path.Join(s.SharedDir(), "cortex-1")
	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	sampleTs := time.Now().Truncate(time.Second)
	startTs := sampleTs.Add(-2 * time.Second)
	step := sampleTs.Sub(startTs)

	sampleSymbols := []string{"", "__name__", "test_start_timestamp_sample"}
	sampleSeries := []writev2.TimeSeries{
		{
			LabelsRefs: []uint32{1, 2},
			Samples: []writev2.Sample{{
				Value:          42,
				Timestamp:      e2e.TimeToMilliseconds(sampleTs),
				StartTimestamp: e2e.TimeToMilliseconds(startTs),
			}},
		},
	}

	writeStats, err := c.PushV2(sampleSymbols, sampleSeries)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 1, 0, 0)

	sampleResult, err := c.QueryRange("test_start_timestamp_sample", startTs, sampleTs, step)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, sampleResult.Type())

	sampleMatrix := sampleResult.(model.Matrix)
	require.Len(t, sampleMatrix, 1)
	require.Len(t, sampleMatrix[0].Values, 2)
	require.Empty(t, sampleMatrix[0].Histograms)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(startTs)), sampleMatrix[0].Values[0].Timestamp)
	assert.Equal(t, model.SampleValue(0), sampleMatrix[0].Values[0].Value)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(sampleTs)), sampleMatrix[0].Values[1].Timestamp)
	assert.Equal(t, model.SampleValue(42), sampleMatrix[0].Values[1].Value)

	histogramCases := []struct {
		metricName string
		isFloat    bool
		isCustom   bool
		idx        uint32
	}{
		{metricName: "test_start_timestamp_histogram", isFloat: false, isCustom: false, idx: rand.Uint32()},
		{metricName: "test_start_timestamp_histogram_float", isFloat: true, isCustom: false, idx: rand.Uint32()},
		{metricName: "test_start_timestamp_histogram_custom", isFloat: false, isCustom: true, idx: rand.Uint32()},
		{metricName: "test_start_timestamp_histogram_float_custom", isFloat: true, isCustom: true, idx: rand.Uint32()},
	}

	for _, tc := range histogramCases {
		symbols, series := e2e.GenerateHistogramSeriesV2(tc.metricName, sampleTs, tc.idx, tc.isCustom, tc.isFloat)
		series[0].Histograms[0].StartTimestamp = e2e.TimeToMilliseconds(startTs)

		writeStats, err = c.PushV2(symbols, series)
		require.NoError(t, err)
		testPushHeader(t, writeStats, 0, 1, 0)

		result, err := c.QueryRange(tc.metricName, startTs, sampleTs, step)
		require.NoError(t, err)
		require.Equal(t, model.ValMatrix, result.Type())

		matrix := result.(model.Matrix)
		require.Len(t, matrix, 1)
		require.Empty(t, matrix[0].Values)
		require.Len(t, matrix[0].Histograms, 2)
		require.NotNil(t, matrix[0].Histograms[0].Histogram)
		require.NotNil(t, matrix[0].Histograms[1].Histogram)
		assert.Equal(t, model.Time(e2e.TimeToMilliseconds(startTs)), matrix[0].Histograms[0].Timestamp)
		assert.Equal(t, model.FloatString(0), matrix[0].Histograms[0].Histogram.Count)
		assert.Equal(t, model.FloatString(0), matrix[0].Histograms[0].Histogram.Sum)

		var expectedCount, expectedSum model.FloatString
		if tc.isFloat {
			var expected *histogram.FloatHistogram
			if tc.isCustom {
				expected = tsdbutil.GenerateTestCustomBucketsFloatHistogram(int64(tc.idx))
			} else {
				expected = tsdbutil.GenerateTestFloatHistogram(int64(tc.idx))
			}
			expectedCount = model.FloatString(expected.Count)
			expectedSum = model.FloatString(expected.Sum)
		} else {
			var expected *histogram.Histogram
			if tc.isCustom {
				expected = tsdbutil.GenerateTestCustomBucketsHistogram(int64(tc.idx))
			} else {
				expected = tsdbutil.GenerateTestHistogram(int64(tc.idx))
			}
			expectedCount = model.FloatString(expected.Count)
			expectedSum = model.FloatString(expected.Sum)
		}

		assert.Equal(t, model.Time(e2e.TimeToMilliseconds(sampleTs)), matrix[0].Histograms[1].Timestamp)
		assert.Equal(t, expectedCount, matrix[0].Histograms[1].Histogram.Count)
		assert.Equal(t, expectedSum, matrix[0].Histograms[1].Histogram.Sum)
	}
}

func TestIngest_CreatedTimestampFallback(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			"-blocks-storage.tsdb.enable-native-histograms":    "true",
			"-ring.store":                         "consul",
			"-consul.hostname":                    consul.NetworkHTTPEndpoint(),
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "true",
			"-store-gateway.sharding-enabled":     "false",
			"-alertmanager.web.external-url":      "http://localhost/alertmanager",
		},
	)

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))
	path := path.Join(s.SharedDir(), "cortex-1")
	flags = mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path})

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	sampleTs := time.Now().Truncate(time.Second)
	startTs := sampleTs.Add(-2 * time.Second)
	step := sampleTs.Sub(startTs)

	// Send a PRW2 request encoded with Cortex proto carrying only created_timestamp.
	sampleReq := &cortexpb.WriteRequestV2{
		Symbols: []string{"", "__name__", "test_created_timestamp_sample"},
		Timeseries: []cortexpb.PreallocTimeseriesV2{
			{
				TimeSeriesV2: &cortexpb.TimeSeriesV2{
					LabelsRefs:       []uint32{1, 2},
					CreatedTimestamp: e2e.TimeToMilliseconds(startTs),
					Samples:          []cortexpb.Sample{{Value: 7, TimestampMs: e2e.TimeToMilliseconds(sampleTs)}},
				},
			},
		},
	}
	pushCortexV2Request(t, cortex.HTTPEndpoint(), "user-1", sampleReq)

	sampleResult, err := c.QueryRange("test_created_timestamp_sample", startTs, sampleTs, step)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, sampleResult.Type())

	sampleMatrix := sampleResult.(model.Matrix)
	require.Len(t, sampleMatrix, 1)
	require.Len(t, sampleMatrix[0].Values, 2)
	require.Empty(t, sampleMatrix[0].Histograms)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(startTs)), sampleMatrix[0].Values[0].Timestamp)
	assert.Equal(t, model.SampleValue(0), sampleMatrix[0].Values[0].Value)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(sampleTs)), sampleMatrix[0].Values[1].Timestamp)
	assert.Equal(t, model.SampleValue(7), sampleMatrix[0].Values[1].Value)

	h := cortexpb.HistogramToHistogramProto(e2e.TimeToMilliseconds(sampleTs), tsdbutil.GenerateTestHistogram(3))
	histReq := &cortexpb.WriteRequestV2{
		Symbols: []string{"", "__name__", "test_created_timestamp_histogram"},
		Timeseries: []cortexpb.PreallocTimeseriesV2{
			{
				TimeSeriesV2: &cortexpb.TimeSeriesV2{
					LabelsRefs:       []uint32{1, 2},
					CreatedTimestamp: e2e.TimeToMilliseconds(startTs),
					Histograms:       []cortexpb.Histogram{h},
				},
			},
		},
	}
	pushCortexV2Request(t, cortex.HTTPEndpoint(), "user-1", histReq)

	histResult, err := c.QueryRange("test_created_timestamp_histogram", startTs, sampleTs, step)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, histResult.Type())

	histMatrix := histResult.(model.Matrix)
	require.Len(t, histMatrix, 1)
	require.Empty(t, histMatrix[0].Values)
	require.Len(t, histMatrix[0].Histograms, 2)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(startTs)), histMatrix[0].Histograms[0].Timestamp)
	assert.Equal(t, model.FloatString(0), histMatrix[0].Histograms[0].Histogram.Count)
	assert.Equal(t, model.FloatString(0), histMatrix[0].Histograms[0].Histogram.Sum)

	expectedHist := tsdbutil.GenerateTestHistogram(3)
	assert.Equal(t, model.Time(e2e.TimeToMilliseconds(sampleTs)), histMatrix[0].Histograms[1].Timestamp)
	assert.Equal(t, model.FloatString(expectedHist.Count), histMatrix[0].Histograms[1].Histogram.Count)
	assert.Equal(t, model.FloatString(expectedHist.Sum), histMatrix[0].Histograms[1].Histogram.Sum)
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
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.enable-native-histograms":     "true",
			// Ingester.
			"-ring.store":             "consul",
			"-consul.hostname":        consul.NetworkHTTPEndpoint(),
			"-ingester.max-exemplars": "100",
			// Distributor.
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "true",
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

	writeStats, err := c.PushV2(symbols, timeseries)
	require.NoError(t, err)
	testPushHeader(t, writeStats, 1, 0, 1)

	start := time.Now().Add(-time.Minute)
	end := now.Add(time.Minute)

	exemplars, err := c.QueryExemplars("test_metric", start, end)
	require.NoError(t, err)
	require.Equal(t, 1, len(exemplars))
}

func Test_WriteStatWithReplication(t *testing.T) {
	// Test `X-Prometheus-Remote-Write-Samples-Written` header value
	// with the replication.
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
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.enable-native-histograms":     "true",
			// Ingester.
			"-ring.store":             "consul",
			"-consul.hostname":        consul.NetworkHTTPEndpoint(),
			"-ingester.max-exemplars": "100",
			// Distributor.
			"-distributor.replication-factor":     "3",
			"-distributor.remote-writev2-enabled": "true",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)

	// Start Cortex components.
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester1 := e2ecortex.NewIngester("ingester-1", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester2 := e2ecortex.NewIngester("ingester-2", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester3 := e2ecortex.NewIngester("ingester-3", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	// series push
	start := now.Add(-time.Minute * 10)
	numSamples := 20
	scrapeInterval := 30 * time.Second
	symbols, series := e2e.GenerateV2SeriesWithSamples("test_series", start, scrapeInterval, 0, numSamples, prompb.Label{Name: "job", Value: "test"})
	writeStats, err := c.PushV2(symbols, []writev2.TimeSeries{series})
	require.NoError(t, err)
	testPushHeader(t, writeStats, 20, 0, 0)
}

// This test verifies PRW1 and PRW2 memory pools do not interfere with each other.
func TestIngest_PRW2_MemoryIndependence(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-blocks-storage.tsdb.block-ranges-period":         blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":               "1s",
			"-blocks-storage.tsdb.retention-period":            ((blockRangePeriod * 2) - 1).String(),
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor":     "1",
			"-distributor.remote-writev2-enabled": "true",
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

	// Start Cortex
	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until Cortex replicas have updated the ring state.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	cPRW1, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-prw1")
	require.NoError(t, err)

	cPRW2, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-prw2")
	require.NoError(t, err)
	var wg sync.WaitGroup

	scrapeInterval := 5 * time.Second
	end := time.Now()
	start := end.Add(-time.Hour * 2)

	expectedPushesPerProtocol := int(end.Sub(start) / scrapeInterval)

	// We will concurrently push two distinct metrics using two different protocols.
	// test_metric_prw1 is pushed via PRW1 with Value: 1.0
	// test_metric_prw2 is pushed via PRW2 with Value: 999.0
	// If the memory pool overlaps due to shallow copy during V2->V1 conversion,
	// test_metric_prw1 will occasionally read 999.0.
	wg.Add(2)

	// Goroutine 1: Send PRW1 Requests
	go func() {
		defer wg.Done()
		// Iterate from start to end by scrapeInterval
		for t := start; t.Before(end); t = t.Add(scrapeInterval) {
			ts := t.UnixMilli()

			seriesV1 := []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "test_metric_prw1"},
					},
					Samples: []prompb.Sample{
						{Value: 1.0, Timestamp: ts},
					},
				},
			}
			_, _ = cPRW1.Push(seriesV1)
		}
	}()

	// Goroutine 2: Send PRW2 Requests
	go func() {
		defer wg.Done()
		// Iterate from start to end by scrapeInterval
		for t := start; t.Before(end); t = t.Add(scrapeInterval) {
			ts := t.UnixMilli()

			symbols := []string{"", "__name__", "test_metric_prw2"}
			seriesV2 := []writev2.TimeSeries{
				{
					LabelsRefs: []uint32{1, 2},
					Samples:    []writev2.Sample{{Value: 999.0, Timestamp: ts}},
				},
			}
			_, _ = cPRW2.PushV2(symbols, seriesV2)
		}
	}()

	// Wait for all concurrent pushes to finish
	wg.Wait()

	// Check PRW1 requests
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(expectedPushesPerProtocol)), []string{"cortex_distributor_push_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "type", "prw1"))))
	// Check PRW2 requests
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(float64(expectedPushesPerProtocol)), []string{"cortex_distributor_push_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "type", "prw2"))))

	resultV1, err := cPRW1.QueryRange(`test_metric_prw1`, start, end, scrapeInterval)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, resultV1.Type())

	matrixV1, ok := resultV1.(model.Matrix)
	require.True(t, ok)
	require.NotEmpty(t, matrixV1)

	// Validate no data pollution occurred.
	for _, series := range matrixV1 {
		for _, sample := range series.Values {
			assert.Equal(t, 1.0, float64(sample.Value), "Memory pool overlapped: PRW1 metric has been corrupted!")
		}
	}

	resultV2, err := cPRW2.QueryRange(`test_metric_prw2`, start, end, scrapeInterval)
	require.NoError(t, err)
	matrixV2, ok := resultV2.(model.Matrix)
	require.True(t, ok)
	require.NotEmpty(t, matrixV2)

	// Validate no data pollution occurred.
	for _, series := range matrixV2 {
		for _, sample := range series.Values {
			assert.Equal(t, 999.0, float64(sample.Value), "Memory pool overlapped: PRW2 metric has been corrupted!")
		}
	}
}

func testPushHeader(t *testing.T, stats remoteapi.WriteResponseStats, expectedSamples, expectedHistogram, expectedExemplars int) {
	require.Equal(t, expectedSamples, stats.Samples)
	require.Equal(t, expectedHistogram, stats.Histograms)
	require.Equal(t, expectedExemplars, stats.Exemplars)
}

func pushCortexV2Request(t *testing.T, distributorAddr, orgID string, req *cortexpb.WriteRequestV2) {
	t.Helper()

	data, err := req.Marshal()
	require.NoError(t, err)

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/prom/push", distributorAddr), bytes.NewReader(compressed))
	require.NoError(t, err)

	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf;proto=io.prometheus.write.v2.Request")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "2.0.0")
	httpReq.Header.Set("X-Scope-OrgID", orgID)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	res, err := httpClient.Do(httpReq)
	require.NoError(t, err)
	defer res.Body.Close() //nolint:errcheck

	require.Equal(t, http.StatusNoContent, res.StatusCode)
}
