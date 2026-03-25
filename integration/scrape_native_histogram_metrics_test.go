//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func scrapeMetrics(endpoint string, useProtobuf bool) (map[string]*io_prometheus_client.MetricFamily, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var format expfmt.Format
	if useProtobuf {
		req.Header.Set("Accept", "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited")
		format = expfmt.NewFormat(expfmt.TypeProtoDelim)
	} else {
		format = expfmt.NewFormat(expfmt.TypeTextPlain)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code %d", resp.StatusCode)
	}

	families := make(map[string]*io_prometheus_client.MetricFamily)
	decoder := expfmt.NewDecoder(resp.Body, format)

	for {
		mf := &io_prometheus_client.MetricFamily{}
		err := decoder.Decode(mf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		families[mf.GetName()] = mf
	}

	return families, nil
}

func setupCortexWithNativeHistograms(t *testing.T) (*e2e.Scenario, *e2ecortex.CortexService) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	consul := e2edb.NewConsulWithName("consul")
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags(), map[string]string{
		"-blocks-storage.tsdb.enable-native-histograms":     "true",
		"-blocks-storage.tsdb.head-compaction-interval":     "1s",
		"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
		"-alertmanager.web.external-url":                    "http://localhost/alertmanager",
		"-alertmanager.cluster.listen-address":              "127.0.0.1:9094",
		"-alertmanager.cluster.advertise-address":           "127.0.0.1:9094",
		"-ring.store":      "consul",
		"-consul.hostname": consul.NetworkHTTPEndpoint(),
	})

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	cortex := e2ecortex.NewSingleBinary("cortex", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	baseTime := time.Now()
	for i := 0; i < 100; i++ {
		series := []prompb.TimeSeries{{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "test_metric"},
				{Name: "job", Value: "test"},
				{Name: "instance", Value: fmt.Sprintf("instance-%d", i%10)},
			},
			Samples: []prompb.Sample{
				{Value: float64(i), Timestamp: e2e.TimeToMilliseconds(baseTime.Add(time.Duration(i) * time.Second))},
			},
		}}
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(100), "cortex_ingester_ingested_samples_total"))
	time.Sleep(5 * time.Second)

	return s, cortex
}

func TestNativeHistogramExposition(t *testing.T) {
	s, cortex := setupCortexWithNativeHistograms(t)
	defer s.Close()

	families, err := scrapeMetrics(fmt.Sprintf("http://%s/metrics", cortex.HTTPEndpoint()), true)
	require.NoError(t, err)

	histFamily, ok := families["cortex_ingester_tsdb_compaction_duration_seconds"]
	require.True(t, ok)
	require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, histFamily.GetType())

	metrics := histFamily.GetMetric()
	require.NotEmpty(t, metrics)

	for _, metric := range metrics {
		h := metric.GetHistogram()
		require.NotNil(t, h)

		require.Equal(t, int32(3), h.GetSchema())
		require.NotNil(t, h.GetZeroThreshold())
		require.Equal(t, uint64(0), h.GetZeroCount())

		sampleCount := h.GetSampleCount()
		require.Greater(t, sampleCount, uint64(0))
		require.Greater(t, h.GetSampleSum(), 0.0)

		posSpans := h.GetPositiveSpan()
		require.NotEmpty(t, posSpans)
		posDeltas := h.GetPositiveDelta()
		require.NotEmpty(t, posDeltas)

		var posCount uint64
		var count int64
		for _, delta := range posDeltas {
			count += delta
			posCount += uint64(count)
		}
		require.Equal(t, sampleCount, posCount)

		negSpans := h.GetNegativeSpan()
		require.Empty(t, negSpans)
		require.Empty(t, h.GetNegativeDelta())
	}
}

func TestClassicHistogramExposition(t *testing.T) {
	s, cortex := setupCortexWithNativeHistograms(t)
	defer s.Close()

	families, err := scrapeMetrics(fmt.Sprintf("http://%s/metrics", cortex.HTTPEndpoint()), false)
	require.NoError(t, err)

	histFamily, ok := families["cortex_ingester_tsdb_compaction_duration_seconds"]
	require.True(t, ok)
	require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, histFamily.GetType())

	metrics := histFamily.GetMetric()
	require.NotEmpty(t, metrics)

	for _, metric := range metrics {
		h := metric.GetHistogram()
		require.NotNil(t, h)

		buckets := h.GetBucket()
		require.NotEmpty(t, buckets)

		sampleCount := h.GetSampleCount()
		require.Greater(t, sampleCount, uint64(0))
		require.Greater(t, h.GetSampleSum(), 0.0)

		lastBucket := buckets[len(buckets)-1]
		require.Equal(t, sampleCount, lastBucket.GetCumulativeCount())
	}
}
