//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"math"
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

func scrapeMetricsProtobuf(endpoint string) (map[string]*io_prometheus_client.MetricFamily, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited")

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
	decoder := expfmt.NewDecoder(resp.Body, expfmt.FmtProtoDelim)

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

// TestDualModeHistogramExposition validates cortex_ingester_tsdb_compaction_duration_seconds
// is exposed in dual mode with both classic buckets and native histogram fields.
func TestDualModeHistogramExposition(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

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

	families, err := scrapeMetricsProtobuf(fmt.Sprintf("http://%s/metrics", cortex.HTTPEndpoint()))
	require.NoError(t, err)

	histFamily, ok := families["cortex_ingester_tsdb_compaction_duration_seconds"]
	require.True(t, ok)
	require.Equal(t, io_prometheus_client.MetricType_HISTOGRAM, histFamily.GetType())

	metrics := histFamily.GetMetric()
	require.NotEmpty(t, metrics)

	expectedBuckets := []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, math.Inf(1)}

	for _, metric := range metrics {
		h := metric.GetHistogram()
		require.NotNil(t, h)

		buckets := h.GetBucket()
		require.Equal(t, 14, len(buckets)) // testing classic histogram custom buckets

		for i, bucket := range buckets {
			require.Equal(t, expectedBuckets[i], bucket.GetUpperBound()) // testing classic histogram custom bucket boundaries
		}

		// Testing Native histogram fields
		require.Equal(t, int32(3), h.GetSchema())
		require.NotNil(t, h.GetZeroThreshold())
		require.Equal(t, uint64(0), h.GetZeroCount())

		sampleCount := h.GetSampleCount()
		sampleSum := h.GetSampleSum()
		require.Greater(t, sampleSum, 0.0)
		require.Equal(t, sampleCount, buckets[len(buckets)-1].GetCumulativeCount())
	}
}
