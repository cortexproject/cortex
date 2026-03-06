//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIndexAPIEndpoint(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	configOverrides := map[string]string{
		// alert manager
		"-alertmanager.web.external-url":   "http://localhost/alertmanager",
		"-alertmanager-storage.backend":    "local",
		"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
	}
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	// Start Cortex in single binary mode, reading the config from file.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	cortex1 := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex1))

	// GET / should succeed
	res, err := e2e.GetRequest(fmt.Sprintf("http://%s", cortex1.Endpoint(9009)))
	require.NoError(t, err)
	assert.Equal(t, 200, res.StatusCode)

	// POST / should fail
	res, err = e2e.PostRequest(fmt.Sprintf("http://%s", cortex1.Endpoint(9009)))
	require.NoError(t, err)
	assert.Equal(t, 405, res.StatusCode)
}

func TestConfigAPIEndpoint(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	configOverrides := map[string]string{
		// alert manager
		"-alertmanager.web.external-url":   "http://localhost/alertmanager",
		"-alertmanager-storage.backend":    "local",
		"-alertmanager-storage.local.path": filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs"),
	}
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	// Start Cortex in single binary mode, reading the config from file.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-local.yaml", cortexConfigFile))

	cortex1 := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex1))

	// Get config from /config API endpoint.
	res, err := e2e.GetRequest(fmt.Sprintf("http://%s/config", cortex1.Endpoint(9009)))
	require.NoError(t, err)

	defer runutil.ExhaustCloseWithErrCapture(&err, res.Body, "config API response")
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Start again Cortex in single binary with the exported config
	// and ensure it starts (pass the readiness probe).
	require.NoError(t, writeFileToSharedDir(s, cortexConfigFile, body))
	configOverrides["-alertmanager.cluster.peers"] = cortex1.HTTPEndpoint()
	cortex2 := e2ecortex.NewSingleBinaryWithConfigFile("cortex-2", cortexConfigFile, configOverrides, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(cortex2))
}

func Test_AllUserStats_WhenIngesterRollingUpdate(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := BlocksStorageFlags()
	flags["-distributor.replication-factor"] = "3"
	flags["-distributor.sharding-strategy"] = "shuffle-sharding"
	flags["-distributor.ingestion-tenant-shard-size"] = "3"
	flags["-distributor.shard-by-all-labels"] = "true"

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

	// stop ingester1 to emulate rolling update
	require.NoError(t, s.Stop(ingester1))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	series, _ := generateSeries("series_1", now)
	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// QueriedIngesters is 2 since ingester1 has been stopped.
	userStats, err := client.AllUserStats()
	require.NoError(t, err)
	require.Len(t, userStats, 1)
	require.Equal(t, uint64(2), userStats[0].QueriedIngesters)
}

func TestTSDBStatus(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := BlocksStorageFlags()
	flags["-distributor.replication-factor"] = "1"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex in single binary mode.
	cortex := e2ecortex.NewSingleBinary("cortex-1", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until the ingester ring is active.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "test-tenant")
	require.NoError(t, err)

	now := time.Now()

	// Push multiple series to create interesting cardinality:
	// - http_requests_total with 3 label combinations
	// - process_cpu_seconds_total with 1 label combination
	series1, _ := generateSeries("http_requests_total", now, prompb.Label{Name: "method", Value: "GET"}, prompb.Label{Name: "status", Value: "200"})
	series2, _ := generateSeries("http_requests_total", now, prompb.Label{Name: "method", Value: "POST"}, prompb.Label{Name: "status", Value: "200"})
	series3, _ := generateSeries("http_requests_total", now, prompb.Label{Name: "method", Value: "GET"}, prompb.Label{Name: "status", Value: "500"})
	series4, _ := generateSeries("process_cpu_seconds_total", now, prompb.Label{Name: "instance", Value: "a"})

	allSeries := append(series1, series2...)
	allSeries = append(allSeries, series3...)
	allSeries = append(allSeries, series4...)

	res, err := client.Push(allSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query TSDB status with default limit.
	status, err := client.TSDBStatus(10)
	require.NoError(t, err)

	assert.Equal(t, uint64(4), status.NumSeries)
	require.GreaterOrEqual(t, len(status.SeriesCountByMetricName), 2)
	assert.Equal(t, "http_requests_total", status.SeriesCountByMetricName[0].Name)
	assert.Equal(t, uint64(3), status.SeriesCountByMetricName[0].Value)
	assert.Equal(t, "process_cpu_seconds_total", status.SeriesCountByMetricName[1].Name)
	assert.Equal(t, uint64(1), status.SeriesCountByMetricName[1].Value)
	assert.NotEmpty(t, status.LabelValueCountByLabelName)
	assert.Greater(t, status.MinTime, int64(0))
	assert.Greater(t, status.MaxTime, int64(0))

	// Query TSDB status with limit=1 to verify truncation.
	status, err = client.TSDBStatus(1)
	require.NoError(t, err)
	assert.Len(t, status.SeriesCountByMetricName, 1)
	assert.Equal(t, "http_requests_total", status.SeriesCountByMetricName[0].Name)
}
