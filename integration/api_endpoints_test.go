//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
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
