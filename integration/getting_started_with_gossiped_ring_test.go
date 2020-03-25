// +build requires_docker

package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestGettingStartedWithGossipedRing(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-1.yaml", "config1.yaml"))
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-2.yaml", "config2.yaml"))

	// We don't care for storage part too much here. Both Cortex instances will write new blocks to /tmp, but that's fine.
	flags := map[string]string{
		// decrease timeouts to make test faster. should still be fine with two instances only
		"-ingester.join-after":     "0s", // join quickly
		"-ingester.observe-period": "5s", // to avoid conflicts in tokens
	}

	// This cortex will fail to join the cluster configured in yaml file. That's fine.
	cortex1 := e2ecortex.NewSingleBinary("cortex-1", e2e.MergeFlags(flags, map[string]string{
		"-config.file":              filepath.Join(e2e.ContainerSharedDir, "config1.yaml"),
		"-ingester.lifecycler.addr": networkName + "-cortex-1", // Ingester's hostname in docker setup
	}), "", 9109, 9195)

	cortex2 := e2ecortex.NewSingleBinary("cortex-2", e2e.MergeFlags(flags, map[string]string{
		"-config.file":              filepath.Join(e2e.ContainerSharedDir, "config2.yaml"),
		"-ingester.lifecycler.addr": networkName + "-cortex-2", // Ingester's hostname in docker setup
		"-memberlist.join":          networkName + "-cortex-1:7946",
	}), "", 9209, 9295)

	require.NoError(t, s.StartAndWaitReady(cortex1))
	require.NoError(t, s.StartAndWaitReady(cortex2))

	// Both Cortex serves should see each other.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))

	// Both Cortex servers should have 512 tokens
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))

	// We need two "ring members" visible from both Cortex instances
	require.NoError(t, cortex1.WaitForMetricWithLabels(e2e.EqualsSingle(2), "cortex_ring_members", map[string]string{"name": "ingester", "state": "ACTIVE"}))
	require.NoError(t, cortex2.WaitForMetricWithLabels(e2e.EqualsSingle(2), "cortex_ring_members", map[string]string{"name": "ingester", "state": "ACTIVE"}))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex2 (Cortex1 may not yet see Cortex2 as ACTIVE due to gossip, so we play it safe by pushing to Cortex2)
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now)

	res, err := c2.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series via Cortex 1
	result, err := c1.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))
}
