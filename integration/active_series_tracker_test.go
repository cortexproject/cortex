//go:build requires_docker

package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestActiveSeriesTrackerPerTenant(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Write runtime config with per-tenant active series trackers.
	runtimeConfig := map[string]interface{}{
		"overrides": map[string]interface{}{
			"user-1": map[string]interface{}{
				"active_series_trackers": []map[string]string{
					{"name": "api_metrics", "matchers": `{__name__=~"api_.*"}`},
					{"name": "node_metrics", "matchers": `{__name__=~"node_.*"}`},
				},
			},
		},
	}
	runtimeCfgYAML, err := yaml.Marshal(runtimeConfig)
	require.NoError(t, err)
	require.NoError(t, writeFileToSharedDir(s, runtimeConfigFile, runtimeCfgYAML))

	flags := BlocksStorageFlags()
	flags["-distributor.shard-by-all-labels"] = "true"
	flags["-ingester.active-series-metrics-enabled"] = "true"
	flags["-ingester.active-series-metrics-update-period"] = "2s"
	flags["-ingester.active-series-metrics-idle-timeout"] = "5m"
	flags["-runtime-config.file"] = filepath.Join(e2e.ContainerSharedDir, runtimeConfigFile)
	flags["-runtime-config.reload-period"] = "1s"
	flags["-alertmanager.web.external-url"] = "http://localhost/alertmanager"
	flags["-alertmanager-storage.backend"] = "local"
	flags["-alertmanager-storage.local.path"] = filepath.Join(e2e.ContainerSharedDir, "alertmanager_configs")

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags["-ring.store"] = "consul"
	flags["-consul.hostname"] = consul.NetworkHTTPEndpoint()

	cortex := e2ecortex.NewSingleBinary("cortex-1", flags, "")
	require.NoError(t, s.StartAndWaitReady(cortex))

	// Wait until the ring is ready.
	require.NoError(t, cortex.WaitSumMetrics(e2e.Equals(float64(512)), "cortex_ring_tokens_total"))

	c, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	for _, name := range []string{"api_requests_total", "api_errors_total", "node_cpu_seconds", "process_memory_bytes"} {
		series, _ := generateSeries(name, now, prompb.Label{Name: "job", Value: "test"})
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode, fmt.Sprintf("push %s failed", name))
	}

	// user-1 has trackers: api_metrics (matches 2), node_metrics (matches 1).
	require.NoError(t, cortex.WaitSumMetricsWithOptions(
		e2e.Equals(2),
		[]string{"cortex_ingester_active_series_per_tracker"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"),
			labels.MustNewMatcher(labels.MatchEqual, "name", "api_metrics"),
		),
		e2e.WaitMissingMetrics,
	))

	require.NoError(t, cortex.WaitSumMetricsWithOptions(
		e2e.Equals(1),
		[]string{"cortex_ingester_active_series_per_tracker"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"),
			labels.MustNewMatcher(labels.MatchEqual, "name", "node_metrics"),
		),
		e2e.WaitMissingMetrics,
	))

	// user-2 has no trackers configured — should have no tracker metrics.
	c2, err := e2ecortex.NewClient(cortex.HTTPEndpoint(), cortex.HTTPEndpoint(), "", "", "user-2")
	require.NoError(t, err)

	series2, _ := generateSeries("api_requests_total", now, prompb.Label{Name: "job", Value: "test"})
	res, err := c2.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait for user-2 active series to be counted.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(
		e2e.Equals(1),
		[]string{"cortex_ingester_active_series"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-2")),
		e2e.WaitMissingMetrics,
	))

	// user-2 should have no tracker metrics.
	sum, err := cortex.SumMetrics(
		[]string{"cortex_ingester_active_series_per_tracker"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-2")),
		e2e.SkipMissingMetrics,
	)
	require.NoError(t, err)
	require.Equal(t, 0.0, sum[0])

	// Now update runtime config: remove node_metrics tracker for user-1.
	runtimeConfig2 := map[string]interface{}{
		"overrides": map[string]interface{}{
			"user-1": map[string]interface{}{
				"active_series_trackers": []map[string]string{
					{"name": "api_metrics", "matchers": `{__name__=~"api_.*"}`},
				},
			},
		},
	}
	runtimeCfgYAML2, err := yaml.Marshal(runtimeConfig2)
	require.NoError(t, err)
	require.NoError(t, writeFileToSharedDir(s, runtimeConfigFile, runtimeCfgYAML2))

	// Wait for the stale node_metrics tracker metric to be removed.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(
		e2e.Equals(0),
		[]string{"cortex_ingester_active_series_per_tracker"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"),
			labels.MustNewMatcher(labels.MatchEqual, "name", "node_metrics"),
		),
		e2e.SkipMissingMetrics,
	))

	// api_metrics tracker should still work.
	require.NoError(t, cortex.WaitSumMetricsWithOptions(
		e2e.Equals(2),
		[]string{"cortex_ingester_active_series_per_tracker"},
		e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"),
			labels.MustNewMatcher(labels.MatchEqual, "name", "api_metrics"),
		),
		e2e.WaitMissingMetrics,
	))
}
