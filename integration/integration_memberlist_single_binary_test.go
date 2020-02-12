package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/framework"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	s, err := framework.NewScenario()
	require.NoError(t, err)
	defer s.Shutdown()

	// Start dependencies
	require.NoError(t, s.StartDynamoDB())
	// Look ma, no Consul!
	require.NoError(t, s.WaitReady("dynamodb"))

	require.NoError(t, startSingleBinary(s, "cortex-1", ""))
	require.NoError(t, startSingleBinary(s, "cortex-2", "cortex-1:8000"))
	require.NoError(t, startSingleBinary(s, "cortex-3", "cortex-2:8000"))

	require.NoError(t, s.WaitReady("cortex-1", "cortex-2", "cortex-3"))

	// All three Cortex serves should see each other.
	require.NoError(t, s.Service("cortex-1").WaitMetric(80, "memberlist_client_cluster_members_count", 3))
	require.NoError(t, s.Service("cortex-2").WaitMetric(80, "memberlist_client_cluster_members_count", 3))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "memberlist_client_cluster_members_count", 3))

	// All Cortex servers should have 512 tokens, altogether 3 * 512
	require.NoError(t, s.Service("cortex-1").WaitMetric(80, "cortex_ring_tokens_total", 3*512))
	require.NoError(t, s.Service("cortex-2").WaitMetric(80, "cortex_ring_tokens_total", 3*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "cortex_ring_tokens_total", 3*512))

	require.NoError(t, s.StopService("cortex-1"))
	require.NoError(t, s.Service("cortex-2").WaitMetric(80, "cortex_ring_tokens_total", 2*512))
	require.NoError(t, s.Service("cortex-2").WaitMetric(80, "memberlist_client_cluster_members_count", 2))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "cortex_ring_tokens_total", 2*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "memberlist_client_cluster_members_count", 2))

	require.NoError(t, s.StopService("cortex-2"))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "cortex_ring_tokens_total", 1*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric(80, "memberlist_client_cluster_members_count", 1))

	require.NoError(t, s.StopService("cortex-3"))
}

func startSingleBinary(s *framework.Scenario, name string, join string) error {
	flags := map[string]string{
		"-target":                        "all", // single-binary mode
		"-log.level":                     "warn",
		"-ingester.final-sleep":          "0s",
		"-ingester.join-after":           "0s", // join quickly
		"-ingester.min-ready-duration":   "0s",
		"-ingester.concurrent-flushes":   "10",
		"-ingester.max-transfer-retries": "0", // disable
		"-ingester.num-tokens":           "512",
		"-ingester.observe-period":       "5s", // to avoid conflicts in tokens
		"-ring.store":                    "memberlist",
		"-memberlist.bind-port":          "8000",
	}

	if join != "" {
		flags["-memberlist.join"] = join
	}

	serv := framework.NewService(
		name,
		framework.GetDefaultCortexImage(),
		framework.NetworkName,
		[]int{80, 8000},
		nil,
		framework.NewCommandWithoutEntrypoint("cortex", framework.BuildArgs(framework.MergeFlags(ChunksStorage, flags))...),
		framework.NewReadinessProbe(80, "/ready", 204),
	)

	backOff := util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond, // bump max backoff... things take little longer with memberlist
		MaxRetries: 100,
	}

	serv.SetBackoff(backOff)
	return s.StartService(serv)
}
