package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	s, err := e2e.NewScenario()
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	require.NoError(t, s.StartService(e2edb.NewDynamoDB()))
	// Look ma, no Consul!
	require.NoError(t, s.WaitReady("dynamodb"))

	require.NoError(t, ioutil.WriteFile(
		filepath.Join(s.SharedDir(), cortexSchemaConfigFile),
		[]byte(cortexSchemaConfigYaml),
		os.ModePerm),
	)
	require.NoError(t, startSingleBinary(s, "cortex-1", ""))
	require.NoError(t, startSingleBinary(s, "cortex-2", "cortex-1:8000"))
	require.NoError(t, startSingleBinary(s, "cortex-3", "cortex-2:8000"))

	require.NoError(t, s.WaitReady("cortex-1", "cortex-2", "cortex-3"))

	// All three Cortex serves should see each other.
	require.NoError(t, s.Service("cortex-1").WaitMetric("memberlist_client_cluster_members_count", 3))
	require.NoError(t, s.Service("cortex-2").WaitMetric("memberlist_client_cluster_members_count", 3))
	require.NoError(t, s.Service("cortex-3").WaitMetric("memberlist_client_cluster_members_count", 3))

	// All Cortex servers should have 512 tokens, altogether 3 * 512.
	require.NoError(t, s.Service("cortex-1").WaitMetric("cortex_ring_tokens_total", 3*512))
	require.NoError(t, s.Service("cortex-2").WaitMetric("cortex_ring_tokens_total", 3*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric("cortex_ring_tokens_total", 3*512))

	require.NoError(t, s.StopService("cortex-1"))
	require.NoError(t, s.Service("cortex-2").WaitMetric("cortex_ring_tokens_total", 2*512))
	require.NoError(t, s.Service("cortex-2").WaitMetric("memberlist_client_cluster_members_count", 2))
	require.NoError(t, s.Service("cortex-3").WaitMetric("cortex_ring_tokens_total", 2*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric("memberlist_client_cluster_members_count", 2))

	require.NoError(t, s.StopService("cortex-2"))
	require.NoError(t, s.Service("cortex-3").WaitMetric("cortex_ring_tokens_total", 1*512))
	require.NoError(t, s.Service("cortex-3").WaitMetric("memberlist_client_cluster_members_count", 1))

	require.NoError(t, s.StopService("cortex-3"))
}

func startSingleBinary(s *e2e.Scenario, name string, join string) error {
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
		"-memberlist.pullpush-interval":  "3s", // speed up state convergence to make test faster and avoid flakiness
	}

	if join != "" {
		flags["-memberlist.join"] = join
	}

	serv := e2e.NewService(
		name,
		e2ecortex.GetDefaultImage(),
		e2e.NetworkName,
		[]int{80, 8000},
		nil,
		e2e.NewCommandWithoutEntrypoint("cortex", buildArgs(mergeFlags(ChunksStorage, flags))...),
		e2e.NewReadinessProbe(80, "/ready", 204),
	)

	backOff := util.BackoffConfig{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond, // Bump max backoff... things take little longer with memberlist.
		MaxRetries: 100,
	}

	serv.SetBackoff(backOff)
	return s.StartService(serv)
}
