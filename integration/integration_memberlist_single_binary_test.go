package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies
	dynamo := e2edb.NewDynamoDB()
	// Look ma, no Consul!
	require.NoError(t, s.StartAndWaitReady(dynamo))

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))

	cortex1 := newSingleBinary("cortex-1", "")
	cortex2 := newSingleBinary("cortex-2", networkName+"-cortex-1:8000")
	cortex3 := newSingleBinary("cortex-3", networkName+"-cortex-1:8000")

	// start cortex-1 first, as cortex-2 and cortex-3 both connect to cortex-1
	require.NoError(t, s.StartAndWaitReady(cortex1))
	require.NoError(t, s.StartAndWaitReady(cortex2, cortex3))

	// All three Cortex serves should see each other.
	require.NoError(t, cortex1.WaitSumMetric("memberlist_client_cluster_members_count", 3))
	require.NoError(t, cortex2.WaitSumMetric("memberlist_client_cluster_members_count", 3))
	require.NoError(t, cortex3.WaitSumMetric("memberlist_client_cluster_members_count", 3))

	// All Cortex servers should have 512 tokens, altogether 3 * 512.
	require.NoError(t, cortex1.WaitSumMetric("cortex_ring_tokens_total", 3*512))
	require.NoError(t, cortex2.WaitSumMetric("cortex_ring_tokens_total", 3*512))
	require.NoError(t, cortex3.WaitSumMetric("cortex_ring_tokens_total", 3*512))

	require.NoError(t, s.Stop(cortex1))
	require.NoError(t, cortex2.WaitSumMetric("cortex_ring_tokens_total", 2*512))
	require.NoError(t, cortex2.WaitSumMetric("memberlist_client_cluster_members_count", 2))
	require.NoError(t, cortex3.WaitSumMetric("cortex_ring_tokens_total", 2*512))
	require.NoError(t, cortex3.WaitSumMetric("memberlist_client_cluster_members_count", 2))

	require.NoError(t, s.Stop(cortex2))
	require.NoError(t, cortex3.WaitSumMetric("cortex_ring_tokens_total", 1*512))
	require.NoError(t, cortex3.WaitSumMetric("memberlist_client_cluster_members_count", 1))

	require.NoError(t, s.Stop(cortex3))
}

func newSingleBinary(name string, join string) *e2e.HTTPService {
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

	serv := e2ecortex.NewSingleBinary(
		name,
		mergeFlags(ChunksStorage, flags),
		"",
		80,
		8000,
	)

	backOff := util.BackoffConfig{
		MinBackoff: 200 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond, // Bump max backoff... things take little longer with memberlist.
		MaxRetries: 100,
	}

	serv.SetBackoff(backOff)
	return serv
}
