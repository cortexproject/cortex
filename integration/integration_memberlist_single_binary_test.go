// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/util/backoff"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testSingleBinaryEnv(t, false, nil)
	})

	t.Run("tls", func(t *testing.T) {
		testSingleBinaryEnv(t, true, nil)
	})

	t.Run("compression-disabled", func(t *testing.T) {
		testSingleBinaryEnv(t, false, map[string]string{
			"-memberlist.compression-enabled": "false",
		})
	})
}

func testSingleBinaryEnv(t *testing.T, tlsEnabled bool, flags map[string]string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies
	minio := e2edb.NewMinio(9000, bucketName)
	// Look ma, no Consul!
	require.NoError(t, s.StartAndWaitReady(minio))

	var cortex1, cortex2, cortex3 *e2ecortex.CortexService
	if tlsEnabled {
		var (
			memberlistDNS = "cortex-memberlist"
		)
		// set the ca
		cert := ca.New("single-binary-memberlist")

		// Ensure the entire path of directories exist.
		require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))
		require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))
		require.NoError(t, cert.WriteCertificate(
			&x509.Certificate{
				Subject: pkix.Name{CommonName: "memberlist"},
				DNSNames: []string{
					memberlistDNS,
				},
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			},
			filepath.Join(s.SharedDir(), clientCertFile),
			filepath.Join(s.SharedDir(), clientKeyFile),
		))

		cortex1 = newSingleBinary("cortex-1", memberlistDNS, "", flags)
		cortex2 = newSingleBinary("cortex-2", memberlistDNS, networkName+"-cortex-1:8000", flags)
		cortex3 = newSingleBinary("cortex-3", memberlistDNS, networkName+"-cortex-1:8000", flags)
	} else {
		cortex1 = newSingleBinary("cortex-1", "", "", flags)
		cortex2 = newSingleBinary("cortex-2", "", networkName+"-cortex-1:8000", flags)
		cortex3 = newSingleBinary("cortex-3", "", networkName+"-cortex-1:8000", flags)
	}

	// start cortex-1 first, as cortex-2 and cortex-3 both connect to cortex-1
	require.NoError(t, s.StartAndWaitReady(cortex1))
	require.NoError(t, s.StartAndWaitReady(cortex2, cortex3))

	// All three Cortex serves should see each other.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(3), "memberlist_client_cluster_members_count"))

	// All Cortex servers should have 512 tokens, altogether 3 * 512.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(3*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(3*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(3*512), "cortex_ring_tokens_total"))

	// All Cortex servers should initially have no tombstones; nobody has left yet.
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))

	require.NoError(t, s.Stop(cortex1))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1), "memberlist_client_kv_store_value_tombstones"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1), "memberlist_client_kv_store_value_tombstones"))

	require.NoError(t, s.Stop(cortex2))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(2), "memberlist_client_kv_store_value_tombstones"))

	require.NoError(t, s.Stop(cortex3))
}

func newSingleBinary(name string, servername string, join string, testFlags map[string]string) *e2ecortex.CortexService {
	flags := map[string]string{
		"-ingester.final-sleep":              "0s",
		"-ingester.join-after":               "0s", // join quickly
		"-ingester.min-ready-duration":       "0s",
		"-ingester.concurrent-flushes":       "10",
		"-ingester.num-tokens":               "512",
		"-ingester.observe-period":           "5s", // to avoid conflicts in tokens
		"-ring.store":                        "memberlist",
		"-memberlist.bind-port":              "8000",
		"-memberlist.left-ingesters-timeout": "600s", // effectively disable
	}

	if join != "" {
		flags["-memberlist.join"] = join
	}

	serv := e2ecortex.NewSingleBinary(
		name,
		mergeFlags(
			BlocksStorageFlags(),
			flags,
			testFlags,
			getTLSFlagsWithPrefix("memberlist", servername, servername == ""),
		),
		"",
		8000,
	)

	backOff := backoff.Config{
		MinBackoff: 200 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond, // Bump max backoff... things take little longer with memberlist.
		MaxRetries: 100,
	}

	serv.SetBackoff(backOff)
	return serv
}

func TestSingleBinaryWithMemberlistScaling(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Scale up instances. These numbers seem enough to reliably reproduce some unwanted
	// consequences of slow propagation, such as missing tombstones.

	maxCortex := 30
	minCortex := 3
	instances := make([]*e2ecortex.CortexService, 0)

	for i := 0; i < maxCortex; i++ {
		name := fmt.Sprintf("cortex-%d", i+1)
		join := ""
		if i > 0 {
			join = e2e.NetworkContainerHostPort(networkName, "cortex-1", 8000)
		}
		c := newSingleBinary(name, "", join, nil)
		require.NoError(t, s.StartAndWaitReady(c))
		instances = append(instances, c)
	}

	// Sanity check the ring membership and give each instance time to see every other instance.

	for _, c := range instances {
		require.NoError(t, c.WaitSumMetrics(e2e.Equals(float64(maxCortex)), "cortex_ring_members"))
		require.NoError(t, c.WaitSumMetrics(e2e.Equals(0), "memberlist_client_kv_store_value_tombstones"))
		require.NoError(t, c.WaitSumMetrics(e2e.Equals(0), "memberlist_client_received_broadcasts_invalid_total"))
	}

	// Scale down as fast as possible but cleanly, in order to send out tombstones.

	stop := errgroup.Group{}
	for len(instances) > minCortex {
		i := len(instances) - 1
		c := instances[i]
		instances = instances[:i]
		stop.Go(func() error { return s.Stop(c) })

		// TODO(#4360): Remove this when issue is resolved.
		//   Wait until memberlist for all nodes has recognised the instance left.
		//   This means that we will not gossip tombstones to leaving nodes.
		for _, c := range instances {
			require.NoError(t, c.WaitSumMetrics(e2e.Equals(float64(len(instances))), "memberlist_client_cluster_members_count"))
		}
	}
	require.NoError(t, stop.Wait())

	// If all is working as expected, then tombstones should have propagated easily within this time period.
	// The logging is mildly spammy, but it has proven extremely useful for debugging convergence cases.
	// We don't use WaitSumMetrics [over all instances] here so we can log the per-instance metrics.

	expectedRingMembers := float64(minCortex)
	expectedTombstones := float64(maxCortex - minCortex)

	require.Eventually(t, func() bool {
		ok := true
		for _, c := range instances {
			metrics, err := c.SumMetrics([]string{
				"cortex_ring_members", "memberlist_client_kv_store_value_tombstones",
			})
			require.NoError(t, err)
			t.Logf("%s: cortex_ring_members=%f memberlist_client_kv_store_value_tombstones=%f\n",
				c.Name(), metrics[0], metrics[1])

			// Don't short circuit the check, so we log the state for all instances.
			if metrics[0] != expectedRingMembers {
				ok = false
			}
			if metrics[1] != expectedTombstones {
				ok = false
			}

		}
		return ok
	}, 30*time.Second, 2*time.Second,
		"expected all instances to have %f ring members and %f tombstones",
		expectedRingMembers, expectedTombstones)
}
