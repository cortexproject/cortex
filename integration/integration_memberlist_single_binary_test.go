// +build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/ca"
	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/util"
)

func TestSingleBinaryWithMemberlist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testSingleBinaryEnv(t, false)
	})

	t.Run("tls", func(t *testing.T) {
		testSingleBinaryEnv(t, true)
	})
}

func testSingleBinaryEnv(t *testing.T, tlsEnabled bool) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies
	dynamo := e2edb.NewDynamoDB()
	// Look ma, no Consul!
	require.NoError(t, s.StartAndWaitReady(dynamo))

	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(cortexSchemaConfigYaml)))
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

		cortex1 = newSingleBinary("cortex-1", memberlistDNS, "")
		cortex2 = newSingleBinary("cortex-2", memberlistDNS, networkName+"-cortex-1:8000")
		cortex3 = newSingleBinary("cortex-3", memberlistDNS, networkName+"-cortex-1:8000")
	} else {
		cortex1 = newSingleBinary("cortex-1", "", "")
		cortex2 = newSingleBinary("cortex-2", "", networkName+"-cortex-1:8000")
		cortex3 = newSingleBinary("cortex-3", "", networkName+"-cortex-1:8000")
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

	require.NoError(t, s.Stop(cortex1))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(2*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))

	require.NoError(t, s.Stop(cortex2))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1*512), "cortex_ring_tokens_total"))
	require.NoError(t, cortex3.WaitSumMetrics(e2e.Equals(1), "memberlist_client_cluster_members_count"))

	require.NoError(t, s.Stop(cortex3))
}

func newSingleBinary(name string, servername string, join string) *e2ecortex.CortexService {
	flags := map[string]string{
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
		mergeFlags(
			ChunksStorageFlags(),
			flags,
			getTLSFlagsWithPrefix("memberlist", servername, servername == ""),
		),
		"",
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
