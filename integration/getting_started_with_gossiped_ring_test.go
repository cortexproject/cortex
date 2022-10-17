//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestGettingStartedWithGossipedRing(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, bucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Cortex components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-1.yaml", "config1.yaml"))
	require.NoError(t, copyFileToSharedDir(s, "docs/configuration/single-process-config-blocks-gossip-2.yaml", "config2.yaml"))

	// We don't care for storage part too much here. Both Cortex instances will write new blocks to /tmp, but that's fine.
	flags := map[string]string{
		// decrease timeouts to make test faster. should still be fine with two instances only
		"-ingester.join-after":                                     "0s", // join quickly
		"-ingester.observe-period":                                 "5s", // to avoid conflicts in tokens
		"-blocks-storage.bucket-store.sync-interval":               "1s", // sync continuously
		"-blocks-storage.backend":                                  "s3",
		"-blocks-storage.s3.bucket-name":                           bucketName,
		"-blocks-storage.s3.access-key-id":                         e2edb.MinioAccessKey,
		"-blocks-storage.s3.secret-access-key":                     e2edb.MinioSecretKey,
		"-blocks-storage.s3.endpoint":                              fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-blocks-storage.s3.insecure":                              "true",
		"-store-gateway.sharding-ring.wait-stability-min-duration": "0", // start quickly
		"-store-gateway.sharding-ring.wait-stability-max-duration": "0", // start quickly
	}

	// This cortex will fail to join the cluster configured in yaml file. That's fine.
	cortex1 := e2ecortex.NewSingleBinaryWithConfigFile("cortex-1", "config1.yaml", e2e.MergeFlags(flags, map[string]string{
		"-ingester.lifecycler.addr": networkName + "-cortex-1", // Ingester's hostname in docker setup
	}), "", 9109, 9195)

	cortex2 := e2ecortex.NewSingleBinaryWithConfigFile("cortex-2", "config2.yaml", e2e.MergeFlags(flags, map[string]string{
		"-ingester.lifecycler.addr": networkName + "-cortex-2", // Ingester's hostname in docker setup
		"-memberlist.join":          networkName + "-cortex-1:7946",
	}), "", 9209, 9295)

	require.NoError(t, s.StartAndWaitReady(cortex1))
	require.NoError(t, s.StartAndWaitReady(cortex2))

	// Both Cortex servers should see each other.
	require.NoError(t, cortex1.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))
	require.NoError(t, cortex2.WaitSumMetrics(e2e.Equals(2), "memberlist_client_cluster_members_count"))

	// Both Cortex servers should have 512 tokens for ingesters ring and 512 tokens for store-gateways ring.
	for _, ringName := range []string{"ingester", "store-gateway", "ruler"} {
		ringMatcher := labels.MustNewMatcher(labels.MatchEqual, "name", ringName)

		require.NoError(t, cortex1.WaitSumMetricsWithOptions(e2e.Equals(2*512), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)))
		require.NoError(t, cortex2.WaitSumMetricsWithOptions(e2e.Equals(2*512), []string{"cortex_ring_tokens_total"}, e2e.WithLabelMatchers(ringMatcher)))
	}

	// We need two "ring members" visible from both Cortex instances for ingesters
	require.NoError(t, cortex1.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, cortex2.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// We need two "ring members" visible from both Cortex instances for rulers
	require.NoError(t, cortex1.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ruler"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, cortex2.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ruler"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	c1, err := e2ecortex.NewClient(cortex1.HTTPEndpoint(), cortex1.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	c2, err := e2ecortex.NewClient(cortex2.HTTPEndpoint(), cortex2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Cortex2
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

	// Flush blocks from ingesters to the store.
	for _, instance := range []*e2ecortex.CortexService{cortex1, cortex2} {
		res, err = e2e.GetRequest("http://" + instance.HTTPEndpoint() + "/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)
	}

	// Given store-gateway blocks sharding is enabled with the default replication factor of 3,
	// and ingestion replication factor is 1, we do expect the series has been ingested by 1
	// single ingester and so we have 1 block shipped from ingesters and loaded by both store-gateways.
	require.NoError(t, cortex1.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))
	require.NoError(t, cortex2.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

	// Make sure that no DNS failures occurred.
	// No actual DNS lookups are necessarily performed, so we can't really assert on that.
	mlMatcher := labels.MustNewMatcher(labels.MatchEqual, "name", "memberlist")
	require.NoError(t, cortex1.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_dns_failures_total"}, e2e.WithLabelMatchers(mlMatcher)))
	require.NoError(t, cortex2.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_dns_failures_total"}, e2e.WithLabelMatchers(mlMatcher)))
}
