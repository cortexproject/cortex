//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestIngesterMetadata(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())

	minio := e2edb.NewMinio(9000, baseFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(baseFlags, map[string]string{
		// alert manager
		"-alertmanager.web.external-url": "http://localhost/alertmanager",
		// consul
		"-ring.store":      "consul",
		"-consul.hostname": consul.NetworkHTTPEndpoint(),
	})

	// Start Cortex components
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until querier has updated the ring.
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	metadataMetricNum := 5
	metadataPerMetrics := 2
	metadata := make([]prompb.MetricMetadata, 0, metadataMetricNum)
	for i := 0; i < metadataMetricNum; i++ {
		for j := 0; j < metadataPerMetrics; j++ {
			metadata = append(metadata, prompb.MetricMetadata{
				MetricFamilyName: fmt.Sprintf("metadata_name_%d", i),
				Help:             fmt.Sprintf("metadata_help_%d_%d", i, j),
				Unit:             fmt.Sprintf("metadata_unit_%d_%d", i, j),
			})
		}
	}
	res, err := client.Push(nil, metadata...)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	testMetadataQueryParams(t, client, metadataMetricNum, metadataPerMetrics)
}

func TestIngesterMetadataWithTenantFederation(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	baseFlags := mergeFlags(AlertmanagerLocalFlags(), BlocksStorageFlags())

	minio := e2edb.NewMinio(9000, baseFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(baseFlags, map[string]string{
		// tenant federation
		"-tenant-federation.enabled": "true",
		// alert manager
		"-alertmanager.web.external-url": "http://localhost/alertmanager",
		// consul
		"-ring.store":      "consul",
		"-consul.hostname": consul.NetworkHTTPEndpoint(),
	})

	// Start Cortex components
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until querier has updated the ring.
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	metadataMetricNum := 5
	metadataPerMetrics := 2
	metadata := make([]prompb.MetricMetadata, 0, metadataMetricNum)
	for i := 0; i < metadataMetricNum; i++ {
		for j := 0; j < metadataPerMetrics; j++ {
			metadata = append(metadata, prompb.MetricMetadata{
				MetricFamilyName: fmt.Sprintf("metadata_name_%d", i),
				Help:             fmt.Sprintf("metadata_help_%d_%d", i, j),
				Unit:             fmt.Sprintf("metadata_unit_%d_%d", i, j),
			})
		}
	}

	numUsers := 2
	tenantIDs := make([]string, numUsers)
	for u := 0; u < numUsers; u++ {
		tenantIDs[u] = fmt.Sprintf("user-%d", u)
		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", tenantIDs[u])
		require.NoError(t, err)

		res, err := c.Push(nil, metadata...)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", strings.Join(tenantIDs, "|"))
	require.NoError(t, err)

	testMetadataQueryParams(t, client, metadataMetricNum, metadataPerMetrics)
}

func testMetadataQueryParams(t *testing.T, client *e2ecortex.Client, metadataMetricNum, metadataPerMetrics int) {
	t.Run("test no parameter", func(t *testing.T) {
		result, err := client.Metadata("", "")
		require.NoError(t, err)
		require.Equal(t, metadataMetricNum, len(result))

		for _, v := range result {
			require.Equal(t, metadataPerMetrics, len(v))
		}
	})

	t.Run("test name parameter", func(t *testing.T) {
		t.Run("existing name", func(t *testing.T) {
			name := "metadata_name_0"
			result, err := client.Metadata(name, "")
			require.NoError(t, err)
			m, ok := result[name]
			require.True(t, ok)
			require.Equal(t, metadataPerMetrics, len(m))
		})
		t.Run("existing name with limit 0", func(t *testing.T) {
			name := "metadata_name_0"
			result, err := client.Metadata(name, "0")
			require.NoError(t, err)
			require.Equal(t, 0, len(result))
		})
		t.Run("non-existing name", func(t *testing.T) {
			result, err := client.Metadata("dummy", "")
			require.NoError(t, err)
			require.Equal(t, 0, len(result))
		})
	})

	t.Run("test limit parameter", func(t *testing.T) {
		t.Run("less than length of metadata", func(t *testing.T) {
			result, err := client.Metadata("", "3")
			require.NoError(t, err)
			require.Equal(t, 3, len(result))
		})
		t.Run("limit: 0", func(t *testing.T) {
			result, err := client.Metadata("", "0")
			require.NoError(t, err)
			require.Equal(t, 0, len(result))
		})
		t.Run("invalid limit", func(t *testing.T) {
			_, err := client.Metadata("", "dummy")
			require.Error(t, err)
		})
	})
}
