package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestZoneAwareReadPath(t *testing.T) {
	const numSeriesToPush = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := BlocksStorageFlags()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Cortex components.
	zoneFlags := func(zone string) map[string]string {
		zoneflags := mergeFlags(flags, map[string]string{
			"-ingester.availability-zone":         zone,
			"-distributor.replication-factor":     "3",
			"-distributor.zone-awareness-enabled": "true",
		})
		return zoneflags
	}

	zoneAwarenessEnabledFlags := mergeFlags(flags, map[string]string{
		"-distributor.zone-awareness-enabled": "true",
		"-distributor.replication-factor":     "3",
	})

	ingester1 := e2ecortex.NewIngesterWithConfigFile("ingester-1", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-a"), "")
	ingester2 := e2ecortex.NewIngesterWithConfigFile("ingester-2", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-a"), "")
	ingester3 := e2ecortex.NewIngesterWithConfigFile("ingester-3", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-b"), "")
	ingester4 := e2ecortex.NewIngesterWithConfigFile("ingester-4", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-b"), "")
	ingester5 := e2ecortex.NewIngesterWithConfigFile("ingester-5", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-c"), "")
	ingester6 := e2ecortex.NewIngesterWithConfigFile("ingester-6", consul.NetworkHTTPEndpoint(), "", zoneFlags("zone-c"), "")
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3, ingester4, ingester5, ingester6))

	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), zoneAwarenessEnabledFlags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), zoneAwarenessEnabledFlags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and queriers have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push 100 series
	now := time.Now()
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= numSeriesToPush; i++ {
		metricName := fmt.Sprintf("series_%d", i)
		series, expectedVector := generateSeries(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Query back 100 series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 ingester in 1 zone
	require.NoError(t, ingester5.Kill())

	// Query back 100 series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 more ingester in the same zone
	require.NoError(t, ingester6.Kill())

	// Query back 100 series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 more ingester in a different zone
	require.NoError(t, ingester1.Kill())

	// Query back 100 series => fail
	for i := 1; i <= numSeriesToPush; i++ {
		metricName := fmt.Sprintf("series_%d", i)
		result, _, err := client.QueryRaw(metricName)
		require.NoError(t, err)
		require.Equal(t, 500, result.StatusCode)
	}
}
