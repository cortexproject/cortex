package integration

import (
	"strconv"
	"testing"

	"github.com/cortexproject/cortex/integration/e2e"
	e2ecache "github.com/cortexproject/cortex/integration/e2e/cache"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/stretchr/testify/require"
)

func TestZoneAwareReadPath(t *testing.T) {
	// Going to high starts hitting filedescriptor limit, since we run all queriers concurrently.
	const numQueries = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start a cortex cluster with 6 ingesters and zone-awareness enabled
	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		"-querier.cache-results":                       "true",
		"-querier.split-queries-by-interval":           "24h",
		"-querier.query-ingesters-within":              "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":                "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-querier.max-outstanding-requests-per-tenant": strconv.Itoa(numQueries), // To avoid getting errors.
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	ingester1 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-a",
	}, "")
	ingester2 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-a",
	}, "")
	ingester3 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-b",
	}, "")
	ingester4 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-b",
	}, "")
	ingester5 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-c",
	}, "")
	ingester6 := e2ecortex.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), "", map[string]string{
		"-ingester.availability-zone": "zone-c",
	}, "")
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3, ingester3, ingester4, ingester5, ingester6))

	distributor := e2ecortex.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), "", flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor))

	// Wait until the distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push 100 series
	/*
		now := time.Now()

		c, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
		require.NoError(t, err)

		series, expectedVectors := generateSeries("series_1", now)
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	*/

	// Query back 100 series > all good
	// SIGKILL 1 ingester in 1 zone
	// Query back 100 series > all good
	// SIGKILL 1 more ingester in the same zone
	// Query back 100 series > all good
	// SIGKILL 1 more ingester in a different zone
	// Query back 100 series > fail
}
