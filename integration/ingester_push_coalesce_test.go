//go:build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

// TestIngesterPushCoalescing sends many concurrent small write requests for
// one tenant to a single ingester with push coalescing enabled, and checks
// that every request succeeds, that the ingester really merged commits, and
// that every sample can be queried back with the value it was written with.
func TestIngesterPushCoalescing(t *testing.T) {
	for _, tc := range []struct {
		name string
		// pushersPerSeries is how many pushers write each series. Above 1,
		// pushers interleave their timestamps on a shared series, so a batch
		// regularly holds several requests for one series, some of them out
		// of order.
		pushersPerSeries int
	}{
		{name: "disjoint series", pushersPerSeries: 1},
		{name: "shared series", pushersPerSeries: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testIngesterPushCoalescing(t, tc.pushersPerSeries)
		})
	}
}

func testIngesterPushCoalescing(t *testing.T, pushersPerSeries int) {
	const (
		pushers          = 64
		pushesPerPusher  = 50
		seriesPerRequest = 5
		metricName       = "push_coalesce_load"

		totalRequests = pushers * pushesPerPusher
		totalSamples  = totalRequests * seriesPerRequest
	)
	seriesGroups := pushers / pushersPerSeries
	totalSeries := seriesGroups * seriesPerRequest

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), map[string]string{
		// One ingester and no replication, so every request for the tenant
		// lands on the same TSDB, which is where coalescing happens.
		"-distributor.replication-factor":        "1",
		"-ingester.push-coalesce.enabled":        "true",
		"-ingester.out-of-order-time-window":     "30m",
		"-ingester.max-series-per-user":          "0",
		"-ingester.max-series-per-metric":        "0",
		"-ingester.max-global-series-per-user":   "0",
		"-ingester.max-global-series-per-metric": "0",
		// Nothing is shipped during the test, so query only the ingester.
		"-limits.query-store-after": (1 * time.Hour).String(),
	})

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul.NetworkHTTPEndpoint(), flags, "")
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway, querier))

	// Wait until the distributor and the querier see the ingester as ACTIVE.
	ingesterActive := e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, ingesterActive))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, ingesterActive))

	// Pushers sharing a series take turns on its timestamps, so every
	// (series, timestamp) pair is written exactly once. They run
	// concurrently, so samples of a shared series arrive out of order, but
	// the span of one series, pushesPerPusher*pushersPerSeries seconds, stays
	// well inside the out-of-order window: every sample must be accepted.
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Second)

	var (
		wg        sync.WaitGroup
		failed    atomic.Int64
		firstFail atomic.String
	)
	fail := func(msg string) {
		failed.Inc()
		firstFail.CompareAndSwap("", msg)
	}
	for p := range pushers {
		group, slot := p/pushersPerSeries, p%pushersPerSeries
		wg.Go(func() {
			client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
			if err != nil {
				fail(err.Error())
				return
			}
			for n := range pushesPerPusher {
				ts := start.Add(time.Duration(n*pushersPerSeries+slot) * time.Second)
				series := make([]prompb.TimeSeries, 0, seriesPerRequest)
				for i := range seriesPerRequest {
					series = append(series, prompb.TimeSeries{
						Labels: []prompb.Label{
							{Name: labels.MetricName, Value: metricName},
							{Name: "group", Value: strconv.Itoa(group)},
							{Name: "series", Value: strconv.Itoa(i)},
						},
						Samples: []prompb.Sample{{Value: float64(n), Timestamp: e2e.TimeToMilliseconds(ts)}},
					})
				}
				res, err := client.Push(series)
				switch {
				case err != nil:
					fail(err.Error())
				case res.StatusCode != 200:
					fail(fmt.Sprintf("pusher %d push %d: status %d", p, n, res.StatusCode))
				}
			}
		})
	}
	wg.Wait()
	require.Zerof(t, failed.Load(), "%d of %d pushes failed, first: %s", failed.Load(), totalRequests, firstFail.Load())

	// Every sample reached the TSDB.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(totalSamples), "cortex_ingester_ingested_samples_total"))

	// The coalescer saw every request, served some of them as followers of
	// another request's commit, and so issued fewer commits than requests.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(totalRequests), "cortex_ingester_push_coalesce_requests_total"))
	require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_ingester_push_coalesce_requests_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "role", "follower"))))
	commits, err := ingester.SumMetrics([]string{"cortex_ingester_push_coalesce_commits_total"})
	require.NoError(t, err)
	require.Less(t, commits[0], float64(totalRequests), "coalescing must issue fewer commits than requests")
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_push_coalesce_commit_failures_total"))
	t.Logf("%d requests committed in %.0f commits", totalRequests, commits[0])

	// Every series and every sample can be read back with its own value.
	client, err := e2ecortex.NewClient("", querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	queryAt := start.Add(time.Duration(pushesPerPusher*pushersPerSeries) * time.Second)

	requireScalar := func(query string, want float64) {
		t.Helper()
		result, err := client.Query(query, queryAt)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type(), query)
		vector := result.(model.Vector)
		require.Len(t, vector, 1, query)
		require.Equal(t, want, float64(vector[0].Value), query)
	}
	requireScalar(fmt.Sprintf("count(%s)", metricName), float64(totalSeries))
	requireScalar(fmt.Sprintf("sum(count_over_time(%s[1h]))", metricName), totalSamples)
	// Every pusher writes the values 0 to pushesPerPusher-1 to each of its
	// seriesPerRequest series.
	requireScalar(fmt.Sprintf("sum(sum_over_time(%s[1h]))", metricName),
		float64(pushers*seriesPerRequest*(pushesPerPusher-1)*pushesPerPusher/2))

	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
}
