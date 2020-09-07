// +build requires_docker

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
)

func TestDeleteSeriesAllIndexBackends(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	bigtable := e2edb.NewBigtable()
	cassandra := e2edb.NewCassandra()

	stores := []string{"aws-dynamo", "bigtable", "cassandra"}
	perStoreDuration := 7 * 24 * time.Hour

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(cassandra, dynamo, bigtable, consul))

	// lets build config for each type of Index Store.
	now := time.Now()
	oldestStoreStartTime := now.Add(time.Duration(-len(stores)) * perStoreDuration)

	storeConfigs := make([]storeConfig, len(stores))
	for i, store := range stores {
		storeConfigs[i] = storeConfig{From: oldestStoreStartTime.Add(time.Duration(i) * perStoreDuration).UTC().Format("2006-01-02"), IndexStore: store}
	}

	flags := mergeFlags(ChunksStorageFlags, map[string]string{
		"-cassandra.addresses":          cassandra.NetworkHTTPEndpoint(),
		"-cassandra.keyspace":           "tests", // keyspace gets created on startup if it does not exist
		"-cassandra.replication-factor": "1",
		"-purger.enable":                "true",
		"-deletes.store":                "bigtable",
	})

	// bigtable client needs to set an environment variable when connecting to an emulator.
	bigtableFlag := map[string]string{"BIGTABLE_EMULATOR_HOST": bigtable.NetworkHTTPEndpoint()}

	// here we are starting and stopping table manager for each index store
	// this is a workaround to make table manager create tables for each config since it considers only latest schema config while creating tables
	for i := range storeConfigs {
		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(buildSchemaConfigWith(storeConfigs[i:i+1]))))

		tableManager := e2ecortex.NewTableManager("table-manager", mergeFlags(flags, map[string]string{
			"-table-manager.retention-period": "2520h", // setting retention high enough
			"-log.level":                      "warn",
		}), "")
		tableManager.HTTPService.SetEnvVars(bigtableFlag)
		require.NoError(t, s.StartAndWaitReady(tableManager))

		// Wait until the first table-manager sync has completed, so that we're
		// sure the tables have been created.
		require.NoError(t, tableManager.WaitSumMetrics(e2e.Greater(0), "cortex_table_manager_sync_success_timestamp_seconds"))
		require.NoError(t, s.Stop(tableManager))
	}

	// Start rest of the Cortex components.
	require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(buildSchemaConfigWith(storeConfigs))))

	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-ingester.retain-period": "0s", // we want to make ingester not retain any chunks in memory after they are flushed so that queries get data only from the store
		"-log.level":              "warn",
	}), "")
	ingester.HTTPService.SetEnvVars(bigtableFlag)

	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")
	querier.HTTPService.SetEnvVars(bigtableFlag)

	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	seriesToPush := []struct {
		name          string
		lables        []prompb.Label
		pushedVectors []model.Vector
	}{
		{
			name:   "series_1",
			lables: []prompb.Label{{Name: "common", Value: "label"}, {Name: "distinct", Value: "label1"}},
		},
		{
			name:   "series_2",
			lables: []prompb.Label{{Name: "common", Value: "label"}, {Name: "distinct", Value: "label2"}},
		},
		{
			name:   "delete_series",
			lables: []prompb.Label{{Name: "common", Value: "label"}, {Name: "distinct", Value: "label3"}},
		},
	}

	// Push some series for each day starting from oldest start time from configs until now so that we test all the Index Stores.
	for ts := oldestStoreStartTime; ts.Before(now); ts = ts.Add(24 * time.Hour) {
		for i, s := range seriesToPush {
			series, expectedVector := generateSeries(s.name, ts, s.lables...)

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			seriesToPush[i].pushedVectors = append(seriesToPush[i].pushedVectors, expectedVector)
		}

		// lets make ingester flush the chunks immediately to the store.
		res, err := e2e.GetRequest("http://" + ingester.HTTPEndpoint() + "/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)

		// Let's wait until all chunks are flushed.
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_flush_queue_length"))
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_flush_series_in_progress"))
	}

	// call flush again because chunks are sometimes still retained in memory due to chunks flush operation being async while chunk cleanup from memory is not.
	res, err := e2e.GetRequest("http://" + ingester.HTTPEndpoint() + "/flush")
	require.NoError(t, err)
	require.Equal(t, 204, res.StatusCode)

	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_chunks"))

	// start a purger
	purger := e2ecortex.NewPurger("purger", flags, "")
	purger.HTTPService.SetEnvVars(bigtableFlag)

	require.NoError(t, s.StartAndWaitReady(purger))

	// perform deletion for 2 days interval for each week covering all the Index Stores.
	deletionDuration := 2 * 24 * time.Hour
	deletedIntervals := intervals{}
	endpoint := "http://" + purger.HTTPEndpoint() + "/prometheus/api/v1/admin/tsdb/delete_series?match[]=delete_series{common=\"label\"}&start=%f&end=%f"
	for ts := now; ts.After(oldestStoreStartTime); ts = ts.Add(-perStoreDuration) {
		// expand the interval by a second on both the ends since the requests are getting aligned with pushed samples at ms precision which
		// sometimes does not match exactly in purger during parsing time from a string.
		deletedInterval := interval{e2e.TimeToMilliseconds(ts.Add(-(deletionDuration + time.Second))), e2e.TimeToMilliseconds(ts.Add(time.Second))}
		deletedIntervals = append(deletedIntervals, deletedInterval)

		res, err := client.PostRequest(fmt.Sprintf(endpoint, float64(deletedInterval.start)/float64(1000), float64(deletedInterval.end)/float64(1000)), nil)
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)
	}

	// check whether purger has received expected number of delete requests.
	require.NoError(t, purger.WaitSumMetrics(e2e.Equals(float64(len(deletedIntervals))), "cortex_purger_delete_requests_received_total"))

	// stop the purger and recreate it since we load requests for deletion every hour.
	require.NoError(t, s.Stop(purger))

	purger = e2ecortex.NewPurger("purger", mergeFlags(flags, map[string]string{
		"-purger.delete-request-cancel-period": "-1m", // we retain delete requests for a day(default)+1m, changing it to a negative value so that delete requests get picked up immediately.
	}), "")
	purger.HTTPService.SetEnvVars(bigtableFlag)
	require.NoError(t, s.StartAndWaitReady(purger))

	require.NoError(t, purger.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_purger_load_pending_requests_attempts_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "status", "success")),
		e2e.WaitMissingMetrics))

	require.NoError(t, purger.WaitSumMetricsWithOptions(e2e.Equals(float64(len(deletedIntervals))), []string{"cortex_purger_delete_requests_processed_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	// query and verify that only delete series for delete interval are gone.
	for _, s := range seriesToPush {
		for _, expectedVector := range s.pushedVectors {
			result, err := client.Query(s.name, expectedVector[0].Timestamp.Time())
			require.NoError(t, err)

			require.Equal(t, model.ValVector, result.Type())
			if s.name == "delete_series" && deletedIntervals.includes(e2e.TimeToMilliseconds(expectedVector[0].Timestamp.Time())) {
				require.Len(t, result.(model.Vector), 0)
			} else {
				assert.Equal(t, expectedVector, result.(model.Vector))
			}
		}
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, Purger, purger)
}

type interval struct {
	start, end int64
}

type intervals []interval

func (i intervals) includes(ts int64) bool {
	for _, interval := range i {
		if ts < interval.start || ts > interval.end {
			continue
		}
		return true
	}
	return false
}
