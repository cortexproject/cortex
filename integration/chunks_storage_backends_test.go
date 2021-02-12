// +build requires_docker

package integration

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/chunk/openstack"
	"github.com/cortexproject/cortex/pkg/chunk/storage"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ruler"
	"github.com/cortexproject/cortex/pkg/ruler/rules"
	cortex_swift "github.com/cortexproject/cortex/pkg/storage/bucket/swift"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

var (
	userID = "e2e-user"
)

func TestChunksStorageAllIndexBackends(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	dynamo := e2edb.NewDynamoDB()
	bigtable := e2edb.NewBigtable()
	cassandra := e2edb.NewCassandra()

	stores := []string{"aws-dynamo", "bigtable", "cassandra"}
	perStoreDuration := 14 * 24 * time.Hour

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(cassandra, dynamo, bigtable, consul))

	// lets build config for each type of Index Store.
	now := time.Now()
	oldestStoreStartTime := now.Add(time.Duration(-len(stores)) * perStoreDuration)

	storeConfigs := make([]storeConfig, len(stores))
	for i, store := range stores {
		storeConfigs[i] = storeConfig{From: oldestStoreStartTime.Add(time.Duration(i) * perStoreDuration).Format("2006-01-02"), IndexStore: store}
	}

	storageFlags := mergeFlags(ChunksStorageFlags(), map[string]string{
		"-cassandra.addresses":          cassandra.NetworkHTTPEndpoint(),
		"-cassandra.keyspace":           "tests", // keyspace gets created on startup if it does not exist
		"-cassandra.replication-factor": "1",
	})

	// bigtable client needs to set an environment variable when connecting to an emulator
	bigtableFlag := map[string]string{"BIGTABLE_EMULATOR_HOST": bigtable.NetworkHTTPEndpoint()}

	// here we are starting and stopping table manager for each index store
	// this is a workaround to make table manager create tables for each config since it considers only latest schema config while creating tables
	for i := range storeConfigs {
		require.NoError(t, writeFileToSharedDir(s, cortexSchemaConfigFile, []byte(buildSchemaConfigWith(storeConfigs[i:i+1]))))

		tableManager := e2ecortex.NewTableManager("table-manager", mergeFlags(storageFlags, map[string]string{
			"-table-manager.retention-period": "2520h", // setting retention high enough
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

	ingester := e2ecortex.NewIngester("ingester", consul.NetworkHTTPEndpoint(), mergeFlags(storageFlags, map[string]string{
		"-ingester.retain-period": "0s", // we want to make ingester not retain any chunks in memory after they are flushed so that queries get data only from the store
	}), "")
	ingester.HTTPService.SetEnvVars(bigtableFlag)

	distributor := e2ecortex.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), storageFlags, "")
	querier := e2ecortex.NewQuerier("querier", consul.NetworkHTTPEndpoint(), storageFlags, "")
	querier.HTTPService.SetEnvVars(bigtableFlag)

	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	client, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push and Query some series from Cortex for each day starting from oldest start time from configs until now so that we test all the Index Stores
	for ts := oldestStoreStartTime; ts.Before(now); ts = ts.Add(24 * time.Hour) {
		series, expectedVector := generateSeries("series_1", ts)

		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		// lets make ingester flush the chunks immediately to the store
		res, err = e2e.GetRequest("http://" + ingester.HTTPEndpoint() + "/flush")
		require.NoError(t, err)
		require.Equal(t, 204, res.StatusCode)

		// Let's wait until all chunks are flushed.
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_flush_queue_length"))
		require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_flush_series_in_progress"))

		// lets verify that chunk store chunk metrics are updated.
		require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(0), "cortex_chunk_store_stored_chunks_total"))
		require.NoError(t, ingester.WaitSumMetrics(e2e.Greater(0), "cortex_chunk_store_stored_chunk_bytes_total"))

		// Query back the series.
		result, err := client.Query("series_1", ts)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))

		// check we've queried them from the chunk store.
		require.NoError(t, querier.WaitSumMetrics(e2e.Greater(0), "cortex_chunk_store_fetched_chunks_total"))
		require.NoError(t, querier.WaitSumMetrics(e2e.Greater(0), "cortex_chunk_store_fetched_chunk_bytes_total"))
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
}

func TestSwiftChunkStorage(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()
	swift := e2edb.NewSwiftStorage()

	require.NoError(t, s.StartAndWaitReady(swift))

	var (
		cfg          storage.Config
		storeConfig  chunk.StoreConfig
		schemaConfig chunk.SchemaConfig
		defaults     validation.Limits
	)
	flagext.DefaultValues(&cfg, &storeConfig, &schemaConfig, &defaults)

	cfg.Swift = swiftConfig(swift)
	schemaConfig.Configs = []chunk.PeriodConfig{
		{
			From:       chunk.DayTime{Time: model.Time(0)},
			IndexType:  "inmemory",
			ObjectType: "swift",
			Schema:     "v10",
			RowShards:  16,
		},
	}

	// inject a memory store so we can create table without a table manager.
	inmemory := chunk.NewMockStorage()
	err = inmemory.CreateTable(context.Background(), chunk.TableDesc{})
	require.NoError(t, err)

	storage.RegisterIndexStore("inmemory", func() (chunk.IndexClient, error) {
		return inmemory, nil
	}, func() (chunk.TableClient, error) {
		return inmemory, nil
	})

	limits, err := validation.NewOverrides(defaults, nil)
	require.NoError(t, err)

	store, err := storage.NewStore(cfg, storeConfig, schemaConfig, limits, nil, nil, log.NewNopLogger())
	require.NoError(t, err)

	defer store.Stop()

	ctx := user.InjectUserID(context.Background(), userID)

	lbls := labels.Labels{
		{Name: labels.MetricName, Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "buzz", Value: "fuzz"},
	}

	c1 := newChunk(model.Time(1), lbls, 10)
	c2 := newChunk(model.Time(2), lbls, 10)
	// Add two chunks.
	err = store.PutOne(ctx, c1.From, c1.Through, c1)
	require.NoError(t, err)
	err = store.PutOne(ctx, c2.From, c2.Through, c2)
	require.NoError(t, err)

	ctx = user.InjectOrgID(ctx, userID)

	// Get the first chunk.
	chunks, err := store.Get(ctx, userID, model.Time(1), model.Time(1), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "foo"))
	require.NoError(t, err)
	require.Equal(t, 1, len(chunks))

	// Get both chunk and verify their content.
	chunks, err = store.Get(ctx, userID, model.Time(1), model.Time(2), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "foo"))
	require.NoError(t, err)
	require.Equal(t, 2, len(chunks))

	sort.Slice(chunks, func(i, j int) bool { return chunks[i].From < chunks[j].From })
	require.Equal(t, c1.Checksum, chunks[0].Checksum)
	require.Equal(t, c2.Checksum, chunks[1].Checksum)

	// Delete the first chunk
	err = store.DeleteChunk(ctx, c1.From, c1.Through, userID, c1.ExternalKey(), lbls, nil)
	require.NoError(t, err)

	// Verify we get now only the second chunk.
	chunks, err = store.Get(ctx, userID, model.Time(1), model.Time(2), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "foo"))
	require.NoError(t, err)
	require.Equal(t, 1, len(chunks))
	require.Equal(t, c2.Metric, chunks[0].Metric)

}

func TestSwiftRuleStorage(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()
	swift := e2edb.NewSwiftStorage()

	require.NoError(t, s.StartAndWaitReady(swift))

	store, err := ruler.NewRuleStorage(ruler.RuleStoreConfig{
		Type:  "swift",
		Swift: swiftConfig(swift),
	}, nil, log.NewNopLogger())
	require.NoError(t, err)
	ctx := context.Background()

	// Add 2 rule groups.
	r1 := newRuleGroup(userID, "foo", "1")
	err = store.SetRuleGroup(ctx, userID, "foo", r1)
	require.NoError(t, err)

	r2 := newRuleGroup(userID, "bar", "2")
	err = store.SetRuleGroup(ctx, userID, "bar", r2)
	require.NoError(t, err)

	// Get rules back.
	rls, err := store.ListAllRuleGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(rls[userID]))
	require.NoError(t, store.LoadRuleGroups(ctx, rls))

	userRules := rls[userID]
	sort.Slice(userRules, func(i, j int) bool { return userRules[i].Name < userRules[j].Name })
	require.Equal(t, r1, userRules[0])
	require.Equal(t, r2, userRules[1])

	// Delete the first rule group
	err = store.DeleteRuleGroup(ctx, userID, "foo", r1.Name)
	require.NoError(t, err)

	//Verify we only have the second rule group
	rls, err = store.ListAllRuleGroups(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, len(rls[userID]))
	require.NoError(t, store.LoadRuleGroups(ctx, rls))
	require.Equal(t, r2, rls[userID][0])
}

func newRuleGroup(userID, namespace, group string) *rules.RuleGroupDesc {
	return &rules.RuleGroupDesc{
		Name:      group,
		Interval:  time.Minute,
		Namespace: namespace,
		Rules: []*rules.RuleDesc{
			{
				Expr:   fmt.Sprintf(`{%s="bar"}`, group),
				Record: group + ":bar",
			},
		},
		User: userID,
	}
}

func swiftConfig(s *e2e.HTTPService) openstack.SwiftConfig {
	return openstack.SwiftConfig{
		Config: cortex_swift.Config{
			AuthURL:       "http://" + s.HTTPEndpoint() + "/auth/v1.0",
			Password:      "testing",
			ContainerName: "e2e",
			Username:      "test:tester",
		},
	}
}

func newChunk(from model.Time, metric labels.Labels, samples int) chunk.Chunk {
	c, _ := encoding.NewForEncoding(encoding.Varbit)
	chunkStart := from
	ts := from
	for i := 0; i < samples; i++ {
		ts = chunkStart.Add(time.Duration(i) * 15 * time.Second)
		nc, err := c.Add(model.SamplePair{Timestamp: ts, Value: model.SampleValue(i)})
		if err != nil {
			panic(err)
		}
		if nc != nil {
			panic("returned chunk was not nil")
		}
	}

	chunk := chunk.NewChunk(
		userID,
		client.Fingerprint(metric),
		metric,
		c,
		chunkStart,
		ts,
	)
	// Force checksum calculation.
	err := chunk.Encode()
	if err != nil {
		panic(err)
	}
	return chunk
}
