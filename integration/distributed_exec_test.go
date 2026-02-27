//go:build integration_query_fuzz
// +build integration_query_fuzz

package integration

import (
	"context"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cortexproject/promqlsmith"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/integration/e2e"
	e2edb "github.com/cortexproject/cortex/integration/e2e/db"
	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/storage/tsdb"
)

func TestDistributedExecutionFuzz(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// start dependencies.
	consul1 := e2edb.NewConsulWithName("consul1")
	consul2 := e2edb.NewConsulWithName("consul2")
	require.NoError(t, s.StartAndWaitReady(consul1, consul2))

	flags := mergeFlags(
		AlertmanagerLocalFlags(),
		map[string]string{
			"-store.engine":                                    blocksStorageEngine,
			"-blocks-storage.backend":                          "filesystem",
			"-blocks-storage.tsdb.head-compaction-interval":    "4m",
			"-blocks-storage.tsdb.block-ranges-period":         "2h",
			"-blocks-storage.tsdb.ship-interval":               "1h",
			"-blocks-storage.bucket-store.sync-interval":       "15m",
			"-blocks-storage.tsdb.retention-period":            "2h",
			"-blocks-storage.bucket-store.index-cache.backend": tsdb.IndexCacheBackendInMemory,
			"-querier.query-store-for-labels-enabled":          "true",
			// Ingester.
			"-ring.store":      "consul",
			"-consul.hostname": consul1.NetworkHTTPEndpoint(),
			// Distributor.
			"-distributor.replication-factor": "1",
			// Store-gateway.
			"-store-gateway.sharding-enabled": "false",
			// alert manager
			"-alertmanager.web.external-url": "http://localhost/alertmanager",
		},
	)
	// make alert manager config dir
	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs", []byte{}))

	path1 := path.Join(s.SharedDir(), "cortex-1")
	path2 := path.Join(s.SharedDir(), "cortex-2")

	flags1 := mergeFlags(flags, map[string]string{"-blocks-storage.filesystem.dir": path1})

	// Start first Cortex replicas
	distributor := e2ecortex.NewDistributor("distributor", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), flags1, "")
	ingester := e2ecortex.NewIngester("ingester", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), flags1, "")
	queryScheduler := e2ecortex.NewQueryScheduler("query-scheduler", flags1, "")
	storeGateway := e2ecortex.NewStoreGateway("store-gateway", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), flags1, "")
	require.NoError(t, s.StartAndWaitReady(queryScheduler, distributor, ingester, storeGateway))
	flags1 = mergeFlags(flags1, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway.NetworkGRPCEndpoint()}, ","),
	})
	queryFrontend := e2ecortex.NewQueryFrontend("query-frontend", mergeFlags(flags1, map[string]string{
		"-frontend.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.Start(queryFrontend))
	querier := e2ecortex.NewQuerier("querier", e2ecortex.RingStoreConsul, consul1.NetworkHTTPEndpoint(), mergeFlags(flags1, map[string]string{
		"-querier.scheduler-address": queryScheduler.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier))
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	c1, err := e2ecortex.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Enable distributed execution for the second Cortex instance.
	flags2 := mergeFlags(flags, map[string]string{
		"-frontend.query-vertical-shard-size": "2",
		"-blocks-storage.filesystem.dir":      path2,
		"-consul.hostname":                    consul2.NetworkHTTPEndpoint(),
		"-querier.thanos-engine":              "true",
		"-querier.distributed-exec-enabled":   "true",
		"-api.querier-default-codec":          "protobuf",
	})

	distributor2 := e2ecortex.NewDistributor("distributor2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), flags2, "")
	ingester2 := e2ecortex.NewIngester("ingester2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), flags2, "")
	queryScheduler2 := e2ecortex.NewQueryScheduler("query-scheduler2", flags2, "")
	storeGateway2 := e2ecortex.NewStoreGateway("store-gateway2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), flags2, "")
	require.NoError(t, s.StartAndWaitReady(queryScheduler2, distributor2, ingester2, storeGateway2))
	flags2 = mergeFlags(flags1, map[string]string{
		"-querier.store-gateway-addresses": strings.Join([]string{storeGateway2.NetworkGRPCEndpoint()}, ","),
	})
	queryFrontend2 := e2ecortex.NewQueryFrontend("query-frontend2", mergeFlags(flags2, map[string]string{
		"-frontend.scheduler-address": queryScheduler2.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.Start(queryFrontend2))
	querier2 := e2ecortex.NewQuerier("querier2", e2ecortex.RingStoreConsul, consul2.NetworkHTTPEndpoint(), mergeFlags(flags2, map[string]string{
		"-querier.scheduler-address": queryScheduler2.NetworkGRPCEndpoint(),
	}), "")
	require.NoError(t, s.StartAndWaitReady(querier2))
	require.NoError(t, distributor2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	c2, err := e2ecortex.NewClient(distributor2.HTTPEndpoint(), queryFrontend2.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	// Push some series to Cortex.
	start := now.Add(-time.Minute * 10)
	end := now.Add(-time.Minute * 1)
	numSeries := 3
	numSamples := 20
	lbls := make([]labels.Labels, numSeries*2)
	serieses := make([]prompb.TimeSeries, numSeries*2)
	scrapeInterval := 30 * time.Second
	for i := 0; i < numSeries; i++ {
		series := e2e.GenerateSeriesWithSamples("test_series_a", start, scrapeInterval, i*numSamples, numSamples, prompb.Label{Name: "job", Value: "test"}, prompb.Label{Name: "series", Value: strconv.Itoa(i)})
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}

	// Generate another set of series for testing binary expression and vector matching.
	for i := numSeries; i < 2*numSeries; i++ {
		prompbLabels := []prompb.Label{{Name: "job", Value: "test"}, {Name: "series", Value: strconv.Itoa(i)}}
		switch i % 3 {
		case 0:
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "200"})
		case 1:
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "400"})
		default:
			prompbLabels = append(prompbLabels, prompb.Label{Name: "status_code", Value: "500"})
		}
		series := e2e.GenerateSeriesWithSamples("test_series_b", start, scrapeInterval, i*numSamples, numSamples, prompbLabels...)
		serieses[i] = series
		builder := labels.NewBuilder(labels.EmptyLabels())
		for _, lbl := range series.Labels {
			builder.Set(lbl.Name, lbl.Value)
		}
		lbls[i] = builder.Labels()
	}
	res, err := c1.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c2.Push(serieses)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	waitUntilReady(t, context.Background(), c1, c2, `{job="test"}`, start, end)

	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(enabledFunctions),
		promqlsmith.WithEnabledAggrs(enabledAggrs),
	}
	ps := promqlsmith.New(rnd, lbls, opts...)

	runQueryFuzzTestCases(t, ps, c1, c2, end, start, end, scrapeInterval, 1000, false)
}
