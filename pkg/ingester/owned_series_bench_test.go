package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
)

// The benchmark ring is deliberately large: 1,000 ingesters with
// 512 tokens each over 3 zones, with replication factor 3.
const (
	benchmarkRingIngesters = 1000
	benchmarkRingZones     = 3
	benchmarkRingTokens    = 512
)

// benchmarkRingPeers returns the ring of every benchmark ingester except the one
// under test, which joins it by itself. Tokens are unique across the ring and
// deterministic.
func benchmarkRingPeers() *ring.Desc {
	r := rand.New(rand.NewSource(1))
	used := map[uint32]struct{}{}
	desc := ring.NewDesc()
	for n := 1; n < benchmarkRingIngesters; n++ {
		tokens := make([]uint32, 0, benchmarkRingTokens)
		for len(tokens) < benchmarkRingTokens {
			token := r.Uint32()
			if _, ok := used[token]; ok {
				continue
			}
			used[token] = struct{}{}
			tokens = append(tokens, token)
		}
		slices.Sort(tokens)
		id := fmt.Sprintf("peer-%d", n)
		desc.Ingesters[id] = ring.InstanceDesc{
			Addr:      id,
			Zone:      fmt.Sprintf("zone-%d", n%benchmarkRingZones),
			State:     ring.ACTIVE,
			Tokens:    tokens,
			Timestamp: time.Now().Unix(),
		}
	}
	return desc
}

// prepareOwnedSeriesBenchmarkIngester starts an ingester in a large
// ring holding seriesPerTenant series for each of numTenants tenants, sharded
// with shardSize, or over the whole ring if it is zero. Every series has been
// judged against the ring, as in steady state.
//
// Recounts only run when the benchmark calls them. The lifecycler heartbeats
// every second, as each series must be judged by a ring read taken after it
// arrived; the heartbeat runs on its own goroutine and does not change the ring.
func prepareOwnedSeriesBenchmarkIngester(b *testing.B, numTenants, shardSize, seriesPerTenant int) *Ingester {
	cfg := defaultIngesterTestConfig(b)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.LifecyclerConfig.NumTokens = benchmarkRingTokens
	cfg.LifecyclerConfig.Zone = "zone-0"
	cfg.LifecyclerConfig.HeartbeatPeriod = time.Second
	cfg.LifecyclerConfig.RingConfig.ReplicationFactor = 3
	cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled = true
	cfg.LifecyclerConfig.RingConfig.HeartbeatTimeout = time.Hour
	cfg.DistributorShardingStrategy = util.ShardingStrategyShuffle
	cfg.DistributorShardByAllLabels = true
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.ownedSeriesCheckPeriod = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true

	peers := benchmarkRingPeers()
	require.NoError(b, cfg.LifecyclerConfig.RingConfig.KVStore.Mock.CAS(context.Background(), RingKey, func(any) (any, bool, error) {
		return peers, true, nil
	}))

	limits := defaultLimitsTestConfig()
	limits.IngestionTenantShardSize = shardSize
	limits.MaxLocalSeriesPerUser = 0
	limits.MaxGlobalSeriesPerUser = 0

	i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", prometheus.NewRegistry())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	b.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(b, 10*time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	nowMs := time.Now().UnixMilli()
	for t := range numTenants {
		userID := fmt.Sprintf("tenant-%d", t)
		ctx := user.InjectOrgID(context.Background(), userID)
		const batch = 1000
		for start := 0; start < seriesPerTenant; start += batch {
			n := min(batch, seriesPerTenant-start)
			series := make([]labels.Labels, n)
			samples := make([]cortexpb.Sample, n)
			for s := range n {
				series[s] = labels.FromStrings(labels.MetricName, "metric", "series", fmt.Sprintf("%08x", uint32(start+s)*2654435761))
				samples[s] = cortexpb.Sample{TimestampMs: nowMs, Value: 1}
			}
			_, err := i.Push(ctx, cortexpb.ToWriteRequest(series, samples, nil, nil, cortexpb.API))
			require.NoError(b, err)
		}
	}

	// Wait for a ring read taken after every push, then judge every series.
	after := i.lifecycler.RingReads()
	test.Poll(b, 10*time.Second, true, func() any {
		_, read := i.lifecycler.RingDesc()
		return read > after
	})
	i.updateOwnedSeries()
	return i
}

// BenchmarkIngester_OwnedSeriesRecount measures the owned-series work an ingester
// does in a large ring, for tenants sharded over a subring of 100 instances
// and for tenants routed over the whole ring:
//
//   - idle: the check made every second when nothing changed.
//   - state_change: a ring change which only changes an instance's state, as
//     every restart in a rolling deploy does, for an instance outside every
//     tenant's shard. Whole-ring ownership is computed again.
//   - topology_change: a ring change which changes an instance's tokens, which
//     rebuilds every subring and computes every tenant's ownership again.
//   - full_recount: the recount of every tracker made every active series update
//     period, with nothing changed.
//
// The cost of judging each series again when its ownership does change is
// measured by BenchmarkActiveSeries_UpdateMetrics.
func BenchmarkIngester_OwnedSeriesRecount(b *testing.B) {
	const seriesPerTenant = 1000

	for _, c := range []struct {
		name       string
		numTenants int
		shardSize  int
	}{
		{name: "whole_ring/tenants=3", numTenants: 3},
		{name: "sharded/shard_size=100/tenants=1", numTenants: 1, shardSize: 100},
		{name: "sharded/shard_size=100/tenants=10", numTenants: 10, shardSize: 100},
		{name: "sharded/shard_size=100/tenants=50", numTenants: 50, shardSize: 100},
	} {
		i := prepareOwnedSeriesBenchmarkIngester(b, c.numTenants, c.shardSize, seriesPerTenant)
		desc, _ := i.lifecycler.RingDesc()

		// A peer outside every tenant's shard, so that only whole-ring ownership
		// depends on its state.
		var outside string
		for n := 1; n < benchmarkRingIngesters && outside == ""; n++ {
			id := fmt.Sprintf("peer-%d", n)
			inAnyShard := false
			for t := range c.numTenants {
				if c.shardSize > 0 {
					if _, err := i.ownedSeries.ring.ShuffleShard(fmt.Sprintf("tenant-%d", t), c.shardSize).GetInstanceState(id); err == nil {
						inAnyShard = true
						break
					}
				}
			}
			if !inAnyShard {
				outside = id
			}
		}
		require.NotEmpty(b, outside)

		changed := func(change func(*ring.InstanceDesc)) *ring.Desc {
			d := ring.NewDesc()
			for id, instance := range desc.Ingesters {
				if id == outside {
					instance.Tokens = slices.Clone(instance.Tokens)
					change(&instance)
				}
				d.Ingesters[id] = instance
			}
			return d
		}
		stateChanged := changed(func(instance *ring.InstanceDesc) { instance.State = ring.LEAVING })
		topologyChanged := changed(func(instance *ring.InstanceDesc) { instance.Tokens[0]++ })

		b.Run(c.name+"/idle", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				i.recountOwnedSeriesIfChanged()
			}
		})

		for name, next := range map[string]*ring.Desc{"state_change": stateChanged, "topology_change": topologyChanged} {
			b.Run(c.name+"/"+name, func(b *testing.B) {
				// Alternate between the two rings, so that every iteration is a
				// change. The ingester's ring is updated directly: the lifecycler's
				// copy does not change, so the recount keeps the ring it is given.
				descs := [2]*ring.Desc{next, desc}
				b.ReportAllocs()
				n := 0
				for b.Loop() {
					i.ownedSeries.ring.UpdateDesc(descs[n%2])
					n++
					i.recountOwnedSeriesIfChanged()
				}
				i.ownedSeries.ring.UpdateDesc(desc)
				i.recountOwnedSeriesIfChanged()
			})
		}

		b.Run(c.name+"/full_recount", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				i.updateOwnedSeries()
			}
		})
	}
}

// BenchmarkIngesterPush_OwnedSeries is BenchmarkIngesterPush with owned series
// tracking disabled, enabled, and enforced, for samples of series the ingester
// already holds and for samples creating new series, which is where ownership
// tracking adds work: a token for the series and, when enforced, the owned count
// for the limit checks.
func BenchmarkIngesterPush_OwnedSeries(b *testing.B) {
	for _, mode := range []struct {
		name               string
		metrics, enforcing bool
	}{
		{name: "owned_series_disabled"},
		{name: "owned_series_enabled", metrics: true},
		{name: "owned_series_enforced", metrics: true, enforcing: true},
	} {
		for _, newSeries := range []bool{false, true} {
			name := mode.name + "/existing_series"
			if newSeries {
				name = mode.name + "/new_series"
			}
			b.Run(name, func(b *testing.B) {
				cfg := defaultIngesterTestConfig(b)
				cfg.LifecyclerConfig.JoinAfter = 0
				cfg.OwnedSeriesMetricsEnabled = mode.metrics
				cfg.OwnedSeriesLimitEnforcementEnabled = mode.enforcing

				// New series are created on every iteration, so the series limits
				// would be reached.
				limits := defaultLimitsTestConfig()
				limits.MaxLocalSeriesPerUser = 0
				limits.MaxLocalSeriesPerMetric = 0

				i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", prometheus.NewRegistry())
				require.NoError(b, err)
				require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
				b.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
				test.Poll(b, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

				ctx := user.InjectOrgID(context.Background(), userID)
				const numSeries = 10000
				startMs := time.Now().UnixMilli()
				series := make([]labels.Labels, numSeries)
				samples := make([]cortexpb.Sample, numSeries)
				build := func(iter int) {
					for s := range numSeries {
						id := s
						if newSeries {
							id = iter*numSeries + s
						}
						series[s] = labels.FromStrings(labels.MetricName, "metric", "series", fmt.Sprintf("%08x", uint32(id)*2654435761))
						samples[s] = cortexpb.Sample{TimestampMs: startMs + int64(iter), Value: 1}
					}
				}
				build(0)
				_, err = i.Push(ctx, cortexpb.ToWriteRequest(series, samples, nil, nil, cortexpb.API))
				require.NoError(b, err)

				b.ReportAllocs()
				for iter := 1; b.Loop(); iter++ {
					b.StopTimer()
					build(iter)
					req := cortexpb.ToWriteRequest(series, samples, nil, nil, cortexpb.API)
					b.StartTimer()
					_, err := i.Push(ctx, req)
					require.NoError(b, err)
				}
			})
		}
	}
}
