package ingester

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	ownedSeriesTestPeers  = 29
	ownedSeriesTestTokens = 128
	ownedSeriesTestSeries = 2000
)

// mutableTenantLimits lets a test change a tenant's limits at runtime, as a
// runtime config reload does.
type mutableTenantLimits struct {
	mu     sync.Mutex
	limits map[string]*validation.Limits
}

func (m *mutableTenantLimits) ByUserID(userID string) *validation.Limits {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limits[userID]
}

func (m *mutableTenantLimits) AllByUserID() map[string]*validation.Limits {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.limits
}

func (m *mutableTenantLimits) setShardSize(defaults validation.Limits, userID string, shardSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := defaults
	l.IngestionTenantShardSize = shardSize
	m.limits[userID] = &l
}

// ownedSeriesTestIngester is an ingester with owned-series tracking, in a ring
// with ownedSeriesTestPeers other instances. The other instances exist only in the
// ring: ownership does not depend on whether an instance is running, only on the
// ring, so the test can compute every instance's view of where series route.
type ownedSeriesTestIngester struct {
	*Ingester
	cfg          Config
	limits       validation.Limits
	tenantLimits *mutableTenantLimits

	// distributorRing watches the ring as a distributor does. Expected routing is
	// computed from it, independently of the ingester's own view of the ring.
	distributorRing *ring.Ring
}

func prepareOwnedSeriesTestIngester(t *testing.T, opts ...func(*Config, *validation.Limits)) *ownedSeriesTestIngester {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.LifecyclerConfig.NumTokens = ownedSeriesTestTokens
	cfg.LifecyclerConfig.RingConfig.ReplicationFactor = 3
	// The ingester's view of the ring is refreshed on each heartbeat.
	cfg.LifecyclerConfig.HeartbeatPeriod = 100 * time.Millisecond
	cfg.DistributorShardingStrategy = util.ShardingStrategyShuffle
	cfg.DistributorShardByAllLabels = true
	// Recounts run only when the test asks for them.
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Hour
	cfg.OwnedSeriesRecomputeMinInterval = time.Hour
	cfg.OwnedSeriesMetricsEnabled = true

	limits := defaultLimitsTestConfig()
	for _, opt := range opts {
		opt(&cfg, &limits)
	}
	tenantLimits := &mutableTenantLimits{limits: map[string]*validation.Limits{}}

	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, tenantLimits, "", prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), i) })
	test.Poll(t, time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })

	distributorRing, err := ring.New(cfg.LifecyclerConfig.RingConfig, "ingester", RingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), distributorRing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), distributorRing) })

	ingester := &ownedSeriesTestIngester{Ingester: i, cfg: cfg, limits: limits, tenantLimits: tenantLimits, distributorRing: distributorRing}
	for n := range ownedSeriesTestPeers {
		ingester.addPeer(t, fmt.Sprintf("peer-%d", n))
	}
	return ingester
}

// addPeer adds an ACTIVE instance to the ring, and waits until both the
// distributor's view of the ring and the ingester's include it.
func (i *ownedSeriesTestIngester) addPeer(t *testing.T, id string) {
	store := i.cfg.LifecyclerConfig.RingConfig.KVStore.Mock
	require.NoError(t, store.CAS(context.Background(), RingKey, func(in any) (any, bool, error) {
		desc := ring.GetOrCreateRingDesc(in)
		tokens := ring.NewRandomTokenGenerator().GenerateTokens(desc, id, "", ownedSeriesTestTokens, true)
		desc.AddIngester(id, id, "", tokens, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	want := i.distributorRing.InstancesCount() + 1
	test.Poll(t, 5*time.Second, want, func() any { return i.distributorRing.InstancesCount() })
	test.Poll(t, 5*time.Second, want, func() any {
		desc := i.lifecycler.RingDesc()
		if desc == nil {
			return 0
		}
		return len(desc.Ingesters)
	})
}

// pushSeries creates n new series for userID and returns their labels.
func (i *ownedSeriesTestIngester) pushSeries(t *testing.T, userID, prefix string, n int) []labels.Labels {
	ctx := user.InjectOrgID(context.Background(), userID)
	nowMs := time.Now().UnixMilli()
	all := make([]labels.Labels, 0, n)
	for s := range n {
		// Scramble the number, so that series spread over the whole token space
		// rather than hashing into clusters of neighbouring tokens.
		lbls := labels.FromStrings(labels.MetricName, "metric", "series", fmt.Sprintf("%s-%08x", prefix, uint32(s)*2654435761))
		req, _ := mockWriteRequest(t, lbls, 1, nowMs)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
		all = append(all, lbls)
	}
	return all
}

// routedHere counts the series the distributor would write to this ingester, by
// routing each one as the distributor does: within the tenant's subring when
// shuffle sharding applies, over the whole ring otherwise.
func (i *ownedSeriesTestIngester) routedHere(t *testing.T, userID string, series []labels.Labels) int {
	routingRing := ring.ReadRing(i.distributorRing)
	if shardSize := i.limiter.getShardSize(userID); shardSize > 0 {
		routingRing = routingRing.ShuffleShard(userID, shardSize)
	}

	here := 0
	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()
	for _, lbls := range series {
		token, err := ring.TokenForLabels(userID, cortexpb.FromLabelsToLabelAdapters(lbls), i.cfg.DistributorShardByAllLabels)
		require.NoError(t, err)
		set, err := routingRing.Get(token, ring.Write, bufDescs, bufHosts, bufZones)
		require.NoError(t, err)
		if set.Includes(i.lifecycler.Addr) {
			here++
		}
	}
	return here
}

// inShard reports whether this ingester is in userID's shard of the given size.
func (i *ownedSeriesTestIngester) inShard(userID string, shardSize int) bool {
	_, err := i.distributorRing.ShuffleShard(userID, shardSize).GetInstanceState(i.lifecycler.ID)
	return err == nil
}

// tenantWithShardMembership returns a tenant ID whose shard of the given size
// includes this ingester, or excludes it.
func (i *ownedSeriesTestIngester) tenantWithShardMembership(t *testing.T, shardSize int, member bool) string {
	for n := range 1000 {
		userID := fmt.Sprintf("tenant-%d", n)
		if i.inShard(userID, shardSize) == member {
			return userID
		}
	}
	t.Fatalf("no tenant found with shard membership %v", member)
	return ""
}

func (i *ownedSeriesTestIngester) owned(t *testing.T, userID string) int {
	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	return db.activeSeries.Owned()
}

// TestIngester_OwnedSeries_ShardedTenantMatchesRouting is the property the subring
// work exists for: for a shuffle-sharded tenant, the series this ingester counts
// as owned are exactly the series the distributor routes to it.
func TestIngester_OwnedSeries_ShardedTenantMatchesRouting(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	series := i.pushSeries(t, userID, "s", ownedSeriesTestSeries)

	i.updateActiveSeries(context.Background())

	expected := i.routedHere(t, userID, series)
	require.Greater(t, expected, 0)
	require.Equal(t, expected, i.owned(t, userID))

	// Counting this tenant over the whole ring, as before subring ownership, gives
	// a different answer, which is the miscount this fixes.
	i.tenantLimits.setShardSize(i.limits, userID, 0)
	require.NotEqual(t, expected, i.routedHere(t, userID, series))
}

// TestIngester_OwnedSeries_WholeRingTenantMatchesRouting checks the tenant whose
// writes are routed over the whole ring is still counted as before.
func TestIngester_OwnedSeries_WholeRingTenantMatchesRouting(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)

	for name, shardSize := range map[string]int{"shuffle sharding off for the tenant": 0, "shard at least the ring size": 100} {
		t.Run(name, func(t *testing.T) {
			userID := "whole-ring-" + fmt.Sprint(shardSize)
			i.tenantLimits.setShardSize(i.limits, userID, shardSize)
			series := i.pushSeries(t, userID, "s", ownedSeriesTestSeries)

			i.updateActiveSeries(context.Background())

			expected := i.routedHere(t, userID, series)
			require.Greater(t, expected, 0)
			require.Equal(t, expected, i.owned(t, userID))
		})
	}
}

// TestIngester_OwnedSeries_NotInShardOwnsNothing covers an ingester holding series
// of a tenant whose shard does not include it, for example after the tenant's shard
// moved: none of those series are routed here, so none are owned.
func TestIngester_OwnedSeries_NotInShardOwnsNothing(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, false)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.pushSeries(t, userID, "s", ownedSeriesTestSeries)

	i.updateActiveSeries(context.Background())
	require.Equal(t, 0, i.owned(t, userID))
}

// TestIngester_OwnedSeries_LimitHoldsWhenShardViewsDisagree covers series arriving
// at an ingester which believes it is not in the tenant's shard, as happens while a
// new shard size has reached the distributors but not yet this ingester. New series
// count as owned when they arrive, so the per-tenant series limit still applies.
func TestIngester_OwnedSeries_LimitHoldsWhenShardViewsDisagree(t *testing.T) {
	const (
		shardSize  = 6
		localLimit = 100
	)
	i := prepareOwnedSeriesTestIngester(t, func(cfg *Config, limits *validation.Limits) {
		cfg.OwnedSeriesLimitEnforcementEnabled = true
		limits.MaxLocalSeriesPerUser = localLimit
	})

	userID := i.tenantWithShardMembership(t, shardSize, false)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.pushSeries(t, userID, "first", 1)
	i.updateActiveSeries(context.Background())

	ctx := user.InjectOrgID(context.Background(), userID)
	rejected := 0
	for n := range 2 * localLimit {
		req, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "metric", "series", fmt.Sprintf("more-%d", n)), 1, time.Now().UnixMilli())
		if _, err := i.Push(ctx, req); err != nil {
			require.True(t, isPerUserSeriesLimitErr(err), "unexpected error: %v", err)
			rejected++
		}
	}
	// The first series no longer counts once ownership is applied, so exactly
	// localLimit new series fit.
	require.Equal(t, localLimit, rejected, "series beyond the local limit must be rejected")
}

// TestIngester_OwnedSeries_ReopenedTenantIsRecounted covers a tenant whose TSDB was
// closed while idle and opened again, which gives it a new tracker with no
// ownership. The frequent recount must apply ownership to it straight away rather
// than wait for the next active series update.
func TestIngester_OwnedSeries_ReopenedTenantIsRecounted(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.pushSeries(t, userID, "s", 10)
	i.updateActiveSeries(context.Background())

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	// Stand in for the TSDB being closed and opened again.
	db.activeSeries = NewActiveSeries()
	series := i.pushSeries(t, userID, "after-reopen", ownedSeriesTestSeries)
	require.Equal(t, ownedSeriesTestSeries, i.owned(t, userID), "a new tracker counts everything until ownership is applied")

	i.recountOwnedSeriesIfChanged()
	require.NotZero(t, db.activeSeries.currFingerprint)
	require.Equal(t, i.routedHere(t, userID, series), i.owned(t, userID))
}

// TestIngester_OwnedSeries_ShardExpansion grows a tenant's shuffle shard while
// this ingester holds the tenant's series, with no ring change. Growing the
// shard keeps this ingester in it but moves some of the tenant's series to the new
// members, while this ingester still holds them. The owned count must drop to the
// series still routed here, without a ring change and without waiting for the next
// active series update.
func TestIngester_OwnedSeries_ShardExpansion(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	series := i.pushSeries(t, userID, "s", ownedSeriesTestSeries)
	i.updateActiveSeries(context.Background())
	before := i.owned(t, userID)
	require.Equal(t, i.routedHere(t, userID, series), before)

	// Growing a shard only adds members, so this ingester stays in it.
	i.tenantLimits.setShardSize(i.limits, userID, 2*shardSize)
	require.True(t, i.inShard(userID, 2*shardSize))

	i.recountOwnedSeriesIfChanged()

	after := i.owned(t, userID)
	require.Equal(t, i.routedHere(t, userID, series), after)
	require.Less(t, after, before, "some series must have moved to the new shard members")

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	require.Equal(t, uint64(ownedSeriesTestSeries), db.Head().NumSeries(), "the moved series are still held in memory")
}

// TestIngester_OwnedSeries_RingChangeRecountsWithoutWaiting checks that a ring
// change is applied by the frequent recount, not only by the next active series
// update, so that the owned count does not lag a local limit which has already
// shrunk.
func TestIngester_OwnedSeries_RingChangeRecountsWithoutWaiting(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)

	wholeRing := "whole-ring"
	wholeRingSeries := i.pushSeries(t, wholeRing, "w", ownedSeriesTestSeries)

	const shardSize = 6
	sharded := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, sharded, shardSize)
	shardedSeries := i.pushSeries(t, sharded, "s", ownedSeriesTestSeries)

	i.updateActiveSeries(context.Background())
	wholeRingBefore := i.owned(t, wholeRing)
	require.Equal(t, i.routedHere(t, wholeRing, wholeRingSeries), wholeRingBefore)
	require.Equal(t, i.routedHere(t, sharded, shardedSeries), i.owned(t, sharded))

	for n := range 10 {
		i.addPeer(t, fmt.Sprintf("new-peer-%d", n))
	}
	i.recountOwnedSeriesIfChanged()

	require.Equal(t, i.routedHere(t, wholeRing, wholeRingSeries), i.owned(t, wholeRing))
	require.Less(t, i.owned(t, wholeRing), wholeRingBefore, "a scale-up must move some of the whole-ring tenant's series away")

	// Adding instances can move a shard. Either way this ingester counts what is
	// routed to it, which is nothing once it is out of the shard.
	require.Equal(t, i.routedHere(t, sharded, shardedSeries), i.owned(t, sharded))
}

// TestIngester_OwnedSeries_RecountSkipsUnchangedTenants checks that the frequent
// recount leaves alone tenants whose ownership has not changed. During a rolling
// restart it runs every few seconds, so recounting every tenant each time would be
// expensive.
func TestIngester_OwnedSeries_RecountSkipsUnchangedTenants(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	unchanged := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, unchanged, shardSize)
	i.pushSeries(t, unchanged, "s", 10)
	i.updateActiveSeries(context.Background())

	db, err := i.getTSDB(unchanged)
	require.NoError(t, err)
	before := db.activeSeries.currFingerprint

	i.recountOwnedSeriesIfChanged()
	require.Equal(t, before, db.activeSeries.currFingerprint)
	require.Contains(t, i.ownedSeries.tenants, unchanged)
}

func TestSubringOwnershipFingerprint(t *testing.T) {
	seen := map[uint64]struct{}{}
	for _, ringFingerprint := range []uint64{1, 2, 1 << 40} {
		for _, shardSize := range []int{3, 6, 9} {
			fingerprint := subringOwnershipFingerprint(ringFingerprint, shardSize)
			require.NotZero(t, fingerprint, "zero means unknown ownership")
			require.NotContains(t, seen, fingerprint)
			seen[fingerprint] = struct{}{}
		}
	}
}

// setPeerState changes the state of an instance in the ring, and waits until the
// ingester's view of the ring has it.
func (i *ownedSeriesTestIngester) setPeerState(t *testing.T, id string, state ring.InstanceState) {
	store := i.cfg.LifecyclerConfig.RingConfig.KVStore.Mock
	require.NoError(t, store.CAS(context.Background(), RingKey, func(in any) (any, bool, error) {
		desc := ring.GetOrCreateRingDesc(in)
		instance := desc.Ingesters[id]
		instance.State = state
		desc.Ingesters[id] = instance
		return desc, true, nil
	}))
	test.Poll(t, 5*time.Second, state, func() any {
		desc := i.lifecycler.RingDesc()
		if desc == nil {
			return nil
		}
		return desc.Ingesters[id].State
	})
}

// TestIngester_OwnedSeries_ChangeOutsideShardDoesNotRecountTenant covers the
// common event of a rolling restart: an ingester outside a sharded tenant's shard
// changes state. The tenant's ownership cannot have changed, so its series must
// not be re-evaluated.
func TestIngester_OwnedSeries_ChangeOutsideShardDoesNotRecountTenant(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.pushSeries(t, userID, "s", 100)
	i.updateActiveSeries(context.Background())

	db, err := i.getTSDB(userID)
	require.NoError(t, err)
	before := db.activeSeries.currFingerprint
	require.NotZero(t, before)

	var outside string
	for n := range ownedSeriesTestPeers {
		id := fmt.Sprintf("peer-%d", n)
		if _, err := i.distributorRing.ShuffleShard(userID, shardSize).GetInstanceState(id); err != nil {
			outside = id
			break
		}
	}
	require.NotEmpty(t, outside)
	i.setPeerState(t, outside, ring.LEAVING)

	i.recountOwnedSeriesIfChanged()
	i.updateActiveSeries(context.Background())
	require.Equal(t, before, db.activeSeries.currFingerprint, "a change outside the shard must not re-evaluate the tenant's series")
}

func TestSeriesOwnership_WithBitmap(t *testing.T) {
	tokens := []uint32{10, 20, 30}
	applied := seriesOwnership{}.withBitmap(tokens, []bool{true, false, true}, 1)
	require.Equal(t, uint64(1), applied.fingerprint)

	t.Run("same bitmap keeps the fingerprint", func(t *testing.T) {
		same := applied.withBitmap(tokens, []bool{true, false, true}, 2)
		require.Equal(t, applied.fingerprint, same.fingerprint)
		require.Equal(t, uint64(2), same.source)
	})
	t.Run("different positions change it", func(t *testing.T) {
		changed := applied.withBitmap(tokens, []bool{true, true, true}, 2)
		require.Equal(t, uint64(2), changed.fingerprint)
	})
	t.Run("a new token list with the same tokens keeps it, and adopts the list", func(t *testing.T) {
		rebuilt := []uint32{10, 20, 30}
		same := applied.withBitmap(rebuilt, []bool{true, false, true}, 2)
		require.Equal(t, applied.fingerprint, same.fingerprint)
		require.Same(t, &rebuilt[0], &same.tokens[0])
	})
	t.Run("different tokens change it", func(t *testing.T) {
		changed := applied.withBitmap([]uint32{10, 20, 31}, []bool{true, false, true}, 2)
		require.Equal(t, uint64(2), changed.fingerprint)
	})
	t.Run("unknown ownership is never kept", func(t *testing.T) {
		unknown := seriesOwnership{}.withBitmap(nil, nil, 3)
		require.Equal(t, uint64(3), unknown.fingerprint)
	})
}

// TestIngester_OwnedSeries_TopologyChangeOutsideShardDoesNotRecountTenant covers
// an instance joining the ring outside a sharded tenant's shard. That rebuilds the
// tenant's subring, but leaves its ownership the same, so its series must not be
// re-evaluated.
func TestIngester_OwnedSeries_TopologyChangeOutsideShardDoesNotRecountTenant(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.pushSeries(t, userID, "s", 500)
	i.updateActiveSeries(context.Background())
	db, err := i.getTSDB(userID)
	require.NoError(t, err)

	members := func() map[string]ring.InstanceDesc {
		m, err := i.distributorRing.ShuffleShard(userID, shardSize).GetInstanceDescsForOperation(ring.Reporting)
		require.NoError(t, err)
		return m
	}

	for n := range 20 {
		before, fingerprint, owned := members(), db.activeSeries.currFingerprint, db.activeSeries.Owned()
		i.addPeer(t, fmt.Sprintf("outside-%d", n))
		if len(members()) != len(before) || !shardHasSameMembers(before, members()) {
			// The new instance moved the shard. Try another.
			i.updateActiveSeries(context.Background())
			continue
		}

		i.recountOwnedSeriesIfChanged()
		require.Equal(t, owned, db.activeSeries.Owned())
		require.Equal(t, fingerprint, db.activeSeries.currFingerprint, "an unchanged shard must not re-evaluate the tenant's series")
		return
	}
	t.Fatal("every added instance moved the shard")
}

func shardHasSameMembers(a, b map[string]ring.InstanceDesc) bool {
	for id := range a {
		if _, ok := b[id]; !ok {
			return false
		}
	}
	return true
}

func TestIngester_OwnershipOperationFollowsExtendWrites(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	require.True(t, cfg.DistributorExtendWrites, "the default must match -distributor.extend-writes")

	i := &Ingester{cfg: cfg}
	require.Equal(t, ring.Write, i.ownershipOperation())

	i.cfg.DistributorExtendWrites = false
	require.Equal(t, ring.WriteNoExtend, i.ownershipOperation())
}
