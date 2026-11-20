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
	"github.com/prometheus/prometheus/tsdb/chunks"
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
	cfg.ownedSeriesCheckPeriod = time.Hour
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
	// The expected size comes from the ring as written, not from a view of it: a
	// view read after the write may already include the new instance.
	want := 0
	require.NoError(t, store.CAS(context.Background(), RingKey, func(in any) (any, bool, error) {
		desc := ring.GetOrCreateRingDesc(in)
		tokens := ring.NewRandomTokenGenerator().GenerateTokens(desc, id, "", ownedSeriesTestTokens, true)
		desc.AddIngester(id, id, "", tokens, ring.ACTIVE, time.Now())
		want = len(desc.Ingesters)
		return desc, true, nil
	}))

	test.Poll(t, 5*time.Second, want, func() any { return i.distributorRing.InstancesCount() })
	test.Poll(t, 5*time.Second, want, func() any {
		desc, _ := i.lifecycler.RingDesc()
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
	i.awaitNewerRingRead(t)
	return all
}

// awaitNewerRingRead waits until the lifecycler has read the ring after
// everything pushed so far. Until then series are too new to be judged by this
// ingester's view of the ring, and count as owned.
func (i *ownedSeriesTestIngester) awaitNewerRingRead(t *testing.T) {
	after := i.lifecycler.RingReads()
	test.Poll(t, 5*time.Second, true, func() any {
		_, read := i.lifecycler.RingDesc()
		return read > after
	})
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
		desc, _ := i.lifecycler.RingDesc()
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

// TestIngester_OwnedSeries_RingChangeIsAppliedWithinASecond checks that once the
// lifecycler reads a changed ring, the owned count follows within about a second,
// without waiting for the recompute interval or the next active series update,
// both of which are an hour here. The lifecycler computes the local limits from
// that same read, so the two change together, and a scale-up does not throttle
// the tenant in between.
//
// The test only observes: the ingester's own update loop does every recount, as in
// a running ingester, since the ownership state belongs to that goroutine.
func TestIngester_OwnedSeries_RingChangeIsAppliedWithinASecond(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t, func(cfg *Config, _ *validation.Limits) {
		cfg.ownedSeriesCheckPeriod = time.Second
	})
	const tenant = "whole-ring"
	series := i.pushSeries(t, tenant, "w", ownedSeriesTestSeries)
	before := i.routedHere(t, tenant, series)

	for n := range 10 {
		i.addPeer(t, fmt.Sprintf("new-peer-%d", n))
	}

	// addPeer has waited for the lifecycler to read the ring with the new peers.
	want := i.routedHere(t, tenant, series)
	require.Less(t, want, before, "a scale-up must move some series away")
	test.Poll(t, 3*time.Second, want, func() any { return i.owned(t, tenant) })
}

// TestIngester_OwnedSeries_TenantFlipsBetweenShardedAndWholeRing covers a tenant
// whose shard size goes from smaller than the ring to none and back, as an
// override can change it at runtime.
func TestIngester_OwnedSeries_TenantFlipsBetweenShardedAndWholeRing(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	series := i.pushSeries(t, userID, "s", ownedSeriesTestSeries)
	i.updateActiveSeries(context.Background())
	sharded := i.owned(t, userID)
	require.Equal(t, i.routedHere(t, userID, series), sharded)

	i.tenantLimits.setShardSize(i.limits, userID, 0)
	i.recountOwnedSeriesIfChanged()
	require.Equal(t, i.routedHere(t, userID, series), i.owned(t, userID))
	require.NotEqual(t, sharded, i.owned(t, userID))

	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	i.recountOwnedSeriesIfChanged()
	require.Equal(t, sharded, i.owned(t, userID))
}

func TestOwnedSeriesLimitCount(t *testing.T) {
	for name, tc := range map[string]struct{ head, owned, tracked, want int }{
		"every series tracked":                    {head: 100, owned: 40, tracked: 100, want: 40},
		"untracked series count as owned":         {head: 100, owned: 40, tracked: 90, want: 50},
		"nothing tracked counts the whole head":   {head: 100, owned: 0, tracked: 0, want: 100},
		"capped at the head while it deletes":     {head: 90, owned: 95, tracked: 100, want: 90},
		"deleted but not yet released, not owned": {head: 90, owned: 40, tracked: 100, want: 40},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, ownedSeriesLimitCount(tc.head, tc.owned, tc.tracked))
		})
	}
}

// TestIngester_OwnedSeries_SeriesRestoredFromTheWALAreJudged restarts an ingester
// on the same WAL. The head is rebuilt without going through Push, so without
// tracker entries every restored series would count as owned until head
// compaction, including the ones the ring sends elsewhere, throttling tenants as
// if owned series were not tracked. They must be judged by the ring like any other.
func TestIngester_OwnedSeries_SeriesRestoredFromTheWALAreJudged(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.LifecyclerConfig.NumTokens = ownedSeriesTestTokens
	cfg.LifecyclerConfig.RingConfig.ReplicationFactor = 3
	cfg.LifecyclerConfig.HeartbeatPeriod = 100 * time.Millisecond
	cfg.DistributorShardByAllLabels = true
	cfg.OwnedSeriesMetricsEnabled = true
	cfg.OwnedSeriesLimitEnforcementEnabled = true
	// The ingester's own loop judges the restored series within a second.
	cfg.ownedSeriesCheckPeriod = time.Second
	cfg.ActiveSeriesMetricsUpdatePeriod = time.Second
	limits := defaultLimitsTestConfig()
	dataDir := t.TempDir()

	start := func() *Ingester {
		i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dataDir, prometheus.NewRegistry())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
		test.Poll(t, 5*time.Second, ring.ACTIVE, func() any { return i.lifecycler.GetState() })
		return i
	}

	first := start()
	ingester := &ownedSeriesTestIngester{Ingester: first, cfg: cfg, limits: limits}
	distributorRing, err := ring.New(cfg.LifecyclerConfig.RingConfig, "ingester", RingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), distributorRing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), distributorRing) })
	ingester.distributorRing = distributorRing
	for n := range ownedSeriesTestPeers {
		ingester.addPeer(t, fmt.Sprintf("peer-%d", n))
	}

	const tenant = "restarted"
	series := ingester.pushSeries(t, tenant, "s", ownedSeriesTestSeries)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), first))

	second := start()
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), second) })
	ingester.Ingester = second
	test.Poll(t, 5*time.Second, true, func() any {
		desc, _ := second.lifecycler.RingDesc()
		_, inRing := desc.GetIngesters()[second.lifecycler.ID]
		return inRing && distributorRing.InstancesCount() == ownedSeriesTestPeers+1
	})

	db, err := second.getTSDB(tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(ownedSeriesTestSeries), db.Head().NumSeries(), "the series are restored from the WAL")

	// Restored series predate every ring read of this process, so the first view
	// may judge them, even after a crash, when the instance is still ACTIVE in the
	// ring and the next read is the first heartbeat.
	restored := series[0]
	stripe := &db.activeSeries.stripes[restored.Hash()%numActiveSeriesStripes]
	stripe.mu.RLock()
	createdRead := stripe.refs[restored.Hash()][0].createdRead
	stripe.mu.RUnlock()
	require.Zero(t, createdRead)

	limitCount := func() int {
		return ownedSeriesLimitCount(int(db.Head().NumSeries()), db.activeSeries.Owned(), db.activeSeries.Tracked())
	}
	test.Poll(t, 5*time.Second, ingester.routedHere(t, tenant, series), func() any { return limitCount() })
	require.Less(t, limitCount(), ownedSeriesTestSeries, "series the ring sends elsewhere must not count")
}

// TestIngester_OwnedSeries_ShardSizeChangeIsAppliedWithinASecond checks that when a
// tenant's shard size changes, which immediately changes its local limit, the
// owned count follows within about a second. Until it does, the tenant can be
// throttled against its new local limit.
func TestIngester_OwnedSeries_ShardSizeChangeIsAppliedWithinASecond(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t, func(cfg *Config, _ *validation.Limits) {
		cfg.ownedSeriesCheckPeriod = time.Second
	})
	const shardSize = 6

	userID := i.tenantWithShardMembership(t, shardSize, true)
	i.tenantLimits.setShardSize(i.limits, userID, shardSize)
	series := i.pushSeries(t, userID, "s", ownedSeriesTestSeries)
	before := i.routedHere(t, userID, series)

	// The new shard size changes the tenant's ownership, so every series is
	// judged again, by a view read after all of them arrived.
	i.tenantLimits.setShardSize(i.limits, userID, 2*shardSize)
	want := i.routedHere(t, userID, series)
	require.Less(t, want, before, "a larger shard moves some series to the new members")
	test.Poll(t, 3*time.Second, want, func() any { return i.owned(t, userID) })
}

// TestIngester_OwnedSeries_PushRecordsTheRingRead checks that a series records the
// lifecycler's ring read number current when its push arrived, so that no older
// view of the ring judges it.
func TestIngester_OwnedSeries_PushRecordsTheRingRead(t *testing.T) {
	i := prepareOwnedSeriesTestIngester(t)
	lbls := i.pushSeries(t, "tenant", "s", 1)[0]

	db, err := i.getTSDB("tenant")
	require.NoError(t, err)
	stripe := &db.activeSeries.stripes[lbls.Hash()%numActiveSeriesStripes]
	stripe.mu.RLock()
	entry := stripe.refs[lbls.Hash()][0]
	stripe.mu.RUnlock()

	require.NotZero(t, entry.createdRead, "the lifecycler has read the ring before any push")
	require.LessOrEqual(t, entry.createdRead, i.lifecycler.RingReads())
	require.False(t, entry.keyless)
}

// TestIngester_PurgeOwnedSeriesBackstopKeepsIdleSeries checks the cutoff of the
// backstop purge run after head compaction. Idle series can stay in the head for
// up to two block ranges and must keep counting as owned, so the cutoff must not
// be the idle timeout.
func TestIngester_PurgeOwnedSeriesBackstopKeepsIdleSeries(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.OwnedSeriesMetricsEnabled = true
	i := &Ingester{cfg: cfg}
	userDB := &userTSDB{activeSeries: NewActiveSeries()}
	now := time.Now()
	blockRange := cfg.BlocksStorageConfig.TSDB.BlockRanges[0]

	idle := labels.FromStrings(labels.MetricName, "idle")
	userDB.activeSeries.UpdateSeries(idle, idle.Hash(), 1, now.Add(-blockRange), false, func(l labels.Labels) labels.Labels { return l })
	gone := labels.FromStrings(labels.MetricName, "gone")
	userDB.activeSeries.UpdateSeries(gone, gone.Hash(), 1, now.Add(-2*blockRange-time.Minute), false, func(l labels.Labels) labels.Labels { return l })

	i.purgeOwnedSeriesBackstop(userDB, now)
	require.Equal(t, 1, userDB.activeSeries.Tracked(), "only the entry older than two block ranges is released")
	require.Equal(t, 1, userDB.activeSeries.Owned())
}

// TestIngester_PurgeOwnedSeriesBackstopKeepsActiveInStep covers the backstop purge
// run after head compaction, between two recounts. It must count active series
// against the cutoff the last recount published, which the push and deletion paths
// use, or the counters fall out of step with them until the next recount.
func TestIngester_PurgeOwnedSeriesBackstopKeepsActiveInStep(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.OwnedSeriesMetricsEnabled = true
	idle := cfg.ActiveSeriesMetricsIdleTimeout
	i := &Ingester{cfg: cfg}
	copyLabels := func(l labels.Labels) labels.Labels { return l }
	t0 := time.Now()

	t.Run("a deletion cannot take the count below zero", func(t *testing.T) {
		userDB := &userTSDB{activeSeries: NewActiveSeries()}
		a, b := labels.FromStrings("__name__", "a"), labels.FromStrings("__name__", "b")
		userDB.activeSeries.UpdateSeriesWithRef(a, a.Hash(), 1, knownSeriesToken(1), t0, false, copyLabels)
		userDB.activeSeries.UpdateSeriesWithRef(b, b.Hash(), 2, knownSeriesToken(1), t0, false, copyLabels)
		userDB.activeSeries.UpdateMetrics(t0.Add(5*time.Minute).Add(-idle), nil, nil, 0, 0)

		i.purgeOwnedSeriesBackstop(userDB, t0.Add(idle+20*time.Second))
		userDB.activeSeries.DeleteSeries(map[chunks.HeadSeriesRef]labels.Labels{1: a})

		require.Equal(t, 1, userDB.activeSeries.Active(), "b is still active as of the last recount")
	})

	t.Run("a reactivation after the backstop is counted", func(t *testing.T) {
		userDB := &userTSDB{activeSeries: NewActiveSeries()}
		a := labels.FromStrings("__name__", "a")
		userDB.activeSeries.UpdateSeries(a, a.Hash(), 1, t0, false, copyLabels)
		userDB.activeSeries.UpdateMetrics(t0.Add(5*time.Minute).Add(-idle), nil, nil, 0, 0)

		i.purgeOwnedSeriesBackstop(userDB, t0.Add(idle+20*time.Second))
		userDB.activeSeries.UpdateSeries(a, a.Hash(), 1, t0.Add(idle+30*time.Second), false, copyLabels)

		require.Equal(t, 1, userDB.activeSeries.Active())
	})
}
