package ingester

import (
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

const (
	ownershipTestSeed      = 7
	ownershipTestTolerance = 1e-9
	ownershipTestRingKey   = "ownership-test"
)

type ownershipTestRingLayout struct {
	zones             int
	perZone           int
	tokensPerInstance int
}

func ownershipTestInstanceID(zone, instance int) string {
	return fmt.Sprintf("zone-%d-ingester-%d", zone, instance)
}

// newOwnershipTestDesc returns a ring with random tokens. Few tokens per
// instance make ownership uneven, as it is in a real ring.
func newOwnershipTestDesc(rnd *rand.Rand, layout ownershipTestRingLayout) *ring.Desc {
	desc := ring.NewDesc()
	used := map[uint32]struct{}{}
	for z := range layout.zones {
		for i := range layout.perZone {
			tokens := make([]uint32, 0, layout.tokensPerInstance)
			for len(tokens) < layout.tokensPerInstance {
				tok := rnd.Uint32()
				if _, dup := used[tok]; dup {
					continue
				}
				used[tok] = struct{}{}
				tokens = append(tokens, tok)
			}
			slices.Sort(tokens)
			id := ownershipTestInstanceID(z, i)
			desc.AddIngester(id, id+"-addr", fmt.Sprintf("zone-%d", z), tokens, ring.ACTIVE, time.Time{})
		}
	}
	return desc
}

func newOwnershipTestRing(t *testing.T, replicationFactor int, excludedZones []string) *ring.Ring {
	t.Helper()
	kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })

	cfg := ring.Config{
		HeartbeatTimeout:     time.Minute,
		ReplicationFactor:    replicationFactor,
		ZoneAwarenessEnabled: true,
		ExcludedZones:        excludedZones,
	}
	r, err := ring.NewDescFedRing(cfg, "ingester-ownership", ownershipTestRingKey, kvClient, log.NewNopLogger())
	require.NoError(t, err)
	return r
}

func TestRingOwnershipDelegate_FeedsACopyOfTheDesc(t *testing.T) {
	desc := ring.NewDesc()
	desc.AddIngester("a-1", "a-1-addr", "a", []uint32{300, 100}, ring.ACTIVE, time.Time{})
	desc.AddIngester("a-2", "a-2-addr", "a", []uint32{200}, ring.ACTIVE, time.Time{})
	desc.AddIngester("b-1", "b-1-addr", "b", []uint32{400}, ring.ACTIVE, time.Time{})

	r := newOwnershipTestRing(t, 1, []string{"b"})
	delegate := &ringOwnershipDelegate{}
	delegate.ring.Store(r)

	delegate.OnRingInstanceHeartbeat(nil, desc)

	// The ring deletes excluded zones and sorts tokens in place; the desc being
	// written to the KV store must be left as it was.
	assert.Contains(t, desc.Ingesters, "b-1")
	assert.Equal(t, []uint32{300, 100}, desc.Ingesters["a-1"].Tokens)

	ownership := r.ZonalOwnership()
	assert.NotContains(t, ownership, "b-1")
	assert.Positive(t, ownership["a-1"])
	assert.Positive(t, ownership["a-2"])
}

func TestRingOwnershipDelegate_NoRing(t *testing.T) {
	delegate := &ringOwnershipDelegate{}
	assert.NotPanics(t, func() { delegate.OnRingInstanceHeartbeat(nil, ring.NewDesc()) })
}

func TestOwnershipRefresher(t *testing.T) {
	const (
		shuffledTenant   = "shuffled"
		unshuffledTenant = "unshuffled"
		shardSize        = 6
	)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)

	r := newOwnershipTestRing(t, layout.zones, nil)
	limits := validation.Limits{}
	tenantLimits := &staticTenantLimits{limits: map[string]*validation.Limits{
		shuffledTenant: {IngestionTenantShardSize: shardSize},
	}}
	overrides := validation.NewOverrides(limits, tenantLimits)

	ingesterID := ownershipTestInstanceID(0, 0)
	refresher := newOwnershipRefresher(r, ingesterID, true, overrides, nil, nil)

	// Nothing is known before the ring has been fed.
	refresher.refresh([]string{shuffledTenant, unshuffledTenant})
	_, ok := refresher.ownership.share(unshuffledTenant)
	assert.False(t, ok)

	r.UpdateDesc(desc)
	refresher.refresh([]string{shuffledTenant, unshuffledTenant})

	share, ok := refresher.ownership.share(unshuffledTenant)
	require.True(t, ok)
	assert.InDelta(t, r.ZonalOwnership()[ingesterID], share, ownershipTestTolerance)

	subOwnership := r.ShuffleShard(shuffledTenant, shardSize).(ring.ZonalOwnershipRing).ZonalOwnership()
	share, ok = refresher.ownership.share(shuffledTenant)
	if want, inShard := subOwnership[ingesterID]; inShard {
		require.True(t, ok)
		assert.InDelta(t, want, share, ownershipTestTolerance)
	} else {
		assert.False(t, ok, "an ingester outside the tenant's shard has no share")
	}

	_, ok = refresher.ownership.share("unknown-tenant")
	assert.False(t, ok)
}

func TestOwnershipRefresher_ZoneMissingFromRing(t *testing.T) {
	const tenant = "tenant"
	// RF 3 over a ring with only 2 zones: Get puts a second copy in one zone,
	// which zonal shares don't describe.
	layout := ownershipTestRingLayout{zones: 2, perZone: 4, tokensPerInstance: 16}
	r := newOwnershipTestRing(t, 3, nil)
	r.UpdateDesc(newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout))

	refresher := newOwnershipRefresher(r, ownershipTestInstanceID(0, 0), false, validation.NewOverrides(validation.Limits{}, nil), nil, nil)
	refresher.refresh([]string{tenant})

	_, ok := refresher.ownership.share(tenant)
	assert.False(t, ok)
}

func TestConfig_RingOwnershipRequiresZoneAwareness(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.UseRingOwnershipForLimits = true

	cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled = false
	assert.ErrorIs(t, cfg.Validate(nil), errRingOwnershipRequiresZoneAwareness)

	cfg.LifecyclerConfig.RingConfig.ZoneAwarenessEnabled = true
	assert.NoError(t, cfg.Validate(nil))
}

func TestLimiter_LocalLimitFromOwnership(t *testing.T) {
	const (
		globalLimit       = 1000
		replicationFactor = 3
		ingesters         = 30
		zones             = 3
		tenant            = "tenant"
	)
	// The existing formula: 1000 / 30 ingesters * RF 3.
	const evenSplitLimit = 100

	tests := map[string]struct {
		share             float64
		zoneAware         bool
		shardByAllLabels  bool
		expectedPerUser   int
		expectedPerMetric int
	}{
		"share is used": {
			share: 0.15, zoneAware: true, shardByAllLabels: true,
			expectedPerUser: 150, expectedPerMetric: 150,
		},
		"no share falls back to the even split": {
			zoneAware: true, shardByAllLabels: true,
			expectedPerUser: evenSplitLimit, expectedPerMetric: evenSplitLimit,
		},
		"share is ignored without zone awareness": {
			share: 0.15, shardByAllLabels: true,
			expectedPerUser: evenSplitLimit, expectedPerMetric: evenSplitLimit,
		},
		"per-metric limit is not divided without shard-by-all-labels": {
			share: 0.15, zoneAware: true,
			expectedPerUser: math.MaxInt32, expectedPerMetric: globalLimit,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ringCount := &ringCountMock{}
			ringCount.On("HealthyInstancesCount").Return(ingesters)
			ringCount.On("ZonesCount").Return(zones)
			overrides := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerUser:   globalLimit,
				MaxGlobalSeriesPerMetric: globalLimit,
			}, nil)

			limiter := NewLimiter(overrides, ringCount, util.ShardingStrategyDefault, tc.shardByAllLabels, replicationFactor, tc.zoneAware, "")
			ownership := &tenantOwnership{}
			if tc.share > 0 {
				ownership.shares.Store(&map[string]float64{tenant: tc.share})
			}
			limiter.ownership = ownership

			assert.Equal(t, tc.expectedPerUser, limiter.maxSeriesPerUser(tenant))
			assert.Equal(t, tc.expectedPerMetric, limiter.maxSeriesPerMetric(tenant))
		})
	}
}

// A tenant at 95% of its global limit, on a ring whose ownership is uneven:
// the even-split limit throttles it on the ingesters with the largest share,
// while the ownership-based limit does not throttle it anywhere.
func TestLimiter_OwnershipAvoidsThrottlingOnUnevenRing(t *testing.T) {
	const (
		globalLimit = 100_000
		pushed      = 95_000
		tenant      = "tenant"
	)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	r := newOwnershipTestRing(t, layout.zones, nil)
	r.UpdateDesc(newOwnershipTestDesc(rnd, layout))

	// Route the tenant's series the way the distributor does.
	received := map[string]int{}
	bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()
	for range pushed {
		set, err := r.Get(rnd.Uint32(), ring.Write, bufDescs, bufHosts, bufZones)
		require.NoError(t, err)
		for _, inst := range set.Instances {
			received[inst.Addr]++
		}
	}

	ringCount := &ringCountMock{}
	ringCount.On("HealthyInstancesCount").Return(layout.zones * layout.perZone)
	ringCount.On("ZonesCount").Return(layout.zones)
	overrides := validation.NewOverrides(validation.Limits{MaxGlobalSeriesPerUser: globalLimit}, nil)
	evenSplit := NewLimiter(overrides, ringCount, util.ShardingStrategyDefault, true, layout.zones, true, "")

	throttledByEvenSplit := 0
	for z := range layout.zones {
		for i := range layout.perZone {
			id := ownershipTestInstanceID(z, i)
			series := received[id+"-addr"]

			if evenSplit.AssertMaxSeriesPerUser(tenant, series) != nil {
				throttledByEvenSplit++
			}

			refresher := newOwnershipRefresher(r, id, false, overrides, nil, nil)
			refresher.refresh([]string{tenant})
			byOwnership := NewLimiter(overrides, ringCount, util.ShardingStrategyDefault, true, layout.zones, true, "")
			byOwnership.ownership = refresher.ownership
			assert.NoError(t, byOwnership.AssertMaxSeriesPerUser(tenant, series), "ingester %s with %d series", id, series)
		}
	}
	t.Logf("throttled by even split on %d of %d ingesters", throttledByEvenSplit, layout.zones*layout.perZone)
	assert.Positive(t, throttledByEvenSplit, "expected the even split to throttle the tenant on this ring")
}

type staticTenantLimits struct {
	limits map[string]*validation.Limits
}

func (s *staticTenantLimits) ByUserID(userID string) *validation.Limits {
	return s.limits[userID]
}

func (s *staticTenantLimits) AllByUserID() map[string]*validation.Limits {
	return s.limits
}

func newOwnershipTestDelegate(t *testing.T, _ string, replicationFactor int) (*ringOwnershipDelegate, *ring.Ring) {
	t.Helper()
	kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })
	r, err := newOwnershipRing(ring.Config{HeartbeatTimeout: time.Minute, ReplicationFactor: replicationFactor, ZoneAwarenessEnabled: true}, kvClient, log.NewNopLogger())
	require.NoError(t, err)

	d := &ringOwnershipDelegate{}
	d.ring.Store(r)
	return d, r
}

// cloneOwnershipTestDesc returns a copy of desc that a test can modify.
func cloneOwnershipTestDesc(desc *ring.Desc) *ring.Desc {
	out := ring.NewDesc()
	for id, instance := range desc.Ingesters {
		instance.Tokens = slices.Clone(instance.Tokens)
		out.Ingesters[id] = instance
	}
	return out
}

func setOwnershipTestState(id string, state ring.InstanceState) func(*ring.Desc) {
	return func(d *ring.Desc) {
		instance := d.Ingesters[id]
		instance.State = state
		d.Ingesters[id] = instance
	}
}

func TestNewOwnershipRing_DoesNotCacheSubrings(t *testing.T) {
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	d, r := newOwnershipTestDelegate(t, ownershipTestInstanceID(0, 0), layout.zones)
	d.OnRingInstanceHeartbeat(nil, newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout))

	assert.NotSame(t, r.ShuffleShard("tenant", 6), r.ShuffleShard("tenant", 6))
}

func TestRingOwnershipDelegate_Generation(t *testing.T) {
	me := ownershipTestInstanceID(0, 0)
	neighbour := ownershipTestInstanceID(0, 1)
	other := ownershipTestInstanceID(1, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 3, tokensPerInstance: 4}
	changeTokens := func(id string) func(*ring.Desc) {
		return func(d *ring.Desc) {
			instance := d.Ingesters[id]
			instance.Tokens[0]++
			d.Ingesters[id] = instance
		}
	}

	tests := map[string]struct {
		prepare func(*ring.Desc)
		change  func(*ring.Desc)
		bumped  bool
	}{
		"heartbeat only": {change: func(d *ring.Desc) {
			for id, instance := range d.Ingesters {
				instance.Timestamp++
				d.Ingesters[id] = instance
			}
		}},
		"a state change other than READONLY in my zone":    {change: setOwnershipTestState(neighbour, ring.LEAVING)},
		"a state change other than READONLY in other zone": {change: setOwnershipTestState(other, ring.JOINING)},
		"my own state change":                              {change: setOwnershipTestState(me, ring.LEAVING)},
		"an instance becomes READONLY in my zone":          {change: setOwnershipTestState(neighbour, ring.READONLY), bumped: true},
		"an instance becomes READONLY in another zone":     {change: setOwnershipTestState(other, ring.READONLY), bumped: true},
		"an instance stops being READONLY": {
			prepare: setOwnershipTestState(other, ring.READONLY), change: setOwnershipTestState(other, ring.ACTIVE), bumped: true,
		},
		"an instance joins": {
			change: func(d *ring.Desc) { d.AddIngester("new", "new", "zone-1", []uint32{42}, ring.JOINING, time.Time{}) }, bumped: true,
		},
		"an instance leaves": {change: func(d *ring.Desc) { d.RemoveIngester(other) }, bumped: true},
		"tokens change":      {change: changeTokens(other), bumped: true},
		"a tokenless instance gets tokens": {
			prepare: func(d *ring.Desc) { d.AddIngester("new", "new", "zone-new", nil, ring.PENDING, time.Time{}) },
			change: func(d *ring.Desc) {
				instance := d.Ingesters["new"]
				instance.Tokens = []uint32{42}
				d.Ingesters["new"] = instance
			},
			bumped: true,
		},
		"an instance changes zone": {
			change: func(d *ring.Desc) {
				instance := d.Ingesters[other]
				instance.Zone = "zone-2"
				d.Ingesters[other] = instance
			},
			bumped: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			d, _ := newOwnershipTestDelegate(t, me, layout.zones)
			desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
			if tc.prepare != nil {
				tc.prepare(desc)
			}
			d.OnRingInstanceHeartbeat(nil, desc)
			require.Equal(t, uint64(1), d.generation.Load(), "the first ring is a change")

			next := cloneOwnershipTestDesc(desc)
			tc.change(next)
			d.OnRingInstanceHeartbeat(nil, next)

			assert.Equal(t, tc.bumped, d.generation.Load() != 1)
		})
	}
}

func TestOwnershipRefresher_ComputesOwnershipOnlyWhenTheSubringChanges(t *testing.T) {
	const (
		tenant    = "tenant"
		shardSize = 6
	)
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))

	limits := &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: shardSize}}}
	overrides := validation.NewOverrides(validation.Limits{}, limits)
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh([]string{tenant})
	require.Equal(t, 1.0, testutil.ToFloat64(refresher.computations))

	// An instance outside the tenant's subring and one in it.
	members := map[string]bool{}
	healthy, unhealthy, err := r.ShuffleShard(tenant, shardSize).GetAllInstanceDescs(ring.Reporting)
	require.NoError(t, err)
	for _, member := range append(healthy, unhealthy...) {
		members[member.Addr] = true
	}
	var outside, inside string
	for _, id := range slices.Sorted(maps.Keys(desc.Ingesters)) {
		if id == me {
			continue
		}
		if members[desc.Ingesters[id].Addr] && inside == "" {
			inside = id
		}
		if !members[desc.Ingesters[id].Addr] && outside == "" {
			outside = id
		}
	}
	require.NotEmpty(t, outside)
	require.NotEmpty(t, inside)

	update := func(change func(*ring.Desc)) float64 {
		before := testutil.ToFloat64(refresher.computations)
		change(desc)
		d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
		refresher.refresh([]string{tenant})
		assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))
		return testutil.ToFloat64(refresher.computations) - before
	}

	assert.Zero(t, update(setOwnershipTestState(inside, ring.LEAVING)), "not READONLY: the ring has not changed")
	assert.Zero(t, update(setOwnershipTestState(outside, ring.READONLY)), "the ring changed, the tenant's subring did not")
	assert.Equal(t, 1.0, update(setOwnershipTestState(inside, ring.READONLY)), "a member of the tenant's subring changed")

	limits.limits[tenant] = &validation.Limits{IngestionTenantShardSize: shardSize + 3}
	refresher.refresh([]string{tenant})
	assert.Equal(t, shardSize+3, refresher.tenants[tenant].shardSize)
	assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))

	refresher.refresh(nil)
	assert.Empty(t, refresher.tenants, "a tenant gone from the ingester is dropped")
}

// A neighbour going from LEAVING to READONLY moves this ingester's share,
// because shard picks skip READONLY instances. On this layout the share goes
// from 0 to about 0.54.
func TestOwnershipRefresher_ReadOnlyNeighbourChangesTheShardPick(t *testing.T) {
	// The tenant name and the seed below pick a layout where this happens.
	const tenant = "t"
	me := ownershipTestInstanceID(0, 0)
	neighbour := ownershipTestInstanceID(0, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 6, tokensPerInstance: 8}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(3)), layout)
	limits := &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}}
	overrides := validation.NewOverrides(validation.Limits{}, limits)

	setOwnershipTestState(neighbour, ring.LEAVING)(desc)
	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, desc)
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh([]string{tenant})
	before := refresher.tenants[tenant].share

	next := cloneOwnershipTestDesc(desc)
	setOwnershipTestState(neighbour, ring.READONLY)(next)
	d.OnRingInstanceHeartbeat(nil, next)
	refresher.refresh([]string{tenant})

	fresh := newOwnershipRefresher(r, me, true, overrides, nil, nil)
	fresh.refresh([]string{tenant})
	require.NotEqual(t, before, fresh.tenants[tenant].share, "the layout must make the pick change")
	assert.Equal(t, fresh.tenants[tenant].share, refresher.tenants[tenant].share)
}

// unevenOwnershipTestDesc returns a ring of 12 instances, 6 in zone-0 and 3
// in each other zone, so that a shard of 11 is a strict subset of zone-0.
func unevenOwnershipTestDesc() *ring.Desc {
	desc := ring.NewDesc()
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	for z, n := range []int{6, 3, 3} {
		for i := range n {
			tokens := make([]uint32, 16)
			for k := range tokens {
				tokens[k] = rnd.Uint32()
			}
			slices.Sort(tokens)
			id := ownershipTestInstanceID(z, i)
			desc.AddIngester(id, id+"-addr", fmt.Sprintf("zone-%d", z), tokens, ring.ACTIVE, time.Time{})
		}
	}
	return desc
}

// A tenant whose shard size reaches the instance count uses the whole ring, so
// an instance leaving another zone changes its share.
func TestOwnershipRefresher_RecomputesWhenATenantSwitchesToTheWholeRing(t *testing.T) {
	const shardSize = 11
	me := ownershipTestInstanceID(0, 0)
	desc := unevenOwnershipTestDesc()
	tenant := ""
	for i := range 100 {
		candidate := fmt.Sprintf("tenant-%d", i)
		o := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{candidate: {IngestionTenantShardSize: shardSize}}})
		if freshOwnershipShares(t, desc, me, o, []string{candidate})[candidate].share > 0 {
			tenant = candidate
			break
		}
	}
	require.NotEmpty(t, tenant)
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: shardSize}}})

	d, r := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh([]string{tenant})
	before := refresher.tenants[tenant].share

	next := cloneOwnershipTestDesc(desc)
	next.RemoveIngester(ownershipTestInstanceID(1, 0))
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(next))
	refresher.refresh([]string{tenant})

	fresh := freshOwnershipShares(t, next, me, overrides, []string{tenant})
	require.NotEqual(t, before, fresh[tenant].share, "the layout must make the switch move the share")
	assert.Equal(t, fresh, comparableShares(refresher.tenants))
}

// Through any sequence of ring changes, the shares a refresher reuses must
// equal those computed from scratch on a separate ring, so that a bug in the
// reuse or in the ring's caches cannot show on both sides.
func TestOwnershipRefresher_CachedSharesMatchFreshComputation(t *testing.T) {
	for _, sharedTokens := range []bool{false, true} {
		t.Run(fmt.Sprintf("shared tokens %t", sharedTokens), func(t *testing.T) {
			testOwnershipRefresherCachedSharesMatchFreshComputation(t, sharedTokens)
		})
	}
}

func testOwnershipRefresherCachedSharesMatchFreshComputation(t *testing.T, sharedTokens bool) {
	const steps = 300
	states := []ring.InstanceState{ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.JOINING, ring.LEAVING, ring.PENDING, ring.READONLY}
	me := ownershipTestInstanceID(0, 0)
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	layout := ownershipTestRingLayout{zones: 3, perZone: 5, tokensPerInstance: 8}
	desc := newOwnershipTestDesc(rnd, layout)

	tenantLimits := map[string]*validation.Limits{}
	var tenants []string
	for i, size := range []int{0, 3, 6, 9, 13, 14, 15, 16, 30} {
		tenant := fmt.Sprintf("tenant-%d", i)
		tenants = append(tenants, tenant)
		tenantLimits[tenant] = &validation.Limits{IngestionTenantShardSize: size}
	}
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: tenantLimits})

	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	added, bumps := 0, 0
	newToken := func(d *ring.Desc) uint32 {
		if sharedTokens && rnd.Intn(3) == 0 {
			// A token some instance of another zone already holds.
			for _, instance := range d.Ingesters {
				if len(instance.Tokens) > 0 {
					return instance.Tokens[rnd.Intn(len(instance.Tokens))]
				}
			}
		}
		return rnd.Uint32()
	}

	for step := range steps {
		next := cloneOwnershipTestDesc(desc)
		for id, instance := range next.Ingesters {
			instance.Timestamp++
			next.Ingesters[id] = instance
		}
		ids := slices.Sorted(maps.Keys(next.Ingesters))
		id := ids[rnd.Intn(len(ids))]
		instance := next.Ingesters[id]
		switch rnd.Intn(12) {
		case 0: // an instance leaves
			if id != me {
				next.RemoveIngester(id)
			}
		case 1: // an instance joins, sometimes in a new zone
			added++
			zone := fmt.Sprintf("zone-%d", rnd.Intn(layout.zones+1))
			name := fmt.Sprintf("added-%d", added)
			next.AddIngester(name, name+"-addr", zone, []uint32{newToken(next), newToken(next)}, ring.JOINING, time.Time{})
		case 2: // an instance joins without tokens
			added++
			name := fmt.Sprintf("added-%d", added)
			next.AddIngester(name, name+"-addr", fmt.Sprintf("zone-%d", rnd.Intn(layout.zones+1)), nil, ring.PENDING, time.Time{})
		case 3: // tokens change, including a tokenless instance getting its first
			instance.Tokens = append(slices.Clone(instance.Tokens), newToken(next))
			slices.Sort(instance.Tokens)
			next.Ingesters[id] = instance
		case 4: // an instance moves zone
			if id != me {
				instance.Zone = fmt.Sprintf("zone-%d", rnd.Intn(layout.zones+1))
				next.Ingesters[id] = instance
			}
		case 5, 6, 7, 8:
			instance.State = states[rnd.Intn(len(states))]
			next.Ingesters[id] = instance
		}
		desc = next
		before := d.generation.Load()
		d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
		if d.generation.Load() != before {
			bumps++
		}
		refresher.refresh(tenants)

		require.Equal(t, freshOwnershipShares(t, desc, me, overrides, tenants), comparableShares(refresher.tenants), "step %d", step)
	}
	// Not a correctness condition: shows the test exercises reuse.
	t.Logf("the generation changed on %d of %d steps; ownership computed %v times", bumps, steps, testutil.ToFloat64(refresher.computations))
	assert.Less(t, bumps, steps)
}

// comparableShares drops what only decides when a share is recomputed, so
// shares can be compared with ones computed from scratch.
func comparableShares(tenants map[string]tenantShare) map[string]tenantShare {
	out := make(map[string]tenantShare, len(tenants))
	for id, t := range tenants {
		t.unsure = false
		out[id] = t
	}
	return out
}

// Heartbeat callbacks can overlap: the Lifecycler's first heartbeat runs on its
// own goroutine. If the ring were updated outside the delegate's lock, one
// callback could update the ring after another, then compare first and find no
// change, leaving the ring and the recorded desc different for good. So a
// callback must not touch the ring while another holds the lock.
func TestRingOwnershipDelegate_UpdatesTheRingUnderTheLock(t *testing.T) {
	const wait = 50 * time.Millisecond
	me := ownershipTestInstanceID(0, 0)
	neighbour := ownershipTestInstanceID(0, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 3, tokensPerInstance: 4}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))

	readOnly := cloneOwnershipTestDesc(desc)
	setOwnershipTestState(neighbour, ring.READONLY)(readOnly)
	neighbourIsReadOnly := func() bool {
		state, err := r.GetInstanceState(neighbour)
		require.NoError(t, err)
		return state == ring.READONLY
	}

	d.mtx.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.OnRingInstanceHeartbeat(nil, readOnly)
	}()
	time.Sleep(wait)
	assert.False(t, neighbourIsReadOnly(), "the ring was updated while another callback held the lock")
	d.mtx.Unlock()

	<-done
	assert.True(t, neighbourIsReadOnly())
	assert.True(t, d.instances[neighbour].readOnly)
}

// freshOwnershipShares computes the shares from scratch on a new ring, so a bug
// in the reuse or in the ring's caches cannot show on both sides.
func freshOwnershipShares(t *testing.T, desc *ring.Desc, me string, overrides *validation.Overrides, tenants []string) map[string]tenantShare {
	t.Helper()
	_, r := newOwnershipTestDelegate(t, me, 3)
	r.UpdateDesc(desc)
	fresh := newOwnershipRefresher(r, me, true, overrides, nil, nil)
	fresh.refresh(tenants)
	return comparableShares(fresh.tenants)
}

// collidingOwnershipTestDesc returns a ring where an instance of zone-1 holds
// a token of a zone-0 neighbour of me, the one just before a token of me. The
// zone-1 instance's ID sorts after every zone-0 ID, so it wins the ring-wide
// token map, and its state decides who owns me's preceding range.
func collidingOwnershipTestDesc(t *testing.T, layout ownershipTestRingLayout, me, collider string) *ring.Desc {
	t.Helper()
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	owner := map[uint32]string{}
	var tokens []uint32
	for id, instance := range desc.Ingesters {
		if instance.Zone == "zone-0" {
			for _, token := range instance.Tokens {
				owner[token] = id
				tokens = append(tokens, token)
			}
		}
	}
	slices.Sort(tokens)
	for i, token := range tokens {
		if owner[token] != me && owner[tokens[(i+1)%len(tokens)]] == me {
			instance := desc.Ingesters[collider]
			instance.Tokens = append(slices.Clone(instance.Tokens[1:]), token)
			slices.Sort(instance.Tokens)
			desc.Ingesters[collider] = instance
			return desc
		}
	}
	t.Fatal("no token found")
	return nil
}

func TestOwnershipRefresher_TokenSharedAcrossZones(t *testing.T) {
	const tenant = "tenant"
	me := ownershipTestInstanceID(0, 0)
	collider := ownershipTestInstanceID(1, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	tenants := []string{tenant}

	tests := map[string]struct {
		// Starts from a ring without the shared token when false.
		sharedFromStart bool
		prepare         func(*ring.Desc)
		change          func(*ring.Desc)
	}{
		"the instance holding it in another zone becomes READONLY": {
			sharedFromStart: true,
			change:          setOwnershipTestState(collider, ring.READONLY),
		},
		"an instance joins another zone with it": {
			change: func(d *ring.Desc) {
				shared := collidingOwnershipTestDesc(t, layout, me, collider).Ingesters[collider].Tokens
				d.AddIngester("zone-1-joined", "zone-1-joined-addr", "zone-1", shared, ring.READONLY, time.Time{})
			},
		},
		"the READONLY instance holding it in another zone leaves": {
			sharedFromStart: true,
			prepare:         setOwnershipTestState(collider, ring.READONLY),
			change:          func(d *ring.Desc) { d.RemoveIngester(collider) },
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
			if tc.sharedFromStart {
				desc = collidingOwnershipTestDesc(t, layout, me, collider)
			}
			if tc.prepare != nil {
				tc.prepare(desc)
			}
			d, r := newOwnershipTestDelegate(t, me, layout.zones)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
			refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
			refresher.refresh(tenants)
			before := refresher.tenants[tenant].share

			next := cloneOwnershipTestDesc(desc)
			tc.change(next)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(next))
			refresher.refresh(tenants)

			fresh := freshOwnershipShares(t, next, me, overrides, tenants)
			require.NotEqual(t, before, fresh[tenant].share, "the layout must make the change move the share")
			assert.Equal(t, fresh, comparableShares(refresher.tenants))
		})
	}
}

// hookedTenantLimits runs hook once, the first time ByUserID is called for
// user, standing in for a heartbeat that updates the ring during a refresh.
type hookedTenantLimits struct {
	staticTenantLimits
	user string
	hook func()
	once sync.Once
}

func (h *hookedTenantLimits) ByUserID(userID string) *validation.Limits {
	if userID == h.user && h.hook != nil {
		h.once.Do(h.hook)
	}
	return h.staticTenantLimits.ByUserID(userID)
}

// arm makes hook run the next time ByUserID is called for user.
func (h *hookedTenantLimits) arm(user string, hook func()) {
	h.user, h.hook, h.once = user, hook, sync.Once{}
}

// A heartbeat during a refresh can change the ring between the generation the
// refresh read and the subring it builds. The stored share must describe the
// ring it was computed from, so that the next refresh compares against the
// right one.
func TestOwnershipRefresher_WholeRingFlagDescribesTheRingUsed(t *testing.T) {
	const shardSize = 11
	me := ownershipTestInstanceID(0, 0)
	desc := unevenOwnershipTestDesc()
	rnd := rand.New(rand.NewSource(ownershipTestSeed + 1))
	// A tenant whose subring includes this ingester.
	tenant := ""
	for i := range 100 {
		candidate := fmt.Sprintf("tenant-%d", i)
		o := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{candidate: {IngestionTenantShardSize: shardSize}}})
		if freshOwnershipShares(t, desc, me, o, []string{candidate})[candidate].share > 0 {
			tenant = candidate
			break
		}
	}
	require.NotEmpty(t, tenant)
	limits := &hookedTenantLimits{user: tenant}
	limits.limits = map[string]*validation.Limits{tenant: {IngestionTenantShardSize: shardSize}}
	overrides := validation.NewOverrides(validation.Limits{}, limits)

	d, r := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh(nil)

	// During the refresh that first sees the tenant, an instance leaves zone-1,
	// so ShuffleShard sees 11 instances and returns the whole ring.
	shrunk := cloneOwnershipTestDesc(desc)
	shrunk.RemoveIngester(ownershipTestInstanceID(1, 2))
	limits.hook = func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(shrunk)) }
	refresher.refresh([]string{tenant})
	require.Equal(t, freshOwnershipShares(t, shrunk, me, overrides, []string{tenant}), comparableShares(refresher.tenants), "the share comes from the ring ShuffleShard used")

	// An instance joins zone-2: 12 instances again.
	grown := cloneOwnershipTestDesc(shrunk)
	grown.AddIngester(ownershipTestInstanceID(2, 9), "zone-2-ingester-9-addr", "zone-2", []uint32{rnd.Uint32(), rnd.Uint32()}, ring.ACTIVE, time.Time{})
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(grown))
	refresher.refresh([]string{tenant})

	fresh := freshOwnershipShares(t, grown, me, overrides, []string{tenant})
	require.NotEqual(t, fresh[tenant].share, freshOwnershipShares(t, shrunk, me, overrides, []string{tenant})[tenant].share, "the layout must make the two rings differ")
	assert.Equal(t, fresh, comparableShares(refresher.tenants))
}

// The ring leaves excluded zones out, so changes there bump the generation
// but leave every tenant's subring, and so its share, as it was.
func TestOwnershipRefresher_ExcludedZones(t *testing.T) {
	const tenant = "tenant"
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 4, perZone: 3, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	newRing := func() (*ringOwnershipDelegate, *ring.Ring) {
		kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { _ = closer.Close() })
		r, err := newOwnershipRing(ring.Config{HeartbeatTimeout: time.Minute, ReplicationFactor: 3, ZoneAwarenessEnabled: true, ExcludedZones: []string{"zone-3"}}, kvClient, log.NewNopLogger())
		require.NoError(t, err)
		d := &ringOwnershipDelegate{}
		d.ring.Store(r)
		return d, r
	}
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}})

	d, r := newRing()
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh([]string{tenant})
	require.Equal(t, 3, refresher.tenants[tenant].zones)
	computed := testutil.ToFloat64(refresher.computations)

	setOwnershipTestState(ownershipTestInstanceID(3, 0), ring.READONLY)(desc)
	desc.RemoveIngester(ownershipTestInstanceID(3, 1))
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh([]string{tenant})

	assert.Equal(t, computed, testutil.ToFloat64(refresher.computations))
	_, fresh := newRing()
	fresh.UpdateDesc(cloneOwnershipTestDesc(desc))
	want := newOwnershipRefresher(fresh, me, true, overrides, nil, nil)
	want.refresh([]string{tenant})
	assert.Equal(t, comparableShares(want.tenants), comparableShares(refresher.tenants))
}

// An instance outside a tenant's subring that takes a token of this ingester
// changes the ring-wide token map the subring's ownership reads, without
// changing any subring member, so the fingerprint stays the same. The tenant,
// seed and token below were found by search to give that case.
func TestOwnershipRefresher_TokenSharedFromOutsideTheSubring(t *testing.T) {
	const (
		tenant = "tenant-1"
		token  = uint32(1036751706)
	)
	me := ownershipTestInstanceID(0, 0)
	collider := ownershipTestInstanceID(1, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 6, tokensPerInstance: 8}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	require.Contains(t, desc.Ingesters[me].Tokens, token)
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}})

	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	refresher.refresh([]string{tenant})
	before := refresher.tenants[tenant]

	instance := desc.Ingesters[collider]
	instance.Tokens = append(slices.Clone(instance.Tokens), token)
	slices.Sort(instance.Tokens)
	desc.Ingesters[collider] = instance
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh([]string{tenant})

	fresh := freshOwnershipShares(t, desc, me, overrides, []string{tenant})
	require.Equal(t, before.fingerprint, fresh[tenant].fingerprint, "the layout must keep the subring the same")
	require.NotEqual(t, before.share, fresh[tenant].share, "the layout must move the share")
	assert.Equal(t, fresh, comparableShares(refresher.tenants))
}

// A share computed while a token was shared across zones depends on an
// instance outside the subring. When the sharing ends, the subring's members
// and so its fingerprint are as before, but the share is not: it must be
// recomputed.
func TestOwnershipRefresher_RecomputesWhenTokensStopBeingShared(t *testing.T) {
	const (
		tenant = "tenant-1"
		token  = uint32(1036751706)
	)
	me := ownershipTestInstanceID(0, 0)
	collider := ownershipTestInstanceID(1, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 6, tokensPerInstance: 8}
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}})
	tenants := []string{tenant}

	for name, end := range map[string]func(*ring.Desc){
		"the other instance drops the token": func(*ring.Desc) {},
		"the other instance leaves":          func(d *ring.Desc) { d.RemoveIngester(collider) },
	} {
		t.Run(name, func(t *testing.T) {
			desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
			require.Contains(t, desc.Ingesters[me].Tokens, token)
			d, r := newOwnershipTestDelegate(t, me, layout.zones)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
			refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
			refresher.refresh(tenants)
			before := refresher.tenants[tenant].share

			shared := cloneOwnershipTestDesc(desc)
			instance := shared.Ingesters[collider]
			instance.Tokens = append(slices.Clone(instance.Tokens), token)
			slices.Sort(instance.Tokens)
			shared.Ingesters[collider] = instance
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(shared))
			refresher.refresh(tenants)
			require.True(t, r.HasDuplicateTokens())
			require.NotEqual(t, before, refresher.tenants[tenant].share, "the layout must make sharing move the share")

			after := cloneOwnershipTestDesc(desc)
			end(after)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(after))
			require.False(t, r.HasDuplicateTokens())
			refresher.refresh(tenants)

			assert.Equal(t, freshOwnershipShares(t, after, me, overrides, tenants), comparableShares(refresher.tenants))
		})
	}
}

// Whole-ring tenants read the live ring, which a heartbeat can change during a
// refresh. If the ring changes after the whole-ring fingerprint is taken and is
// back as it was by the next refresh, the fingerprint matches again, so the
// share stored with it must not come from the changed ring.
func TestOwnershipRefresher_WholeRingChangedDuringARefreshAndBack(t *testing.T) {
	const (
		first  = "whole-1"
		second = "whole-2"
	)
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	limits := &hookedTenantLimits{}
	limits.limits = map[string]*validation.Limits{}
	overrides := validation.NewOverrides(validation.Limits{}, limits)
	tenants := []string{first, second}

	// A neighbour whose READONLY flip moves the whole-ring share.
	baseline := freshOwnershipShares(t, desc, me, overrides, tenants)[first].share
	readOnly := cloneOwnershipTestDesc(desc)
	for i := 1; i < layout.perZone; i++ {
		candidate := cloneOwnershipTestDesc(desc)
		setOwnershipTestState(ownershipTestInstanceID(0, i), ring.READONLY)(candidate)
		if freshOwnershipShares(t, candidate, me, overrides, tenants)[first].share != baseline {
			readOnly = candidate
			break
		}
	}
	require.NotEqual(t, baseline, freshOwnershipShares(t, readOnly, me, overrides, tenants)[first].share)

	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)

	// The neighbour becomes READONLY after the first whole-ring tenant, and
	// is back to ACTIVE before the next refresh.
	limits.arm(second, func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(readOnly)) })
	refresher.refresh(tenants)
	back := cloneOwnershipTestDesc(desc)
	for id, instance := range back.Ingesters {
		instance.Timestamp += 5
		back.Ingesters[id] = instance
	}
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(back))
	refresher.refresh(tenants)

	assert.Equal(t, freshOwnershipShares(t, back, me, overrides, tenants), comparableShares(refresher.tenants))
}

// Randomized: each step a new ring bumps the generation, so the next refresh
// checks every tenant; during that refresh, at a random tenant, a second change
// lands, and it is undone before the following refresh. Tenants are refreshed
// in a random order. The shares must then equal those computed from scratch.
func TestOwnershipRefresher_ChangesDuringARefreshUndone(t *testing.T) {
	const steps = 300
	me := ownershipTestInstanceID(0, 0)
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	layout := ownershipTestRingLayout{zones: 3, perZone: 5, tokensPerInstance: 8}
	limits := &hookedTenantLimits{}
	limits.limits = map[string]*validation.Limits{}
	var tenants []string
	for i, size := range []int{0, 0, 3, 6, 9, 14, 15, 16, 30} {
		tenant := fmt.Sprintf("tenant-%d", i)
		tenants = append(tenants, tenant)
		limits.limits[tenant] = &validation.Limits{IngestionTenantShardSize: size}
	}
	overrides := validation.NewOverrides(validation.Limits{}, limits)
	states := []ring.InstanceState{ring.ACTIVE, ring.JOINING, ring.LEAVING, ring.PENDING, ring.READONLY}

	d, r := newOwnershipTestDelegate(t, me, layout.zones)
	refresher := newOwnershipRefresher(r, me, true, overrides, nil, d.generation.Load)
	timestamp := int64(0)
	heartbeat := func(desc *ring.Desc) {
		timestamp++
		next := cloneOwnershipTestDesc(desc)
		for id, instance := range next.Ingesters {
			instance.Timestamp = timestamp
			next.Ingesters[id] = instance
		}
		d.OnRingInstanceHeartbeat(nil, next)
	}

	for step := range steps {
		desc := newOwnershipTestDesc(rand.New(rand.NewSource(int64(step))), layout)
		changed := cloneOwnershipTestDesc(desc)
		ids := slices.Sorted(maps.Keys(changed.Ingesters))
		id := ids[rnd.Intn(len(ids))]
		instance := changed.Ingesters[id]
		switch rnd.Intn(4) {
		case 0:
			if id != me {
				changed.RemoveIngester(id)
			}
		case 1:
			changed.AddIngester("joined", "joined-addr", fmt.Sprintf("zone-%d", rnd.Intn(layout.zones)), []uint32{rnd.Uint32(), rnd.Uint32()}, ring.ACTIVE, time.Time{})
		case 2:
			instance.Tokens = append(slices.Clone(instance.Tokens), rnd.Uint32())
			slices.Sort(instance.Tokens)
			changed.Ingesters[id] = instance
		case 3:
			instance.State = states[rnd.Intn(len(states))]
			changed.Ingesters[id] = instance
		}

		heartbeat(desc)
		rnd.Shuffle(len(tenants), func(i, j int) { tenants[i], tenants[j] = tenants[j], tenants[i] })
		limits.arm(tenants[rnd.Intn(len(tenants))], func() { heartbeat(changed) })
		refresher.refresh(tenants)
		heartbeat(desc)
		refresher.refresh(tenants)

		require.Equal(t, freshOwnershipShares(t, desc, me, overrides, tenants), comparableShares(refresher.tenants), "step %d", step)
	}
}
