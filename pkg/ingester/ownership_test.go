package ingester

import (
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
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

func TestRingOwnershipDelegate_KeepsACopyOfTheDesc(t *testing.T) {
	desc := ring.NewDesc()
	desc.AddIngester("a-1", "a-1-addr", "a", []uint32{300, 100}, ring.ACTIVE, time.Time{})
	desc.AddIngester("a-2", "a-2-addr", "a", []uint32{200}, ring.ACTIVE, time.Time{})
	desc.AddIngester("b-1", "b-1-addr", "b", []uint32{400}, ring.ACTIVE, time.Time{})

	d, newRing := newOwnershipTestDelegate(t, "a-1", 1, "b")
	d.OnRingInstanceHeartbeat(nil, desc)
	// The KV store can change the desc it writes after the callback.
	desc.RemoveIngester("a-2")
	slices.Sort(desc.Ingesters["a-1"].Tokens)

	kept := d.latest()
	assert.Contains(t, kept.Ingesters, "a-2")
	assert.Equal(t, []uint32{300, 100}, kept.Ingesters["a-1"].Tokens)

	// The ring deletes excluded zones and sorts tokens in place, but not in the
	// desc it is built from.
	r, err := newRing()
	require.NoError(t, err)
	r.UpdateDesc(kept)
	assert.Contains(t, kept.Ingesters, "b-1")
	assert.Equal(t, []uint32{300, 100}, kept.Ingesters["a-1"].Tokens)

	ownership := r.ZonalOwnership()
	assert.NotContains(t, ownership, "b-1")
	assert.Positive(t, ownership["a-1"])
	assert.Positive(t, ownership["a-2"])
}

func TestRingOwnershipDelegate_Disabled(t *testing.T) {
	d := &ringOwnershipDelegate{}
	d.OnRingInstanceHeartbeat(nil, newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), ownershipTestRingLayout{zones: 3, perZone: 1, tokensPerInstance: 1}))
	assert.Nil(t, d.latest())
}

func TestOwnershipRefresher(t *testing.T) {
	const (
		shuffledTenant   = "shuffled"
		unshuffledTenant = "unshuffled"
		shardSize        = 6
	)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)

	limits := validation.Limits{}
	tenantLimits := &staticTenantLimits{limits: map[string]*validation.Limits{
		shuffledTenant: {IngestionTenantShardSize: shardSize},
	}}
	overrides := validation.NewOverrides(limits, tenantLimits)

	ingesterID := ownershipTestInstanceID(0, 0)
	d, newRing := newOwnershipTestDelegate(t, ingesterID, layout.zones)
	refresher := newOwnershipRefresher(d.latest, newRing, ingesterID, true, overrides, nil)

	// Nothing is known before the ring has been fed.
	refresher.refresh(context.Background(), []string{shuffledTenant, unshuffledTenant})
	_, ok := refresher.ownership.share(unshuffledTenant)
	assert.False(t, ok)

	d.OnRingInstanceHeartbeat(nil, desc)
	refresher.refresh(context.Background(), []string{shuffledTenant, unshuffledTenant})

	r := newOwnershipTestRing(t, layout.zones, nil)
	r.UpdateDesc(desc)

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
	d, newRing := newOwnershipTestDelegate(t, ownershipTestInstanceID(0, 0), 3)
	d.OnRingInstanceHeartbeat(nil, newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout))

	refresher := newOwnershipRefresher(d.latest, newRing, ownershipTestInstanceID(0, 0), false, validation.NewOverrides(validation.Limits{}, nil), nil)
	refresher.refresh(context.Background(), []string{tenant})

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
	desc := newOwnershipTestDesc(rnd, layout)
	r := newOwnershipTestRing(t, layout.zones, nil)
	r.UpdateDesc(desc)
	d, newRing := newOwnershipTestDelegate(t, "", layout.zones)
	d.OnRingInstanceHeartbeat(nil, desc)

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

			refresher := newOwnershipRefresher(d.latest, newRing, id, false, overrides, nil)
			refresher.refresh(context.Background(), []string{tenant})
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

// newOwnershipTestDelegate returns an enabled delegate, and a function that
// builds rings with the replication factor given, as the ingester does.
func newOwnershipTestDelegate(t *testing.T, _ string, replicationFactor int, excludedZones ...string) (*ringOwnershipDelegate, func() (*ring.Ring, error)) {
	t.Helper()
	kvClient, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { _ = closer.Close() })
	cfg := ring.Config{HeartbeatTimeout: time.Minute, ReplicationFactor: replicationFactor, ZoneAwarenessEnabled: true, ExcludedZones: excludedZones}
	return &ringOwnershipDelegate{enabled: true}, func() (*ring.Ring, error) {
		return newOwnershipRing(cfg, kvClient, log.NewNopLogger())
	}
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
	_, newRing := newOwnershipTestDelegate(t, ownershipTestInstanceID(0, 0), layout.zones)
	r, err := newRing()
	require.NoError(t, err)
	r.UpdateDesc(newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout))

	assert.NotSame(t, r.ShuffleShard("tenant", 6), r.ShuffleShard("tenant", 6))
}

func TestRingOwnershipDelegate_KeepsADescOnEachChange(t *testing.T) {
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
		changed bool
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
		"an instance becomes READONLY in my zone":          {change: setOwnershipTestState(neighbour, ring.READONLY), changed: true},
		"an instance becomes READONLY in another zone":     {change: setOwnershipTestState(other, ring.READONLY), changed: true},
		"an instance stops being READONLY": {
			prepare: setOwnershipTestState(other, ring.READONLY), change: setOwnershipTestState(other, ring.ACTIVE), changed: true,
		},
		"an instance joins": {
			change: func(d *ring.Desc) { d.AddIngester("new", "new", "zone-1", []uint32{42}, ring.JOINING, time.Time{}) }, changed: true,
		},
		"an instance leaves": {change: func(d *ring.Desc) { d.RemoveIngester(other) }, changed: true},
		"tokens change":      {change: changeTokens(other), changed: true},
		"a tokenless instance gets tokens": {
			prepare: func(d *ring.Desc) { d.AddIngester("new", "new", "zone-new", nil, ring.PENDING, time.Time{}) },
			change: func(d *ring.Desc) {
				instance := d.Ingesters["new"]
				instance.Tokens = []uint32{42}
				d.Ingesters["new"] = instance
			},
			changed: true,
		},
		"an address change": {change: func(d *ring.Desc) {
			instance := d.Ingesters[other]
			instance.Addr = "moved"
			d.Ingesters[other] = instance
		}},
		"an instance changes zone": {
			change: func(d *ring.Desc) {
				instance := d.Ingesters[other]
				instance.Zone = "zone-2"
				d.Ingesters[other] = instance
			},
			changed: true,
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
			first := d.latest()
			require.NotNil(t, first, "the first ring is a change")

			next := cloneOwnershipTestDesc(desc)
			tc.change(next)
			d.OnRingInstanceHeartbeat(nil, next)

			assert.Equal(t, tc.changed, d.latest() != first)
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
	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))

	limits := &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: shardSize}}}
	overrides := validation.NewOverrides(validation.Limits{}, limits)
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	require.Equal(t, 1.0, testutil.ToFloat64(refresher.computations))

	// An instance outside the tenant's subring and one in it.
	members := map[string]bool{}
	healthy, unhealthy, err := refresher.ring.ShuffleShard(tenant, shardSize).GetAllInstanceDescs(ring.Reporting)
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
		refresher.refresh(context.Background(), []string{tenant})
		assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))
		return testutil.ToFloat64(refresher.computations) - before
	}

	built := refresher.ring
	assert.Zero(t, update(setOwnershipTestState(inside, ring.LEAVING)), "not READONLY: the ring has not changed")
	assert.Same(t, built, refresher.ring, "so it is not built again")
	assert.Zero(t, update(setOwnershipTestState(outside, ring.READONLY)), "the ring changed, the tenant's subring did not")
	assert.Equal(t, 1.0, update(setOwnershipTestState(inside, ring.READONLY)), "a member of the tenant's subring changed")

	limits.limits[tenant] = &validation.Limits{IngestionTenantShardSize: shardSize + 3}
	refresher.refresh(context.Background(), []string{tenant})
	assert.Equal(t, shardSize+3, refresher.tenants[tenant].shardSize)
	assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))

	refresher.refresh(context.Background(), nil)
	assert.Empty(t, refresher.tenants, "a tenant gone from the ingester is dropped")
}

// A neighbour going from LEAVING to READONLY moves this ingester's share,
// because a shard picks one more instance for each READONLY one it includes.
// On this layout the share goes from 0 to about 0.54.
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
	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, desc)
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	before := refresher.tenants[tenant].share

	next := cloneOwnershipTestDesc(desc)
	setOwnershipTestState(neighbour, ring.READONLY)(next)
	d.OnRingInstanceHeartbeat(nil, next)
	refresher.refresh(context.Background(), []string{tenant})

	fresh := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	fresh.refresh(context.Background(), []string{tenant})
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

	d, newRing := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	before := refresher.tenants[tenant].share

	next := cloneOwnershipTestDesc(desc)
	next.RemoveIngester(ownershipTestInstanceID(1, 0))
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(next))
	refresher.refresh(context.Background(), []string{tenant})

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

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
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
		before := d.latest()
		d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
		if d.latest() != before {
			bumps++
		}
		refresher.refresh(context.Background(), tenants)

		require.Equal(t, freshOwnershipShares(t, desc, me, overrides, tenants), comparableShares(refresher.tenants), "step %d", step)
	}
	// Not a correctness condition: shows the test exercises reuse.
	t.Logf("the ring changed on %d of %d steps; ownership computed %v times", bumps, steps, testutil.ToFloat64(refresher.computations))
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
// own goroutine. A callback must not compare or replace the kept desc while
// another holds the delegate's lock.
func TestRingOwnershipDelegate_KeepsTheDescUnderTheLock(t *testing.T) {
	const wait = 50 * time.Millisecond
	me := ownershipTestInstanceID(0, 0)
	neighbour := ownershipTestInstanceID(0, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 3, tokensPerInstance: 4}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	d, _ := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))

	readOnly := cloneOwnershipTestDesc(desc)
	setOwnershipTestState(neighbour, ring.READONLY)(readOnly)

	d.mtx.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.OnRingInstanceHeartbeat(nil, readOnly)
	}()
	time.Sleep(wait)
	assert.Equal(t, ring.ACTIVE, d.desc.Ingesters[neighbour].State, "the desc was kept while another callback held the lock")
	d.mtx.Unlock()

	<-done
	assert.Equal(t, ring.READONLY, d.latest().Ingesters[neighbour].State)
}

// freshOwnershipShares computes the shares from scratch on a new ring, so a bug
// in the reuse or in the ring's caches cannot show on both sides.
func freshOwnershipShares(t *testing.T, desc *ring.Desc, me string, overrides *validation.Overrides, tenants []string) map[string]tenantShare {
	t.Helper()
	d, newRing := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, desc)
	fresh := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	fresh.refresh(context.Background(), tenants)
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
		"the instance holding it in another zone leaves": {
			sharedFromStart: true,
			change:          func(d *ring.Desc) { d.RemoveIngester(collider) },
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
			d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
			refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
			refresher.refresh(context.Background(), tenants)
			before := refresher.tenants[tenant].share

			next := cloneOwnershipTestDesc(desc)
			tc.change(next)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(next))
			refresher.refresh(context.Background(), tenants)

			fresh := freshOwnershipShares(t, next, me, overrides, tenants)
			require.NotEqual(t, before, fresh[tenant].share, "the layout must make the change move the share")
			assert.Equal(t, fresh, comparableShares(refresher.tenants))
		})
	}
}

// hookedTenantLimits runs hook once, the first time ByUserID is called for
// user, standing in for a heartbeat that lands during a refresh.
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

// A heartbeat during a refresh changes the ring after the refresh started. The
// stored share must describe the ring the refresh read, so that the next
// refresh compares against the right one.
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

	d, newRing := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), nil)

	// During the refresh that first sees the tenant, an instance leaves zone-1,
	// so that ShuffleShard on the new ring sees 11 instances and returns the
	// whole ring. The refresh keeps reading the ring it started with.
	shrunk := cloneOwnershipTestDesc(desc)
	shrunk.RemoveIngester(ownershipTestInstanceID(1, 2))
	limits.hook = func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(shrunk)) }
	refresher.refresh(context.Background(), []string{tenant})
	require.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants), "the share comes from the ring the refresh started with")

	// An instance joins zone-2: 12 instances again.
	grown := cloneOwnershipTestDesc(shrunk)
	grown.AddIngester(ownershipTestInstanceID(2, 9), "zone-2-ingester-9-addr", "zone-2", []uint32{rnd.Uint32(), rnd.Uint32()}, ring.ACTIVE, time.Time{})
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(grown))
	refresher.refresh(context.Background(), []string{tenant})

	fresh := freshOwnershipShares(t, grown, me, overrides, []string{tenant})
	require.NotEqual(t, fresh[tenant].share, freshOwnershipShares(t, shrunk, me, overrides, []string{tenant})[tenant].share, "the layout must make the two rings differ")
	assert.Equal(t, fresh, comparableShares(refresher.tenants))
}

// The ring leaves excluded zones out, so changes there count as changes but
// leave every tenant's subring, and so its share, as it was.
func TestOwnershipRefresher_ExcludedZones(t *testing.T) {
	const tenant = "tenant"
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 4, perZone: 3, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	overrides := validation.NewOverrides(validation.Limits{}, &staticTenantLimits{limits: map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}})

	d, newRing := newOwnershipTestDelegate(t, me, 3, "zone-3")
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	require.Equal(t, 3, refresher.tenants[tenant].zones)
	computed := testutil.ToFloat64(refresher.computations)

	setOwnershipTestState(ownershipTestInstanceID(3, 0), ring.READONLY)(desc)
	desc.RemoveIngester(ownershipTestInstanceID(3, 1))
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh(context.Background(), []string{tenant})

	assert.Equal(t, computed, testutil.ToFloat64(refresher.computations))
	fresh, freshRing := newOwnershipTestDelegate(t, me, 3, "zone-3")
	fresh.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	want := newOwnershipRefresher(fresh.latest, freshRing, me, true, overrides, nil)
	want.refresh(context.Background(), []string{tenant})
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

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	before := refresher.tenants[tenant]

	instance := desc.Ingesters[collider]
	instance.Tokens = append(slices.Clone(instance.Tokens), token)
	slices.Sort(instance.Tokens)
	desc.Ingesters[collider] = instance
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh(context.Background(), []string{tenant})

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
			d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
			refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
			refresher.refresh(context.Background(), tenants)
			before := refresher.tenants[tenant].share

			shared := cloneOwnershipTestDesc(desc)
			instance := shared.Ingesters[collider]
			instance.Tokens = append(slices.Clone(instance.Tokens), token)
			slices.Sort(instance.Tokens)
			shared.Ingesters[collider] = instance
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(shared))
			refresher.refresh(context.Background(), tenants)
			require.True(t, refresher.ring.HasDuplicateTokens())
			require.NotEqual(t, before, refresher.tenants[tenant].share, "the layout must make sharing move the share")

			after := cloneOwnershipTestDesc(desc)
			end(after)
			d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(after))
			refresher.refresh(context.Background(), tenants)
			require.False(t, refresher.ring.HasDuplicateTokens())

			assert.Equal(t, freshOwnershipShares(t, after, me, overrides, tenants), comparableShares(refresher.tenants))
		})
	}
}

// A heartbeat can change the ring during a refresh. If it changes after the
// whole-ring fingerprint is taken and is back as it was by the next refresh, the
// fingerprint matches again, so the share stored with it must not come from the
// changed ring.
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

	readOnly := readOnlyNeighbour(t, desc, me, layout.perZone, overrides, first)

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)

	// The neighbour becomes READONLY after the first whole-ring tenant, and
	// is back to ACTIVE before the next refresh.
	limits.arm(second, func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(readOnly)) })
	refresher.refresh(context.Background(), tenants)
	back := cloneOwnershipTestDesc(desc)
	for id, instance := range back.Ingesters {
		instance.Timestamp += 5
		back.Ingesters[id] = instance
	}
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(back))
	refresher.refresh(context.Background(), tenants)

	assert.Equal(t, freshOwnershipShares(t, back, me, overrides, tenants), comparableShares(refresher.tenants))
}

// Randomized: each step a new ring replaces the kept desc, so the next refresh
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

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
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
		refresher.refresh(context.Background(), tenants)
		heartbeat(desc)
		refresher.refresh(context.Background(), tenants)

		require.Equal(t, freshOwnershipShares(t, desc, me, overrides, tenants), comparableShares(refresher.tenants), "step %d", step)
	}
}

func TestInstanceHasher(t *testing.T) {
	instance := func(zone string, state ring.InstanceState, tokens ...uint32) ring.InstanceDesc {
		return ring.InstanceDesc{Zone: zone, State: state, Tokens: tokens}
	}
	var h instanceHasher
	base := h.hash(instance("zone-a", ring.ACTIVE, 1, 2, 3))

	assert.Equal(t, base, h.hash(instance("zone-a", ring.ACTIVE, 1, 2, 3)), "the buffer is reused between calls")
	for name, other := range map[string]ring.InstanceDesc{
		"zone":     instance("zone-b", ring.ACTIVE, 1, 2, 3),
		"READONLY": instance("zone-a", ring.READONLY, 1, 2, 3),
		"tokens":   instance("zone-a", ring.ACTIVE, 1, 2, 4),
		"no token": instance("zone-a", ring.ACTIVE, 1, 2),
	} {
		assert.NotEqual(t, base, h.hash(other), name)
	}
	assert.Equal(t, base, h.hash(instance("zone-a", ring.LEAVING, 1, 2, 3)), "states other than READONLY do not count")
	// The zone's length is hashed, so a zone cannot run into the tokens: without
	// it, both of these would hash "a", then five zero bytes.
	assert.NotEqual(t, h.hash(instance("a", ring.ACTIVE, 0)), h.hash(instance("a\x00\x00\x00\x00", ring.ACTIVE)))
}

// Two identical tokenless instances join a fourth zone, so the whole ring has
// four zones and the share must be left out. A fingerprint that lets identical
// members cancel, as XOR would, misses it.
func TestOwnershipRefresher_IdenticalMembersDoNotCancel(t *testing.T) {
	const tenant = "whole"
	me := ownershipTestInstanceID(0, 0)
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16})
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	d, newRing := newOwnershipTestDelegate(t, me, 3)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), []string{tenant})
	_, ok := refresher.ownership.share(tenant)
	require.True(t, ok)

	next := cloneOwnershipTestDesc(desc)
	for _, id := range []string{"zone-3-a", "zone-3-b"} {
		next.AddIngester(id, id+"-addr", "zone-3", nil, ring.PENDING, time.Time{})
	}
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(next))
	refresher.refresh(context.Background(), []string{tenant})

	_, ok = refresher.ownership.share(tenant)
	assert.False(t, ok, "four zones: the share must not be used")
}

// A refresh stopped part-way, as on shutdown, leaves the shares as they were,
// and the next refresh checks every tenant.
func TestOwnershipRefresher_StoppedRefreshChangesNothing(t *testing.T) {
	const tenant = "whole"
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	tenants := []string{tenant}
	overrides := validation.NewOverrides(validation.Limits{}, nil)

	changed := readOnlyNeighbour(t, desc, me, layout.perZone, overrides, tenant)

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), tenants)
	before := maps.Clone(refresher.tenants)

	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(changed))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	refresher.refresh(ctx, tenants)
	assert.Equal(t, before, refresher.tenants)

	refresher.refresh(context.Background(), tenants)
	assert.Equal(t, freshOwnershipShares(t, changed, me, overrides, tenants), comparableShares(refresher.tenants))
}

// A token becomes shared during a refresh and stops being shared before the
// next one. No share may stay computed from the ring with it shared.
func TestOwnershipRefresher_TokenSharedDuringARefresh(t *testing.T) {
	const (
		tenant = "tenant-1"
		token  = uint32(1036751706)
	)
	me := ownershipTestInstanceID(0, 0)
	collider := ownershipTestInstanceID(1, 1)
	layout := ownershipTestRingLayout{zones: 3, perZone: 6, tokensPerInstance: 8}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	require.Contains(t, desc.Ingesters[me].Tokens, token)
	// The collider, outside the tenant's shard, takes one of my tokens.
	shared := cloneOwnershipTestDesc(desc)
	in := shared.Ingesters[collider]
	in.Tokens = append(slices.Clone(in.Tokens), token)
	slices.Sort(in.Tokens)
	shared.Ingesters[collider] = in

	limits := &hookedTenantLimits{}
	limits.limits = map[string]*validation.Limits{tenant: {IngestionTenantShardSize: 6}}
	overrides := validation.NewOverrides(validation.Limits{}, limits)
	require.NotEqual(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), freshOwnershipShares(t, shared, me, overrides, []string{tenant}))

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.refresh(context.Background(), nil)
	limits.arm(tenant, func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(shared)) })
	refresher.refresh(context.Background(), []string{tenant})

	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh(context.Background(), []string{tenant})
	assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))
}

// readOnlyNeighbour returns desc with a zone-0 neighbour of me made READONLY,
// one whose change moves me's share of tenant.
func readOnlyNeighbour(t *testing.T, desc *ring.Desc, me string, perZone int, overrides *validation.Overrides, tenant string) *ring.Desc {
	t.Helper()
	baseline := freshOwnershipShares(t, desc, me, overrides, []string{tenant})[tenant].share
	for i := 1; i < perZone; i++ {
		changed := cloneOwnershipTestDesc(desc)
		setOwnershipTestState(ownershipTestInstanceID(0, i), ring.READONLY)(changed)
		if freshOwnershipShares(t, changed, me, overrides, []string{tenant})[tenant].share != baseline {
			return changed
		}
	}
	t.Fatal("no neighbour moves the share")
	return nil
}

// hookedCounter runs hook once before counting, standing in for a heartbeat
// that lands while a share is computed.
type hookedCounter struct {
	prometheus.Counter
	hook func()
}

func (c *hookedCounter) Inc() {
	if hook := c.hook; hook != nil {
		c.hook = nil
		hook()
	}
	c.Counter.Inc()
}

// A heartbeat lands while the whole ring's share is computed, after its
// fingerprint was read, and is undone before the next refresh. The share and
// the fingerprint stored with it must come from the same ring.
func TestOwnershipRefresher_WholeRingChangesDuringItsComputation(t *testing.T) {
	const tenant = "whole"
	me := ownershipTestInstanceID(0, 0)
	layout := ownershipTestRingLayout{zones: 3, perZone: 4, tokensPerInstance: 16}
	desc := newOwnershipTestDesc(rand.New(rand.NewSource(ownershipTestSeed)), layout)
	overrides := validation.NewOverrides(validation.Limits{}, nil)
	changed := readOnlyNeighbour(t, desc, me, layout.perZone, overrides, tenant)

	d, newRing := newOwnershipTestDelegate(t, me, layout.zones)
	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher := newOwnershipRefresher(d.latest, newRing, me, true, overrides, nil)
	refresher.computations = &hookedCounter{
		Counter: refresher.computations,
		hook:    func() { d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(changed)) },
	}
	refresher.refresh(context.Background(), []string{tenant})

	d.OnRingInstanceHeartbeat(nil, cloneOwnershipTestDesc(desc))
	refresher.refresh(context.Background(), []string{tenant})
	assert.Equal(t, freshOwnershipShares(t, desc, me, overrides, []string{tenant}), comparableShares(refresher.tenants))
}
