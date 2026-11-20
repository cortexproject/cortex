package ingester

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
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

	ownership := r.ZonalOwnership(ring.Write)
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
	refresher := newOwnershipRefresher(r, ingesterID, true, true, overrides)

	// Nothing is known before the ring has been fed.
	refresher.refresh([]string{shuffledTenant, unshuffledTenant})
	_, ok := refresher.ownership.share(unshuffledTenant)
	assert.False(t, ok)

	r.UpdateDesc(desc)
	refresher.refresh([]string{shuffledTenant, unshuffledTenant})

	share, ok := refresher.ownership.share(unshuffledTenant)
	require.True(t, ok)
	assert.InDelta(t, r.ZonalOwnership(ring.Write)[ingesterID], share, ownershipTestTolerance)

	subOwnership := r.ShuffleShard(shuffledTenant, shardSize).(ring.ZonalOwnershipRing).ZonalOwnership(ring.Write)
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

	refresher := newOwnershipRefresher(r, ownershipTestInstanceID(0, 0), true, false, validation.NewOverrides(validation.Limits{}, nil))
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

			refresher := newOwnershipRefresher(r, id, true, false, overrides)
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
