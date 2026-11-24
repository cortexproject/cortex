package ring

import (
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	quarterOfRing        = uint32(1) << 30
	ownershipTolerance   = 1e-9
	routingSampleKeys    = 200_000
	routingTolerance     = 0.005
	ownershipTestSeed    = 42
	ownershipTestTimeout = time.Minute
)

type ownershipTestInstance struct {
	id     string
	zone   string
	tokens []uint32
	state  InstanceState
	stale  bool
}

func newOwnershipTestDesc(instances []ownershipTestInstance) *Desc {
	desc := NewDesc()
	for _, inst := range instances {
		ing := desc.AddIngester(inst.id, inst.id+"-addr", inst.zone, inst.tokens, inst.state, time.Time{})
		if inst.stale {
			ing.Timestamp = 0
			desc.Ingesters[inst.id] = ing
		}
	}
	return desc
}

func newOwnershipTestRing(t *testing.T, replicationFactor int, desc *Desc) *Ring {
	t.Helper()
	cfg := Config{
		HeartbeatTimeout:     ownershipTestTimeout,
		ReplicationFactor:    replicationFactor,
		ZoneAwarenessEnabled: true,
	}
	r, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, &MockClient{}, NewDefaultReplicationStrategy(), prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	r.updateRingState(desc)
	return r
}

// Zone a: a1 owns (0, q], a2 owns (q, 2q], a3 owns (2q, 0] (wraps), so 1/4, 1/4, 1/2.
// Zone b: b1 owns (5q/2, q/2] (wraps), b2 owns (q/2, 5q/2], so 1/2 each.
func handPickedInstances(override map[string]func(*ownershipTestInstance)) []ownershipTestInstance {
	instances := []ownershipTestInstance{
		{id: "a1", zone: "a", tokens: []uint32{quarterOfRing}, state: ACTIVE},
		{id: "a2", zone: "a", tokens: []uint32{2 * quarterOfRing}, state: ACTIVE},
		{id: "a3", zone: "a", tokens: []uint32{0}, state: ACTIVE},
		{id: "b1", zone: "b", tokens: []uint32{quarterOfRing / 2}, state: ACTIVE},
		{id: "b2", zone: "b", tokens: []uint32{5 * (quarterOfRing / 2)}, state: ACTIVE},
	}
	for i := range instances {
		if f, ok := override[instances[i].id]; ok {
			f(&instances[i])
		}
	}
	return instances
}

func withState(s InstanceState) func(*ownershipTestInstance) {
	return func(i *ownershipTestInstance) { i.state = s }
}

func TestRing_ZonalOwnership_HandPickedTokens(t *testing.T) {
	balancedZoneB := map[string]float64{"b1": 0.5, "b2": 0.5}

	tests := map[string]struct {
		override map[string]func(*ownershipTestInstance)
		expected map[string]float64
	}{
		"all ACTIVE": {
			expected: map[string]float64{"a1": 0.25, "a2": 0.25, "a3": 0.5},
		},
		"JOINING instance keeps its range": {
			override: map[string]func(*ownershipTestInstance){"a2": withState(JOINING)},
			expected: map[string]float64{"a1": 0.25, "a2": 0.25, "a3": 0.5},
		},
		"LEAVING and PENDING instances keep their ranges": {
			override: map[string]func(*ownershipTestInstance){"a1": withState(LEAVING), "a2": withState(PENDING)},
			expected: map[string]float64{"a1": 0.25, "a2": 0.25, "a3": 0.5},
		},
		"READONLY instance is left out": {
			override: map[string]func(*ownershipTestInstance){"a2": withState(READONLY)},
			expected: map[string]float64{"a1": 0.25, "a2": 0, "a3": 0.75},
		},
		"only one instance left in the zone owns all of it": {
			override: map[string]func(*ownershipTestInstance){"a1": withState(READONLY), "a2": withState(READONLY)},
			expected: map[string]float64{"a1": 0, "a2": 0, "a3": 1},
		},
		"every instance in the zone left out": {
			override: map[string]func(*ownershipTestInstance){
				"a1": withState(READONLY), "a2": withState(READONLY), "a3": withState(READONLY),
			},
			expected: map[string]float64{"a1": 0, "a2": 0, "a3": 0},
		},
		"ACTIVE instance with a stale heartbeat keeps its range": {
			override: map[string]func(*ownershipTestInstance){"a2": func(i *ownershipTestInstance) { i.stale = true }},
			expected: map[string]float64{"a1": 0.25, "a2": 0.25, "a3": 0.5},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(tc.override)))

			got := r.ZonalOwnership()

			expected := map[string]float64{}
			maps.Copy(expected, balancedZoneB)
			maps.Copy(expected, tc.expected)
			require.Len(t, got, len(expected))
			for id, want := range expected {
				assert.InDelta(t, want, got[id], ownershipTolerance, "instance %s", id)
			}
		})
	}
}

func TestRing_ZonalOwnership_DuplicateTokenOwnsNoExtraRange(t *testing.T) {
	desc := newOwnershipTestDesc([]ownershipTestInstance{
		{id: "a1", zone: "a", tokens: []uint32{quarterOfRing, quarterOfRing}, state: ACTIVE},
		{id: "a2", zone: "a", tokens: []uint32{3 * quarterOfRing}, state: ACTIVE},
	})
	r := newOwnershipTestRing(t, 1, desc)

	got := r.ZonalOwnership()

	assert.InDelta(t, 0.5, got["a1"], ownershipTolerance)
	assert.InDelta(t, 0.5, got["a2"], ownershipTolerance)
}

// A token shared across zones maps to one instance ring-wide. Get skips it in
// the other zone, whose range then falls to that zone's next token.
func TestRing_ZonalOwnership_TokenSharedAcrossZones(t *testing.T) {
	q := uint32(quarterOfRing)
	// b1 sorts after a2, so the ring-wide token map gives 2q to b1.
	desc := newOwnershipTestDesc([]ownershipTestInstance{
		{id: "a1", zone: "a", tokens: []uint32{q}, state: ACTIVE},
		{id: "a2", zone: "a", tokens: []uint32{2 * q}, state: ACTIVE},
		{id: "a3", zone: "a", tokens: []uint32{3 * q}, state: ACTIVE},
		{id: "b1", zone: "b", tokens: []uint32{2 * q}, state: ACTIVE},
		{id: "b2", zone: "b", tokens: []uint32{0}, state: ACTIVE},
	})
	got := newOwnershipTestRing(t, 2, desc).ZonalOwnership()

	expected := map[string]float64{"a1": 0.5, "a2": 0, "a3": 0.5, "b1": 0.5, "b2": 0.5}
	for id, share := range expected {
		assert.InDelta(t, share, got[id], ownershipTolerance, id)
	}
}

// randomOwnershipInstances builds a ring with random unique tokens. stateOf
// gives the state of instance i in zone z; nil means every instance is ACTIVE.
func randomOwnershipInstances(rnd *rand.Rand, zones, perZone, tokensPerInstance int, stateOf func(z, i int) InstanceState) []ownershipTestInstance {
	used := map[uint32]struct{}{}
	var instances []ownershipTestInstance
	for z := range zones {
		for i := range perZone {
			tokens := make([]uint32, 0, tokensPerInstance)
			for len(tokens) < tokensPerInstance {
				tok := rnd.Uint32()
				if _, dup := used[tok]; dup {
					continue
				}
				used[tok] = struct{}{}
				tokens = append(tokens, tok)
			}
			state := ACTIVE
			if stateOf != nil {
				state = stateOf(z, i)
			}
			instances = append(instances, ownershipTestInstance{
				id:     fmt.Sprintf("zone-%d-instance-%d", z, i),
				zone:   fmt.Sprintf("zone-%d", z),
				tokens: tokens,
				state:  state,
			})
		}
	}
	return instances
}

// joiningAndReadOnly puts the first instance of zone 0 in JOINING and the first
// instance of zone 1 in READONLY. Keeping them in different zones means every
// key still has a write quorum, with or without replica set extension.
func joiningAndReadOnly(z, i int) InstanceState {
	switch {
	case z == 0 && i == 0:
		return JOINING
	case z == 1 && i == 0:
		return READONLY
	default:
		return ACTIVE
	}
}

func TestRing_ZonalOwnership_SumsToOnePerZone(t *testing.T) {
	const (
		zones             = 3
		perZone           = 10
		tokensPerInstance = 128
	)
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	instances := randomOwnershipInstances(rnd, zones, perZone, tokensPerInstance, joiningAndReadOnly)
	r := newOwnershipTestRing(t, zones, newOwnershipTestDesc(instances))

	got := r.ZonalOwnership()

	sumByZone := map[string]float64{}
	for _, inst := range instances {
		if inst.state == READONLY {
			assert.Zero(t, got[inst.id], "instance %s is %s", inst.id, inst.state)
		}
		sumByZone[inst.zone] += got[inst.id]
	}
	require.Len(t, sumByZone, zones)
	for zone, sum := range sumByZone {
		assert.InDelta(t, 1, sum, ownershipTolerance, "zone %s", zone)
	}
}

// The ownership must describe where Get sends keys without replica set
// extension: the fraction of random keys Get routes to an ACTIVE instance is
// its zonal ownership.
func TestRing_ZonalOwnership_MatchesWriteRouting(t *testing.T) {
	const (
		zones             = 3
		perZone           = 6
		tokensPerInstance = 16
	)
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	instances := randomOwnershipInstances(rnd, zones, perZone, tokensPerInstance, joiningAndReadOnly)
	r := newOwnershipTestRing(t, zones, newOwnershipTestDesc(instances))
	ownership := r.ZonalOwnership()

	idByAddr := map[string]string{}
	for _, inst := range instances {
		idByAddr[inst.id+"-addr"] = inst.id
	}

	received := map[string]int{}
	bufDescs, bufHosts, bufZones := MakeBuffersForGet()
	for range routingSampleKeys {
		set, err := r.Get(rnd.Uint32(), WriteNoExtend, bufDescs, bufHosts, bufZones)
		require.NoError(t, err)
		for _, ing := range set.Instances {
			received[idByAddr[ing.Addr]]++
		}
	}

	for _, inst := range instances {
		fraction := float64(received[inst.id]) / routingSampleKeys
		if inst.state != ACTIVE {
			// Filter never sends writes to non-ACTIVE instances, whatever
			// ownership says about the range they hold.
			assert.Zero(t, received[inst.id], "instance %s is %s", inst.id, inst.state)
			continue
		}
		assert.InDelta(t, ownership[inst.id], fraction, routingTolerance, "instance %s", inst.id)
	}
}

func TestRing_ZonalOwnership_Cached(t *testing.T) {
	r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(nil)))

	first := r.ZonalOwnership()
	second := r.ZonalOwnership()

	assert.Equal(t, reflect.ValueOf(first).Pointer(), reflect.ValueOf(second).Pointer())
}

func TestRing_ZonalOwnership_InvalidatedOnReadOnlyChange(t *testing.T) {
	r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(nil)))
	require.InDelta(t, 0.25, r.ZonalOwnership()["a2"], ownershipTolerance)

	// Same tokens, different state: updateRingState takes its fast path.
	r.updateRingState(newOwnershipTestDesc(handPickedInstances(map[string]func(*ownershipTestInstance){"a2": withState(READONLY)})))

	got := r.ZonalOwnership()
	assert.Zero(t, got["a2"])
	assert.InDelta(t, 0.75, got["a3"], ownershipTolerance)
}

func TestRing_ZonalOwnership_KeptOnOtherStateChangesAndHeartbeats(t *testing.T) {
	r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(nil)))
	first := reflect.ValueOf(r.ZonalOwnership()).Pointer()

	desc := newOwnershipTestDesc(handPickedInstances(map[string]func(*ownershipTestInstance){"a2": withState(LEAVING)}))
	for id, instance := range desc.Ingesters {
		instance.Timestamp++
		desc.Ingesters[id] = instance
	}
	r.updateRingState(desc)

	assert.Equal(t, first, reflect.ValueOf(r.ZonalOwnership()).Pointer())
}

func TestRing_ZonalOwnership_InvalidatedOnTokenChange(t *testing.T) {
	r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(nil)))
	require.InDelta(t, 0.25, r.ZonalOwnership()["a1"], ownershipTolerance)

	// Moving a1's token from q to q/4 shrinks its range to 1/16 and grows a2's.
	r.updateRingState(newOwnershipTestDesc(handPickedInstances(map[string]func(*ownershipTestInstance){
		"a1": func(i *ownershipTestInstance) { i.tokens = []uint32{quarterOfRing / 4} },
	})))

	got := r.ZonalOwnership()
	assert.InDelta(t, 0.0625, got["a1"], ownershipTolerance)
	assert.InDelta(t, 0.4375, got["a2"], ownershipTolerance)
}

func TestRing_ZonalOwnership_Subrings(t *testing.T) {
	const (
		zones             = 2
		perZone           = 4
		tokensPerInstance = 32
		shardSize         = 4
		tenant            = "tenant"
	)
	rnd := rand.New(rand.NewSource(ownershipTestSeed))
	instances := randomOwnershipInstances(rnd, zones, perZone, tokensPerInstance, nil)
	r := newOwnershipTestRing(t, zones, newOwnershipTestDesc(instances))

	subring := r.ShuffleShard(tenant, shardSize)
	subOwnership := subring.(ZonalOwnershipRing).ZonalOwnership()
	require.Len(t, subOwnership, shardSize)

	sumByZone := map[string]float64{}
	var target string
	for _, inst := range instances {
		share, inShard := subOwnership[inst.id]
		if !inShard {
			continue
		}
		sumByZone[inst.zone] += share
		if target == "" {
			target = inst.id
		}
	}
	for zone, sum := range sumByZone {
		assert.InDelta(t, 1, sum, ownershipTolerance, "zone %s", zone)
	}
	require.Positive(t, subOwnership[target])

	setTarget := func(state InstanceState) {
		for i := range instances {
			if instances[i].id == target {
				instances[i].state = state
			}
		}
		r.updateRingState(newOwnershipTestDesc(instances))
	}

	// A state change other than READONLY keeps the cached subring and its
	// ownership.
	setTarget(LEAVING)
	cached := r.ShuffleShard(tenant, shardSize)
	require.Same(t, subring, cached)
	assert.Equal(t, reflect.ValueOf(subOwnership).Pointer(), reflect.ValueOf(cached.(ZonalOwnershipRing).ZonalOwnership()).Pointer())

	// READONLY resets the subring cache, and the new subring leaves it out.
	setTarget(READONLY)
	rebuilt := r.ShuffleShard(tenant, shardSize)
	require.NotSame(t, subring, rebuilt)
	assert.Zero(t, rebuilt.(ZonalOwnershipRing).ZonalOwnership()[target])
}

func TestRing_HasDuplicateTokens(t *testing.T) {
	tests := map[string]struct {
		instances []ownershipTestInstance
		expected  bool
	}{
		"no shared token": {
			instances: []ownershipTestInstance{
				{id: "a1", zone: "a", tokens: []uint32{1, 2}, state: ACTIVE},
				{id: "b1", zone: "b", tokens: []uint32{3}, state: ACTIVE},
			},
		},
		"a token shared across zones": {
			instances: []ownershipTestInstance{
				{id: "a1", zone: "a", tokens: []uint32{1, 2}, state: ACTIVE},
				{id: "b1", zone: "b", tokens: []uint32{2}, state: ACTIVE},
			},
			expected: true,
		},
		"a token shared within a zone": {
			instances: []ownershipTestInstance{
				{id: "a1", zone: "a", tokens: []uint32{1}, state: ACTIVE},
				{id: "a2", zone: "a", tokens: []uint32{1}, state: ACTIVE},
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			r := newOwnershipTestRing(t, 1, newOwnershipTestDesc(tc.instances))
			assert.Equal(t, tc.expected, r.HasDuplicateTokens())
		})
	}
}

// Callers tell whether ShuffleShard used the whole ring by comparing the result
// with the ring itself, so it must return the receiver, not a copy.
func TestRing_ShuffleShardReturnsTheRingItselfWhenItCoversTheRing(t *testing.T) {
	r := newOwnershipTestRing(t, 2, newOwnershipTestDesc(handPickedInstances(nil)))
	instances := r.InstancesCount()

	for _, size := range []int{-1, 0, instances, instances + 1} {
		assert.Same(t, r, r.ShuffleShard("tenant", size), "shard size %d", size)
	}
	assert.NotSame(t, r, r.ShuffleShard("tenant", instances-1))
}
