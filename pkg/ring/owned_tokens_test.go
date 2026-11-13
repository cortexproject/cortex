package ring

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownershipTopologies is the matrix of ring shapes that ownership must be
// correct for. The zone-aware RF==numZones case is the one that happens to work
// with a "first token in my zone wins" ownership check, because there the ring
// picks exactly one instance per zone. Every other shape in this table does not,
// which is the point of the table.
var ownershipTopologies = map[string]struct {
	numInstances         int
	numZones             int
	replicationFactor    int
	zoneAwarenessEnabled bool
}{
	// Zone awareness disabled. This is the Cortex default
	// (-distributor.zone-awareness-enabled defaults to false) and the shape the
	// ownership check must not get wrong.
	"zones disabled, RF=1":  {numInstances: 9, numZones: 3, replicationFactor: 1, zoneAwarenessEnabled: false},
	"zones disabled, RF=3":  {numInstances: 9, numZones: 3, replicationFactor: 3, zoneAwarenessEnabled: false},
	"zones disabled, RF=5":  {numInstances: 9, numZones: 3, replicationFactor: 5, zoneAwarenessEnabled: false},
	"single zone, RF=3":     {numInstances: 9, numZones: 1, replicationFactor: 3, zoneAwarenessEnabled: false},
	"zones disabled, RF=12": {numInstances: 12, numZones: 3, replicationFactor: 12, zoneAwarenessEnabled: false},

	// Zone awareness enabled.
	"zone aware, RF == numZones": {numInstances: 9, numZones: 3, replicationFactor: 3, zoneAwarenessEnabled: true},
	"zone aware, RF > numZones":  {numInstances: 9, numZones: 3, replicationFactor: 6, zoneAwarenessEnabled: true},
	"zone aware, RF < numZones":  {numInstances: 9, numZones: 3, replicationFactor: 2, zoneAwarenessEnabled: true},
	"zone aware, RF % numZones != 0": {
		numInstances: 12, numZones: 3, replicationFactor: 4, zoneAwarenessEnabled: true,
	},
	"zone aware, 2 zones RF=3": {numInstances: 8, numZones: 2, replicationFactor: 3, zoneAwarenessEnabled: true},
}

// newRingForOwnershipTest builds a Ring in the same way the rest of this
// package's tests do, with every instance ACTIVE and heartbeating so that the
// replication strategy's Filter is an order-preserving identity. That makes
// Ring.Get observable as the raw replica-set walk, which is what ownership is
// defined against.
func newRingForOwnershipTest(ringDesc *Desc, replicationFactor int, zoneAwarenessEnabled bool) *Ring {
	return &Ring{
		cfg: Config{
			HeartbeatTimeout:     time.Hour,
			ZoneAwarenessEnabled: zoneAwarenessEnabled,
			ReplicationFactor:    replicationFactor,
		},
		ringDesc:            ringDesc,
		ringTokens:          ringDesc.GetTokens(),
		ringTokensByZone:    ringDesc.getTokensByZone(),
		ringInstanceByToken: ringDesc.getTokensInfo(),
		ringZones:           getZones(ringDesc.getTokensByZone()),
		strategy:            NewDefaultReplicationStrategy(),
		KVClient:            &MockClient{},
	}
}

// keyForPosition returns a key that SearchToken maps to exactly position p, so
// that a per-position bitmap can be compared against a per-key Ring.Get.
func keyForPosition(tokens []uint32, p int) uint32 {
	if p == 0 {
		if tokens[0] > 0 {
			// No token is greater than tokens[0]-1 before index 0.
			return tokens[0] - 1
		}
		// tokens[0] == 0, so instead rely on SearchToken wrapping past the end.
		return math.MaxUint32
	}
	// tokens[p-1] is not greater than itself, but tokens[p] is, so the first
	// token strictly greater than this key sits at index p.
	return tokens[p-1]
}

// TestOwnedTokenPositions_MatchesGet is the load-bearing property for the whole
// owned-series feature: an instance owns a token position if and only if the
// ring would route a key in that position to it.
//
// If this test cannot be made to pass, then "the replica set is a pure function
// of the start index" is false and the owned-series design needs to change
// before anything is built on top of it.
func TestOwnedTokenPositions_MatchesGet(t *testing.T) {
	for testName, testData := range ownershipTopologies {
		t.Run(testName, func(t *testing.T) {
			ringDesc := &Desc{Ingesters: generateRingInstances(testData.numInstances, testData.numZones, 128)}
			ring := newRingForOwnershipTest(ringDesc, testData.replicationFactor, testData.zoneAwarenessEnabled)

			allTokens := ringDesc.GetTokens()
			require.NotEmpty(t, allTokens)

			// Compute each instance's ownership bitmap once, the way a real
			// caller would: once per ring change, not once per series.
			bitmaps := make(map[string][]bool, len(ringDesc.Ingesters))
			for instanceID := range ringDesc.Ingesters {
				tokens, owned, err := ring.OwnedTokenPositions(instanceID, Write)
				require.NoError(t, err)
				require.Equal(t, allTokens, tokens, "returned token list must be the ring's full sorted token list")
				require.Len(t, owned, len(allTokens), "bitmap must be parallel to the token list")
				bitmaps[instanceID] = owned
			}

			bufDescs, bufHosts, bufZones := MakeBuffersForGet()

			for p := range allTokens {
				key := keyForPosition(allTokens, p)
				require.Equal(t, p, SearchToken(allTokens, key), "test helper produced a key for the wrong position")

				set, err := ring.Get(key, Write, bufDescs, bufHosts, bufZones)
				require.NoError(t, err)

				for instanceID, instanceDesc := range ringDesc.Ingesters {
					// Compare via Addr because that is what ReplicationSet
					// matches on, but drive the lookup from the instance ID, so
					// this stays correct even when the two differ.
					assert.Equal(t, set.Includes(instanceDesc.Addr), bitmaps[instanceID][p],
						"ownership disagrees with routing at token position %d (token %d) for instance %s",
						p, allTokens[p], instanceID)
				}
			}
		})
	}
}

// TestOwnedTokenPositions_Conservation checks the aggregate invariant that makes
// the series accounting add up: summed across all instances, every token
// position is owned exactly replicationFactor times. This is what guarantees
// that the per-ingester owned counts sum to RF x the total series count, and so
// that comparing owned against a limit already scaled by RF is dimensionally
// correct.
func TestOwnedTokenPositions_Conservation(t *testing.T) {
	for testName, testData := range ownershipTopologies {
		t.Run(testName, func(t *testing.T) {
			ringDesc := &Desc{Ingesters: generateRingInstances(testData.numInstances, testData.numZones, 128)}
			allTokens := ringDesc.GetTokens()

			ownersPerPosition := make([]int, len(allTokens))
			for instanceID := range ringDesc.Ingesters {
				_, owned, err := newRingForOwnershipTest(ringDesc, testData.replicationFactor, testData.zoneAwarenessEnabled).OwnedTokenPositions(instanceID, Write)
				require.NoError(t, err)

				for p, isOwned := range owned {
					if isOwned {
						ownersPerPosition[p]++
					}
				}
			}

			for p, owners := range ownersPerPosition {
				assert.Equal(t, testData.replicationFactor, owners,
					"token position %d (token %d) is owned by %d instances, expected exactly RF=%d",
					p, allTokens[p], owners, testData.replicationFactor)
			}
		})
	}
}

// TestOwnedTokenPositions_EmptyRing documents the behaviour on a ring with no
// instances. Callers must get an empty bitmap rather than a panic, because the
// ingester asks this question during startup before the ring has been read.
func TestOwnedTokenPositions_EmptyRing(t *testing.T) {
	tokens, owned, err := newRingForOwnershipTest(NewDesc(), 3, false).OwnedTokenPositions("instance-1", Write)
	require.NoError(t, err)
	assert.Empty(t, tokens)
	assert.Empty(t, owned)
}

// TestOwnedTokenPositions_AddrDiffersFromInstanceID guards a bug that the shared
// test fixtures cannot catch on their own: generateRingInstance happens to set
// Addr equal to the instance ID, so an implementation which compared addresses
// instead of instance IDs would pass every other test in this file and then own
// nothing at all in a real ring, which is keyed by instance ID
// ("ingester-zone-a-0") while Addr is a host:port.
func TestOwnedTokenPositions_AddrDiffersFromInstanceID(t *testing.T) {
	const (
		numInstances      = 9
		numTokens         = 128
		replicationFactor = 3
	)

	// Build a ring where the instance ID and the Addr are deliberately unrelated.
	ringDesc := &Desc{Ingesters: map[string]InstanceDesc{}}
	g := NewRandomTokenGenerator()
	for i := 1; i <= numInstances; i++ {
		instanceID := fmt.Sprintf("ingester-zone-a-%d", i)
		addr := fmt.Sprintf("10.0.0.%d:9095", i)
		ringDesc.Ingesters[instanceID] = InstanceDesc{
			Addr:                addr,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Unix(),
			State:               ACTIVE,
			Tokens:              g.GenerateTokens(NewDesc(), instanceID, "", numTokens, true),
			Zone:                "",
		}
	}

	ring := newRingForOwnershipTest(ringDesc, replicationFactor, false)
	allTokens := ringDesc.GetTokens()

	bufDescs, bufHosts, bufZones := MakeBuffersForGet()
	totalOwned := 0

	for instanceID, instanceDesc := range ringDesc.Ingesters {
		tokens, owned, err := ring.OwnedTokenPositions(instanceID, Write)
		require.NoError(t, err)
		require.Equal(t, allTokens, tokens)

		for p := range allTokens {
			key := keyForPosition(allTokens, p)
			require.Equal(t, p, SearchToken(allTokens, key))

			set, err := ring.Get(key, Write, bufDescs, bufHosts, bufZones)
			require.NoError(t, err)

			assert.Equal(t, set.Includes(instanceDesc.Addr), owned[p],
				"ownership disagrees with routing at position %d for instance %s (addr %s)",
				p, instanceID, instanceDesc.Addr)

			if owned[p] {
				totalOwned++
			}
		}
	}

	// Sanity check that the test actually exercised ownership rather than
	// trivially agreeing on an all-false bitmap.
	assert.Equal(t, replicationFactor*len(allTokens), totalOwned)
}

// TestOwnedTokenPositions_NonActiveInstancesExtendReplicaSet pins down what
// ownership means while instances are transitioning, which is deliberately not the
// same as what the write path routes.
//
// An instance in a state other than ACTIVE extends the replica set, so the ring
// picks an additional instance for the affected token ranges. Both the
// transitioning instance and the extra one own those ranges, because both really
// are holding that data: the departing instance still has the series in its head,
// and the distributor really is writing to the extension.
//
// Ownership is computed from the raw walk and deliberately does not apply the
// replication strategy's health filter, so a LEAVING instance keeps owning its
// series even though Ring.Get for a write excludes it. That asymmetry is the point:
// ownership answers "what am I holding, and must therefore count against my
// limit", not "where would a new write go". Filtering here would make an
// instance's own series count depend on its peers' heartbeat luck.
func TestOwnedTokenPositions_NonActiveInstancesExtendReplicaSet(t *testing.T) {
	const (
		numInstances      = 9
		replicationFactor = 3
		transitioning     = "instance-1"
	)

	for _, state := range []InstanceState{JOINING, LEAVING, READONLY} {
		t.Run(state.String(), func(t *testing.T) {
			ringDesc := &Desc{Ingesters: generateRingInstances(numInstances, 1, 128)}

			instance := ringDesc.Ingesters[transitioning]
			instance.State = state
			ringDesc.Ingesters[transitioning] = instance

			ring := newRingForOwnershipTest(ringDesc, replicationFactor, false)
			allTokens := ringDesc.GetTokens()

			ownersPerPosition := make([]int, len(allTokens))
			var transitioningOwned []bool

			for instanceID := range ringDesc.Ingesters {
				_, owned, err := ring.OwnedTokenPositions(instanceID, Write)
				require.NoError(t, err)

				if instanceID == transitioning {
					transitioningOwned = owned
				}
				for p, isOwned := range owned {
					if isOwned {
						ownersPerPosition[p]++
					}
				}
			}

			// The transitioning instance still holds its series, so it still owns them.
			transitioningOwnedCount := 0
			for _, isOwned := range transitioningOwned {
				if isOwned {
					transitioningOwnedCount++
				}
			}
			assert.NotZero(t, transitioningOwnedCount,
				"a %s instance still holds its series and must still own them", state)

			bufDescs, bufHosts, bufZones := MakeBuffersForGet()
			extended := 0

			for p := range allTokens {
				// Extension means these ranges have more owners than the replication
				// factor, never fewer.
				assert.GreaterOrEqual(t, ownersPerPosition[p], replicationFactor,
					"token position %d has fewer owners than RF", p)
				if ownersPerPosition[p] > replicationFactor {
					extended++
				}

				key := keyForPosition(allTokens, p)
				set, err := ring.Get(key, Write, bufDescs, bufHosts, bufZones)
				require.NoError(t, err)

				// Writes still land on exactly RF healthy instances, and never on the
				// transitioning one, even where it owns the range.
				assert.Len(t, set.Instances, replicationFactor)
				assert.False(t, set.Includes(ringDesc.Ingesters[transitioning].Addr),
					"a %s instance must not receive writes at position %d", state, p)
			}

			assert.NotZero(t, extended,
				"a %s instance must cause the replica set to be extended somewhere", state)
		})
	}
}

// TestOwnershipFingerprint pins down the fingerprint's two jobs. It must change
// whenever ownership could have changed, because a false "unchanged" leaves every
// ingester using a stale ownership bitmap. It must NOT change on a heartbeat,
// because heartbeats are the overwhelmingly common ring update, and recomputing
// ownership each time would cost far more than the cheap per-series lookup saves.
//
// Rings are updated through updateRingState, as a watched ring is, because the
// fingerprint relies on the topology timestamp that updateRingState maintains.
func TestOwnershipFingerprint(t *testing.T) {
	base := func() *Desc {
		return &Desc{Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
		}}
	}
	newRing := func(t *testing.T) *Ring {
		r, err := NewWithStoreClientAndStrategy(Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 1}, testRingName, testRingKey, &MockClient{}, NewDefaultReplicationStrategy(), nil, log.NewNopLogger())
		require.NoError(t, err)
		r.updateRingState(base())
		return r
	}

	t.Run("is zero while the ring has no instances", func(t *testing.T) {
		r, err := NewWithStoreClientAndStrategy(Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 1}, testRingName, testRingKey, &MockClient{}, NewDefaultReplicationStrategy(), nil, log.NewNopLogger())
		require.NoError(t, err)
		assert.Zero(t, r.OwnershipFingerprint())
	})

	t.Run("is stable for an equal ring", func(t *testing.T) {
		r := newRing(t)
		before := r.OwnershipFingerprint()
		r.updateRingState(base())
		assert.Equal(t, before, r.OwnershipFingerprint())
	})

	t.Run("ignores heartbeats", func(t *testing.T) {
		r := newRing(t)
		before := r.OwnershipFingerprint()
		heartbeat := base()
		for id, instance := range heartbeat.Ingesters {
			instance.Timestamp = 9999
			heartbeat.Ingesters[id] = instance
		}
		r.updateRingState(heartbeat)
		assert.Equal(t, before, r.OwnershipFingerprint(), "a heartbeat must not force an ownership recompute")
	})

	// Each of these changes the replica set, so each must invalidate the bitmap.
	// The state case is the one Desc.RingCompare cannot distinguish from a
	// heartbeat, which is why states are hashed on top of the topology timestamp.
	for name, change := range map[string]func(d *Desc){
		"state change": func(d *Desc) {
			instance := d.Ingesters["instance-1"]
			instance.State = LEAVING
			d.Ingesters["instance-1"] = instance
		},
		"token change": func(d *Desc) {
			instance := d.Ingesters["instance-1"]
			instance.Tokens = []uint32{100, 301}
			d.Ingesters["instance-1"] = instance
		},
		"zone change": func(d *Desc) {
			instance := d.Ingesters["instance-1"]
			instance.Zone = "zone-c"
			d.Ingesters["instance-1"] = instance
		},
		"instance added": func(d *Desc) {
			d.Ingesters["instance-3"] = InstanceDesc{Addr: "10.0.0.3:9095", Zone: "zone-c", State: ACTIVE, Tokens: []uint32{500}, Timestamp: 1000}
		},
		"instance removed": func(d *Desc) {
			delete(d.Ingesters, "instance-2")
		},
		"tokens moved between instances": func(d *Desc) {
			one, two := d.Ingesters["instance-1"], d.Ingesters["instance-2"]
			one.Tokens, two.Tokens = []uint32{100, 300, 400}, []uint32{200}
			d.Ingesters["instance-1"], d.Ingesters["instance-2"] = one, two
		},
	} {
		t.Run("changes on "+name, func(t *testing.T) {
			r := newRing(t)
			before := r.OwnershipFingerprint()
			changed := base()
			change(changed)
			r.updateRingState(changed)
			assert.NotEqual(t, before, r.OwnershipFingerprint())
		})
	}
}

// BenchmarkOwnedTokenPositions measures the cost of the once-per-ring-change
// precompute. This is the price paid to make the per-series ownership check a
// binary search plus an array index, so it is expected to be large relative to a
// single Ring.Get and still negligible relative to the interval between ring
// changes.
func BenchmarkOwnedTokenPositions(b *testing.B) {
	const (
		numInstances      = 100
		numZones          = 3
		replicationFactor = 3
	)

	ringDesc := &Desc{Ingesters: generateRingInstances(numInstances, numZones, numTokens)}

	var instanceID string
	for id := range ringDesc.Ingesters {
		instanceID = id
		break
	}

	ring := newRingForOwnershipTest(ringDesc, replicationFactor, true)

	b.ReportAllocs()

	for b.Loop() {
		_, owned, err := ring.OwnedTokenPositions(instanceID, Write)
		if err != nil {
			b.Fatal(err)
		}
		if len(owned) == 0 {
			b.Fatal("expected a non-empty bitmap")
		}
	}
}

// TestOwnedTokenPositions_UnknownInstance documents that an instance which is
// not in the ring owns nothing. The ingester can be in this state briefly after
// being forgotten from the ring, and it must not then claim ownership of
// everything.
func TestOwnedTokenPositions_UnknownInstance(t *testing.T) {
	ringDesc := &Desc{Ingesters: generateRingInstances(6, 3, 128)}

	tokens, owned, err := newRingForOwnershipTest(ringDesc, 3, false).OwnedTokenPositions("instance-does-not-exist", Write)
	require.NoError(t, err)
	require.Len(t, owned, len(tokens))

	for p, isOwned := range owned {
		assert.False(t, isOwned, "unknown instance must not own token position %d", p)
	}
}

// newWatchedRingForOwnershipTest builds a Ring updated through updateRingState, as
// a ring watching the KV store is, so that the shuffle-shard subring cache and the
// topology timestamp behave as they do in a running ingester.
func newWatchedRingForOwnershipTest(t *testing.T, ringDesc *Desc, replicationFactor int, zoneAwarenessEnabled bool) *Ring {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: replicationFactor, ZoneAwarenessEnabled: zoneAwarenessEnabled}
	r, err := NewWithStoreClientAndStrategy(cfg, testRingName, testRingKey, &MockClient{}, NewDefaultReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	r.updateRingState(ringDesc)
	return r
}

// TestOwnedTokenPositions_SubringMatchesGet is MatchesGet for shuffle-sharded
// tenants: on the subring a tenant's writes are routed within, an instance owns a
// token position if and only if the subring routes a key in that position to it.
// Instances outside the subring own nothing for that tenant.
func TestOwnedTokenPositions_SubringMatchesGet(t *testing.T) {
	const (
		numInstances      = 30
		numZones          = 3
		replicationFactor = 3
	)

	for _, zoneAwarenessEnabled := range []bool{true, false} {
		r := newWatchedRingForOwnershipTest(t, &Desc{Ingesters: generateRingInstances(numInstances, numZones, 128)}, replicationFactor, zoneAwarenessEnabled)

		for _, shardSize := range []int{3, 6, 9, 15} {
			t.Run(fmt.Sprintf("zone awareness %v, shard size %d", zoneAwarenessEnabled, shardSize), func(t *testing.T) {
				subring, ok := r.ShuffleShard("tenant", shardSize).(*Ring)
				require.True(t, ok)
				require.Less(t, subring.InstancesCount(), numInstances, "the shard must be smaller than the ring for this test to mean anything")

				tokens := subring.ringTokens
				bufDescs, bufHosts, bufZones := MakeBuffersForGet()
				ownedPositions := 0

				for instanceID, instance := range r.ringDesc.Ingesters {
					subTokens, owned, err := subring.OwnedTokenPositions(instanceID, Write)
					require.NoError(t, err)
					require.Equal(t, tokens, subTokens)
					require.Len(t, owned, len(tokens))

					_, inShard := subring.ringDesc.Ingesters[instanceID]
					for p := range tokens {
						if !inShard {
							require.False(t, owned[p], "instance %s is not in the shard but owns position %d", instanceID, p)
							continue
						}

						set, err := subring.Get(keyForPosition(tokens, p), Write, bufDescs, bufHosts, bufZones)
						require.NoError(t, err)
						require.Equal(t, set.Includes(instance.Addr), owned[p],
							"ownership disagrees with subring routing at position %d for instance %s", p, instanceID)
						if owned[p] {
							ownedPositions++
						}
					}
				}

				require.Equal(t, replicationFactor*len(tokens), ownedPositions, "every subring position must be owned exactly RF times")
			})
		}
	}
}

// TestOwnedTokenPositions_SubringDiffersFromRing shows why sharded tenants need
// their own ownership: for a member of a tenant's shard, whole-ring ownership and
// subring ownership disagree about many keys, because the two rings pick different
// replica sets. Using whole-ring ownership for such a tenant miscounts its series.
func TestOwnedTokenPositions_SubringDiffersFromRing(t *testing.T) {
	r := newWatchedRingForOwnershipTest(t, &Desc{Ingesters: generateRingInstances(30, 3, 128)}, 3, true)
	subring, ok := r.ShuffleShard("tenant", 6).(*Ring)
	require.True(t, ok)

	var member string
	for id := range subring.ringDesc.Ingesters {
		member = id
		break
	}

	ringTokens, ringOwned, err := r.OwnedTokenPositions(member, Write)
	require.NoError(t, err)
	subTokens, subOwned, err := subring.OwnedTokenPositions(member, Write)
	require.NoError(t, err)

	disagreements := 0
	for key := uint32(0); key < math.MaxUint32-1<<20; key += 1 << 20 {
		if ringOwned[SearchToken(ringTokens, key)] != subOwned[SearchToken(subTokens, key)] {
			disagreements++
		}
	}
	require.Greater(t, disagreements, 0)
}

// TestOwnedTokenPositions_SubringSeesStateChanges checks that a state change in
// the full ring reaches subring ownership. A state change is not a topology
// change, so cached subrings are kept, but the cache refreshes their states on
// every lookup. A member which is not ACTIVE extends the subring's replica sets,
// so some positions then have more than RF owners.
func TestOwnedTokenPositions_SubringSeesStateChanges(t *testing.T) {
	const replicationFactor = 3

	ringDesc := &Desc{Ingesters: generateRingInstances(30, 3, 128)}
	r := newWatchedRingForOwnershipTest(t, ringDesc, replicationFactor, true)
	subring := r.ShuffleShard("tenant", 6).(*Ring)

	var leaving string
	for id := range subring.ringDesc.Ingesters {
		leaving = id
		break
	}

	changed := &Desc{Ingesters: map[string]InstanceDesc{}}
	for id, instance := range ringDesc.Ingesters {
		if id == leaving {
			instance.State = LEAVING
		}
		changed.Ingesters[id] = instance
	}
	r.updateRingState(changed)

	subring = r.ShuffleShard("tenant", 6).(*Ring)
	ownersPerPosition := make([]int, len(subring.ringTokens))
	for id := range subring.ringDesc.Ingesters {
		_, owned, err := subring.OwnedTokenPositions(id, Write)
		require.NoError(t, err)
		for p, isOwned := range owned {
			if isOwned {
				ownersPerPosition[p]++
			}
		}
	}

	extended := 0
	for _, owners := range ownersPerPosition {
		require.GreaterOrEqual(t, owners, replicationFactor)
		if owners > replicationFactor {
			extended++
		}
	}
	require.Greater(t, extended, 0, "a LEAVING member must extend the subring's replica sets")
}

// TestOwnedTokenPositions_WriteNoExtendDoesNotExtend checks ownership for a
// distributor running with -distributor.extend-writes=false, which routes with
// WriteNoExtend: a non-ACTIVE instance does not extend the replica set, so every
// position is owned by exactly RF instances, unlike with Write.
func TestOwnedTokenPositions_WriteNoExtendDoesNotExtend(t *testing.T) {
	const replicationFactor = 3
	ringDesc := &Desc{Ingesters: generateRingInstances(9, 1, 128)}
	leaving := ringDesc.Ingesters["instance-1"]
	leaving.State = LEAVING
	ringDesc.Ingesters["instance-1"] = leaving
	ring := newRingForOwnershipTest(ringDesc, replicationFactor, false)

	owners := func(op Operation) []int {
		perPosition := make([]int, len(ring.ringTokens))
		for id := range ringDesc.Ingesters {
			_, owned, err := ring.OwnedTokenPositions(id, op)
			require.NoError(t, err)
			for p, isOwned := range owned {
				if isOwned {
					perPosition[p]++
				}
			}
		}
		return perPosition
	}

	extended := 0
	for p, n := range owners(Write) {
		require.GreaterOrEqual(t, n, replicationFactor, "position %d", p)
		if n > replicationFactor {
			extended++
		}
	}
	require.Greater(t, extended, 0, "Write extends past the LEAVING instance")

	for p, n := range owners(WriteNoExtend) {
		require.Equal(t, replicationFactor, n, "WriteNoExtend owns position %d exactly RF times", p)
	}
}
