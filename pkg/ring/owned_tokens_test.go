package ring

import (
	"fmt"
	"math"
	"testing"
	"time"

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
				tokens, owned, err := OwnedTokenPositions(ringDesc, instanceID, Write, testData.replicationFactor, testData.zoneAwarenessEnabled)
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
				_, owned, err := OwnedTokenPositions(ringDesc, instanceID, Write, testData.replicationFactor, testData.zoneAwarenessEnabled)
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
	tokens, owned, err := OwnedTokenPositions(NewDesc(), "instance-1", Write, 3, false)
	require.NoError(t, err)
	assert.Empty(t, tokens)
	assert.Empty(t, owned)
}

// TestOwnedTokenPositions_AddrDiffersFromInstanceID guards a bug that the shared
// test fixtures cannot catch on their own: generateRingInstance happens to set
// Addr equal to the instance ID, so an implementation which compared addresses
// instead of instance IDs would pass every other test in this file and then own
// nothing at all in production, where a ring is keyed by instance ID
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
		tokens, owned, err := OwnedTokenPositions(ringDesc, instanceID, Write, replicationFactor, false)
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
				_, owned, err := OwnedTokenPositions(ringDesc, instanceID, Write, replicationFactor, false)
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
// because heartbeats are the overwhelmingly common reason the lifecycler reruns
// this, and recomputing ownership each time would cost far more than the cheap
// per-series lookup saves.
func TestOwnershipFingerprint(t *testing.T) {
	base := &Desc{Ingesters: map[string]InstanceDesc{
		"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
		"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
	}}
	baseFingerprint := ownershipFingerprint(base)

	t.Run("is stable for equal rings", func(t *testing.T) {
		same := &Desc{Ingesters: map[string]InstanceDesc{
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
		}}
		assert.Equal(t, baseFingerprint, ownershipFingerprint(same),
			"fingerprint must not depend on map iteration order")
	})

	t.Run("ignores heartbeats", func(t *testing.T) {
		heartbeat := &Desc{Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 9999},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 8888},
		}}
		assert.Equal(t, baseFingerprint, ownershipFingerprint(heartbeat),
			"a heartbeat must not force an ownership recompute")
	})

	// Each of these changes the replica set, so each must invalidate the bitmap.
	// The state case is the one Desc.RingCompare cannot distinguish from a
	// heartbeat, which is why this fingerprint exists at all.
	for name, changed := range map[string]*Desc{
		"state change": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: LEAVING, Tokens: []uint32{100, 300}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
		}},
		"token change": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 301}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
		}},
		"zone change": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-c", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
		}},
		"instance added": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200, 400}, Timestamp: 1000},
			"instance-3": {Addr: "10.0.0.3:9095", Zone: "zone-c", State: ACTIVE, Tokens: []uint32{500}, Timestamp: 1000},
		}},
		"instance removed": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300}, Timestamp: 1000},
		}},
		"tokens moved between instances": {Ingesters: map[string]InstanceDesc{
			"instance-1": {Addr: "10.0.0.1:9095", Zone: "zone-a", State: ACTIVE, Tokens: []uint32{100, 300, 400}, Timestamp: 1000},
			"instance-2": {Addr: "10.0.0.2:9095", Zone: "zone-b", State: ACTIVE, Tokens: []uint32{200}, Timestamp: 1000},
		}},
	} {
		t.Run("changes on "+name, func(t *testing.T) {
			assert.NotEqual(t, baseFingerprint, ownershipFingerprint(changed))
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

	b.ReportAllocs()

	for b.Loop() {
		_, owned, err := OwnedTokenPositions(ringDesc, instanceID, Write, replicationFactor, true)
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

	tokens, owned, err := OwnedTokenPositions(ringDesc, "instance-does-not-exist", Write, 3, false)
	require.NoError(t, err)
	require.Len(t, owned, len(tokens))

	for p, isOwned := range owned {
		assert.False(t, isOwned, "unknown instance must not own token position %d", p)
	}
}
