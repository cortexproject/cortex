package ring

import (
	"runtime"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewRingFromDesc_AnswersLikeAWatchedRing checks that a ring updated by hand
// gives the same ownership and the same shuffle-shard subrings as a ring watching
// the KV store, given the same Desc.
func TestNewRingFromDesc_AnswersLikeAWatchedRing(t *testing.T) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3, ZoneAwarenessEnabled: true}
	desc := &Desc{Ingesters: generateUniqueRingInstances(30, 3, 128)}

	watched := newWatchedRingForOwnershipTest(t, cloneDesc(desc), 3, true)
	fromDesc, err := NewRingFromDesc(cfg, "test", log.NewNopLogger())
	require.NoError(t, err)
	fromDesc.UpdateDesc(cloneDesc(desc))

	require.Equal(t, watched.OwnershipFingerprint() != 0, fromDesc.OwnershipFingerprint() != 0)
	for id := range desc.Ingesters {
		wantTokens, wantOwned, err := watched.OwnedTokenPositions(id, Write)
		require.NoError(t, err)
		gotTokens, gotOwned, err := fromDesc.OwnedTokenPositions(id, Write)
		require.NoError(t, err)
		require.Equal(t, wantTokens, gotTokens)
		require.Equal(t, wantOwned, gotOwned, "instance %s", id)
	}

	for _, shardSize := range []int{3, 6, 12} {
		want := watched.ShuffleShard("tenant", shardSize).(*Ring)
		got := fromDesc.ShuffleShard("tenant", shardSize).(*Ring)
		require.ElementsMatch(t, keys(want.ringDesc.Ingesters), keys(got.ringDesc.Ingesters), "shard size %d", shardSize)
	}
}

// TestRing_UpdateDesc_StateChangeKeepsSubrings checks that a state-only update,
// the common case during a rolling restart, keeps the cached subrings and still
// reaches them, as on a watched ring.
func TestRing_UpdateDesc_StateChangeKeepsSubrings(t *testing.T) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3, ZoneAwarenessEnabled: true}
	desc := &Desc{Ingesters: generateUniqueRingInstances(30, 3, 128)}
	r, err := NewRingFromDesc(cfg, "test", log.NewNopLogger())
	require.NoError(t, err)
	r.UpdateDesc(cloneDesc(desc))

	before := r.ShuffleShard("tenant", 6).(*Ring)
	var member string
	for id := range before.ringDesc.Ingesters {
		member = id
		break
	}
	beforeFingerprint := before.OwnershipFingerprint()

	changed := cloneDesc(desc)
	instance := changed.Ingesters[member]
	instance.State = LEAVING
	changed.Ingesters[member] = instance
	r.UpdateDesc(changed)

	after := r.ShuffleShard("tenant", 6).(*Ring)
	assert.Same(t, before, after, "a state change must not rebuild the subring")
	assert.Equal(t, LEAVING, after.ringDesc.Ingesters[member].State)
	assert.NotEqual(t, beforeFingerprint, after.OwnershipFingerprint())
}

// TestRing_UpdateDesc_ExcludedZonesDoNotModifyTheInput checks that UpdateDesc
// leaves the Desc it is given alone even when excluded zones make the ring drop
// instances. The Desc is shared with the lifecycler.
func TestRing_UpdateDesc_ExcludedZonesDoNotModifyTheInput(t *testing.T) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 1, ExcludedZones: []string{"zone-0"}}
	desc := &Desc{Ingesters: generateUniqueRingInstances(6, 3, 16)}
	r, err := NewRingFromDesc(cfg, "test", log.NewNopLogger())
	require.NoError(t, err)

	r.UpdateDesc(desc)
	assert.Len(t, desc.Ingesters, 6)
	assert.Equal(t, 4, r.InstancesCount())
}

func TestOwnershipMayHaveChanged(t *testing.T) {
	base := func() *Desc {
		return &Desc{Ingesters: map[string]InstanceDesc{
			"a": {Addr: "a", Zone: "z1", State: ACTIVE, Tokens: []uint32{1, 3}, Timestamp: 10},
			"b": {Addr: "b", Zone: "z2", State: ACTIVE, Tokens: []uint32{2, 4}, Timestamp: 10},
		}}
	}

	assert.True(t, ownershipMayHaveChanged(nil, base()))
	assert.False(t, ownershipMayHaveChanged(base(), base()))

	heartbeat := base()
	instance := heartbeat.Ingesters["a"]
	instance.Timestamp = 99
	heartbeat.Ingesters["a"] = instance
	assert.False(t, ownershipMayHaveChanged(base(), heartbeat), "a heartbeat must not count as a change")

	moved := base()
	instance = moved.Ingesters["a"]
	instance.Addr = "a-restarted"
	instance.RegisteredTimestamp = 99
	moved.Ingesters["a"] = instance
	assert.False(t, ownershipMayHaveChanged(base(), moved), "a new address must not count as a change")

	for name, change := range map[string]func(d *Desc){
		"state": func(d *Desc) {
			instance := d.Ingesters["a"]
			instance.State = LEAVING
			d.Ingesters["a"] = instance
		},
		"read-only": func(d *Desc) {
			instance := d.Ingesters["a"]
			instance.State = READONLY
			d.Ingesters["a"] = instance
		},
		"tokens": func(d *Desc) {
			instance := d.Ingesters["a"]
			instance.Tokens = []uint32{1, 5}
			d.Ingesters["a"] = instance
		},
		"instance added": func(d *Desc) {
			d.Ingesters["c"] = InstanceDesc{Addr: "c", Zone: "z3", State: ACTIVE, Tokens: []uint32{6}}
		},
		"instance removed": func(d *Desc) { delete(d.Ingesters, "b") },
	} {
		changed := base()
		change(changed)
		assert.True(t, ownershipMayHaveChanged(base(), changed), name)
	}
}

func TestCloneDesc_SharesNothing(t *testing.T) {
	desc := &Desc{Ingesters: map[string]InstanceDesc{"a": {Addr: "a", Tokens: []uint32{1, 2}}}}
	clone := cloneDesc(desc)

	instance := desc.Ingesters["a"]
	instance.Tokens[0] = 100
	desc.Ingesters["b"] = InstanceDesc{Addr: "b"}

	assert.Equal(t, []uint32{1, 2}, clone.Ingesters["a"].Tokens)
	assert.NotContains(t, clone.Ingesters, "b")
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestOwnershipFingerprint_IgnoresAddressChanges covers an instance coming back
// with a new address, as a restarted pod usually does. The ring treats it as a
// topology change, but replica sets do not depend on addresses, so ownership is
// unchanged, for the whole ring and for a subring.
func TestOwnershipFingerprint_IgnoresAddressChanges(t *testing.T) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3, ZoneAwarenessEnabled: true}
	desc := &Desc{Ingesters: generateUniqueRingInstances(30, 3, 32)}
	r, err := NewRingFromDesc(cfg, "test", log.NewNopLogger())
	require.NoError(t, err)
	r.UpdateDesc(cloneDesc(desc))
	before := r.OwnershipFingerprint()
	subBefore := r.ShuffleShard("tenant", 6).(*Ring).OwnershipFingerprint()

	moved := cloneDesc(desc)
	for id, instance := range moved.Ingesters {
		instance.Addr = "10.9.9.9:9095"
		moved.Ingesters[id] = instance
		break
	}
	require.Equal(t, Different, desc.RingCompare(moved), "an address change is a topology change")
	r.UpdateDesc(moved)

	assert.Equal(t, before, r.OwnershipFingerprint())
	assert.Equal(t, subBefore, r.ShuffleShard("tenant", 6).(*Ring).OwnershipFingerprint())
}

// TestRing_UpdateDesc_DoesNothingOnAWatchedRing covers UpdateDesc called on a ring
// whose state comes from watching the KV store, where it would race with the watch.
func TestRing_UpdateDesc_DoesNothingOnAWatchedRing(t *testing.T) {
	desc := &Desc{Ingesters: generateUniqueRingInstances(3, 1, 4)}
	watched := newWatchedRingForOwnershipTest(t, desc, 1, false)
	watched.mtx.RLock()
	before := watched.ringDesc
	watched.mtx.RUnlock()

	watched.UpdateDesc(&Desc{Ingesters: generateUniqueRingInstances(5, 1, 4)})
	watched.mtx.RLock()
	defer watched.mtx.RUnlock()
	assert.Same(t, before, watched.ringDesc)
}

// TestOwnershipFingerprint_SubringIgnoresStateChangesOfOtherInstances checks that
// a subring's fingerprint, unlike the whole ring's, does not change when an
// instance outside the shard changes state, as happens on every restart during a
// rolling deploy.
func TestOwnershipFingerprint_SubringIgnoresStateChangesOfOtherInstances(t *testing.T) {
	cfg := Config{HeartbeatTimeout: time.Hour, ReplicationFactor: 3, ZoneAwarenessEnabled: true}
	desc := &Desc{Ingesters: generateUniqueRingInstances(30, 3, 32)}
	r, err := NewRingFromDesc(cfg, "test", log.NewNopLogger())
	require.NoError(t, err)
	r.UpdateDesc(cloneDesc(desc))
	ringBefore := r.OwnershipFingerprint()
	subring := r.ShuffleShard("tenant", 6).(*Ring)
	subBefore := subring.OwnershipFingerprint()

	changed := cloneDesc(desc)
	for id, instance := range changed.Ingesters {
		if _, member := subring.ringDesc.Ingesters[id]; !member {
			instance.State = LEAVING
			changed.Ingesters[id] = instance
			break
		}
	}
	r.UpdateDesc(changed)

	assert.NotEqual(t, ringBefore, r.OwnershipFingerprint())
	assert.Equal(t, subBefore, r.ShuffleShard("tenant", 6).(*Ring).OwnershipFingerprint())
}

// BenchmarkRing_UpdateDesc measures bringing the ingester's ring up to date with
// a ring the lifecycler read: cheap for a change of instance states, which keeps
// the cached subrings, and a full rebuild for a topology change.
func BenchmarkRing_UpdateDesc(b *testing.B) {
	desc := generateBenchmarkRing(benchmarkRingInstances, benchmarkRingZones, benchmarkRingTokens)

	stateChanged := cloneDesc(desc)
	instance := stateChanged.Ingesters["ingester-1"]
	instance.State = LEAVING
	stateChanged.Ingesters["ingester-1"] = instance

	topologyChanged := cloneDesc(desc)
	instance = topologyChanged.Ingesters["ingester-1"]
	instance.Tokens = append([]uint32(nil), instance.Tokens...)
	instance.Tokens[0]++
	topologyChanged.Ingesters["ingester-1"] = instance

	for name, next := range map[string]*Desc{"state_change": stateChanged, "topology_change": topologyChanged} {
		b.Run(name, func(b *testing.B) {
			r := newBenchmarkRing(b, desc)
			descs := [2]*Desc{next, desc}
			b.ReportAllocs()
			n := 0
			for b.Loop() {
				r.UpdateDesc(descs[n%2])
				n++
			}
		})
	}
}

// BenchmarkRing_NewRingFromDescMemory reports the heap held by the ingester's own
// ring, built from the lifecycler's copy of the ring.
func BenchmarkRing_NewRingFromDescMemory(b *testing.B) {
	desc := generateBenchmarkRing(benchmarkRingInstances, benchmarkRingZones, benchmarkRingTokens)
	for b.Loop() {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		r := newBenchmarkRing(b, desc)
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "B/ring")
		runtime.KeepAlive(r)
	}
}

// BenchmarkOwnershipMayHaveChanged measures the check the lifecycler makes on
// every ring read to decide whether to keep a new copy of the ring, and the copy
// itself, which is only made when the check says so.
func BenchmarkOwnershipMayHaveChanged(b *testing.B) {
	desc := generateBenchmarkRing(benchmarkRingInstances, benchmarkRingZones, benchmarkRingTokens)
	heartbeat := cloneDesc(desc)
	for id, instance := range heartbeat.Ingesters {
		instance.Timestamp++
		heartbeat.Ingesters[id] = instance
	}

	b.Run("heartbeat_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if ownershipMayHaveChanged(desc, heartbeat) {
				b.Fatal("a heartbeat must not change ownership")
			}
		}
	})
	b.Run("copy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = cloneDesc(desc)
		}
	})
}
