package ring

import (
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
	desc := &Desc{Ingesters: generateRingInstances(30, 3, 128)}

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
	desc := &Desc{Ingesters: generateRingInstances(30, 3, 128)}
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
	desc := &Desc{Ingesters: generateRingInstances(6, 3, 16)}
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
	desc := &Desc{Ingesters: generateRingInstances(30, 3, 32)}
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
	desc := &Desc{Ingesters: generateRingInstances(3, 1, 4)}
	watched := newWatchedRingForOwnershipTest(t, desc, 1, false)
	watched.mtx.RLock()
	before := watched.ringDesc
	watched.mtx.RUnlock()

	watched.UpdateDesc(&Desc{Ingesters: generateRingInstances(5, 1, 4)})
	watched.mtx.RLock()
	defer watched.mtx.RUnlock()
	assert.Same(t, before, watched.ringDesc)
}
