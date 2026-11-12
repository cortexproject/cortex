package ring

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

// OwnershipFingerprint returns a value which changes if and only if something
// OwnedTokenPositions depends on may have changed: the set of instances, their
// zones and tokens, or their states.
//
// It is cheap enough to call on every check, because it does not hash any
// tokens. Instances, zones and tokens only change together with the ring's
// topology, which updateRingState already timestamps, so the fingerprint hashes
// that timestamp and each instance's state.
//
// States have to be hashed separately because a state change is not a topology
// change: Desc.RingCompare reports it the same way as a heartbeat. Yet ownership
// depends on state, because a non-ACTIVE instance extends the replica set.
//
// Heartbeat timestamps are deliberately ignored. They change constantly, and
// recomputing ownership on every heartbeat would cost far more than making the
// per-series check cheap saves.
//
// It is zero while the ring has no instances, including before it has been read.
func (r *Ring) OwnershipFingerprint() uint64 {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	if r.ringDesc == nil || len(r.ringDesc.Ingesters) == 0 {
		return 0
	}

	ids := make([]string, 0, len(r.ringDesc.Ingesters))
	for id := range r.ringDesc.Ingesters {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var (
		h   = fnv.New64a()
		buf [8]byte
		sep = []byte{0}
	)

	binary.LittleEndian.PutUint64(buf[:], uint64(r.lastTopologyChange.UnixNano()))
	_, _ = h.Write(buf[:])

	for _, id := range ids {
		_, _ = h.Write([]byte(id))
		_, _ = h.Write(sep)
		binary.LittleEndian.PutUint64(buf[:], uint64(r.ringDesc.Ingesters[id].State))
		_, _ = h.Write(buf[:])
	}

	return h.Sum64()
}

// OwnedTokenPositions returns this ring's sorted token list together with a
// parallel bitmap marking which token positions are owned by instanceID.
//
// A position p is "owned" by an instance when that instance is a member of the
// replica set this ring selects for any key k with SearchToken(tokens, k) == p.
// In other words ownership answers "would a write for a series hashing into this
// token range be replicated to me?", which is exactly the question the
// distributor answers when it routes. It is NOT the narrower question "am I the
// first instance in this token range": with a replication factor greater than
// one every series is held by several instances, and all of them must count it,
// because the per-instance series limit is itself scaled by the replication
// factor.
//
// It answers for this ring, whatever it is. Called on the subring returned by
// ShuffleShard, it answers for a tenant whose writes are routed within that
// subring, which picks different replica sets from the full ring.
//
// The bitmap is derived from replicaSetAt, the same walk Ring.Get uses, so
// ownership and routing cannot drift apart.
//
// Ownership deliberately does NOT apply the replication strategy's health
// filter. It must be stable across heartbeat flapping: a series does not stop
// being this instance's responsibility because a peer missed a heartbeat.
//
// An instance which is not in the ring owns nothing. The returned token list is
// the ring's own, which is replaced rather than modified when the ring changes,
// so it must not be modified by the caller.
//
// This is O(len(tokens) x replicationFactor) and is meant to be called once per
// ring change, never per series. Per-series questions are then answered with
// owned[SearchToken(tokens, key)], which is a binary search plus an array index.
func (r *Ring) OwnedTokenPositions(instanceID string, op Operation) ([]uint32, []bool, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	// The ingester can ask before the ring has been read. That is an empty
	// answer rather than an error.
	if r.ringDesc == nil || len(r.ringTokens) == 0 {
		return nil, nil, nil
	}

	topology := r.topology()
	owned := make([]bool, len(topology.tokens))
	bufDescs, bufHosts, bufZones := MakeBuffersForGet()

	for position := range topology.tokens {
		_, instanceIDs, err := topology.replicaSetAt(position, op, bufDescs, bufHosts, bufZones)
		if err != nil {
			return nil, nil, err
		}

		// Compare instance IDs, not addresses: InstanceDesc carries no ID of its
		// own, and a ring is keyed by instance ID while Addr is a separate,
		// different value.
		for _, id := range instanceIDs {
			if id == instanceID {
				owned[position] = true
				break
			}
		}
	}

	return topology.tokens, owned, nil
}

// ownershipFingerprint returns a value which changes if and only if something the
// replica-set walk depends on has changed: the set of instances, their zones,
// their states and their tokens.
//
// It deliberately ignores Timestamp and RegisteredTimestamp. Heartbeats bump
// Timestamp constantly, and recomputing ownership on every heartbeat would cost
// far more than making the per-series check cheap saves.
//
// Desc.RingCompare cannot be used for this: it reports a timestamp-only change
// and a state change as the same EqualButStatesAndTimestamps result, and
// ownership does depend on state, because a non-ACTIVE instance extends the
// replica set.
func ownershipFingerprint(d *Desc) uint64 {
	ids := make([]string, 0, len(d.Ingesters))
	for id := range d.Ingesters {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var (
		h   = fnv.New64a()
		buf [8]byte
		sep = []byte{0}
	)

	for _, id := range ids {
		instance := d.Ingesters[id]

		_, _ = h.Write([]byte(id))
		_, _ = h.Write(sep)
		_, _ = h.Write([]byte(instance.Zone))
		_, _ = h.Write(sep)

		binary.LittleEndian.PutUint64(buf[:], uint64(instance.State))
		_, _ = h.Write(buf[:])

		for _, token := range instance.Tokens {
			binary.LittleEndian.PutUint32(buf[:4], token)
			_, _ = h.Write(buf[:4])
		}
		_, _ = h.Write(sep)
	}

	return h.Sum64()
}

// OwnedTokenPositions returns the ring's full sorted token list together with a
// parallel bitmap marking which token positions are owned by instanceID.
//
// Deprecated: use Ring.OwnedTokenPositions, which also answers for a shuffle-shard
// subring. This is kept only until the lifecycler stops computing ownership.
//
// A position p is "owned" by an instance when that instance is a member of the
// replica set the ring selects for any key k with SearchToken(tokens, k) == p.
// In other words ownership answers "would a write for a series hashing into this
// token range be replicated to me?", which is exactly the question the
// distributor answers when it routes. It is NOT the narrower question "am I the
// first instance in this token range": with a replication factor greater than
// one every series is held by several instances, and all of them must count it,
// because the per-instance series limit is itself scaled by the replication
// factor.
//
// The bitmap is derived from replicaSetAt, the same walk Ring.Get uses, so
// ownership and routing cannot drift apart.
//
// Ownership deliberately does NOT apply the replication strategy's health
// filter. It must be stable across heartbeat flapping: a series does not stop
// being this instance's responsibility because a peer missed a heartbeat.
//
// This is O(len(tokens) x replicationFactor) and is meant to be called once per
// ring change, never per series. Per-series questions are then answered with
// owned[SearchToken(tokens, key)], which is a binary search plus an array index.
func OwnedTokenPositions(d *Desc, instanceID string, op Operation, replicationFactor int, zoneAwarenessEnabled bool) ([]uint32, []bool, error) {
	tokens := d.GetTokens()
	owned := make([]bool, len(tokens))

	// The ingester asks this question during startup, before it has read the
	// ring, and briefly after being forgotten from the ring. Both are empty
	// answers rather than errors.
	if len(tokens) == 0 {
		return tokens, owned, nil
	}

	// Count distinct zones directly. Deriving this from getTokensByZone would
	// merge every token in the ring into a sorted slice per zone purely to take
	// the length of the resulting map.
	zones := make(map[string]struct{}, len(d.Ingesters))
	for _, instance := range d.Ingesters {
		zones[instance.Zone] = struct{}{}
	}

	topology := ringTopology{
		tokens:               tokens,
		instanceByToken:      d.getTokensInfo(),
		instances:            d.Ingesters,
		numZones:             len(zones),
		replicationFactor:    replicationFactor,
		zoneAwarenessEnabled: zoneAwarenessEnabled,
	}

	bufDescs, bufHosts, bufZones := MakeBuffersForGet()

	for position := range tokens {
		_, instanceIDs, err := topology.replicaSetAt(position, op, bufDescs, bufHosts, bufZones)
		if err != nil {
			return nil, nil, err
		}

		// Compare instance IDs, not addresses: InstanceDesc carries no ID of its
		// own, and a ring is keyed by instance ID while Addr is a separate,
		// different value.
		for _, id := range instanceIDs {
			if id == instanceID {
				owned[position] = true
				break
			}
		}
	}

	return tokens, owned, nil
}
