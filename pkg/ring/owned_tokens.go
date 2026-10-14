package ring

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
)

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
