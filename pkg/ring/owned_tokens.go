package ring

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

	topology := ringTopology{
		tokens:               tokens,
		instanceByToken:      d.getTokensInfo(),
		instances:            d.Ingesters,
		numZones:             len(d.getTokensByZone()),
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
