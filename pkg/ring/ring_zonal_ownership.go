package ring

// ringHashSpaceSize is the number of distinct token values in the ring.
const ringHashSpaceSize = float64(maxTokenValue) + 1

// ZonalOwnershipRing is implemented by rings that can report the share of
// the hash space each instance owns within its own zone.
type ZonalOwnershipRing interface {
	// ZonalOwnership returns, for every instance in the ring, the fraction
	// of its zone's hash space it owns for writes made with op. See
	// (*Ring).ZonalOwnership for the exact semantics.
	ZonalOwnership(op Operation) map[string]float64
}

// ZonalOwnership returns, for every instance in the ring, the fraction of its
// zone's hash space that op routes to that instance.
//
// Tokens of instances for which op extends the replica set (for example
// JOINING or LEAVING instances with Write, or READONLY instances with either
// write operation) are left out: their ranges fall to the next remaining token
// in the same zone, which is the instance Get extends to. Instances excluded
// this way report 0. Instances that Get keeps but Filter drops without
// replacement (such as an ACTIVE instance with a stale heartbeat) keep their
// tokens, because no other instance in the zone receives those keys.
//
// With zone-aware replication and one replica per zone, the values in a zone
// are the fraction of a tenant's keys each instance receives. Without zone
// awareness every instance is in the same zone and the values describe only
// the first replica, so callers should not use them.
//
// The returned map is cached until the ring's tokens or instance states
// change, and must not be modified.
func (r *Ring) ZonalOwnership(op Operation) map[string]float64 {
	r.mtx.RLock()
	cached, ok := r.zonalOwnershipByOp[op]
	r.mtx.RUnlock()
	if ok {
		return cached
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()
	if cached, ok := r.zonalOwnershipByOp[op]; ok {
		return cached
	}

	ownership := r.computeZonalOwnership(op)
	if r.zonalOwnershipByOp == nil {
		r.zonalOwnershipByOp = map[Operation]map[string]float64{}
	}
	r.zonalOwnershipByOp[op] = ownership
	return ownership
}

// invalidateZonalOwnership drops the cached ownership. The caller must hold
// r.mtx for writing.
func (r *Ring) invalidateZonalOwnership() {
	r.zonalOwnershipByOp = nil
}

// computeZonalOwnership must be called with r.mtx held.
func (r *Ring) computeZonalOwnership(op Operation) map[string]float64 {
	ownership := make(map[string]float64, len(r.ringDesc.Ingesters))
	for id := range r.ringDesc.Ingesters {
		ownership[id] = 0
	}

	for _, zoneTokens := range r.ringTokensByZone {
		// Tokens are sorted, so the remaining tokens stay sorted.
		remaining := make([]uint32, 0, len(zoneTokens))
		for _, token := range zoneTokens {
			if r.ownsTokenForWrites(token, op) {
				remaining = append(remaining, token)
			}
		}

		for i, token := range remaining {
			prev := remaining[(i+len(remaining)-1)%len(remaining)]
			id := r.ringInstanceByToken[token].InstanceID
			ownership[id] += float64(ownedRange(prev, token, len(remaining))) / ringHashSpaceSize
		}
	}

	return ownership
}

// ownsTokenForWrites reports whether writes made with op stop at the instance
// holding token, rather than extending past it.
func (r *Ring) ownsTokenForWrites(token uint32, op Operation) bool {
	info, ok := r.ringInstanceByToken[token]
	if !ok {
		return false
	}
	instance, ok := r.ringDesc.Ingesters[info.InstanceID]
	if !ok {
		return false
	}
	return !op.ShouldExtendReplicaSetOnState(instance.State)
}

// ownedRange returns the number of token values in (prev, token], the range
// owned by token when prev is the preceding token in the same zone.
func ownedRange(prev, token uint32, zoneTokenCount int) int64 {
	if prev == token && zoneTokenCount > 1 {
		// A duplicate token: the first copy already owns the range.
		return 0
	}
	return tokenDistance(prev, token)
}
