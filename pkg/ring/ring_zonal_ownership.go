package ring

// ringHashSpaceSize is the number of distinct token values in the ring.
const ringHashSpaceSize = float64(maxTokenValue) + 1

// ZonalOwnershipRing is implemented by rings that can report the share of
// the hash space each instance owns within its own zone.
type ZonalOwnershipRing interface {
	// ZonalOwnership returns, for every instance in the ring, the fraction
	// of its zone's hash space it owns. See (*Ring).ZonalOwnership for the
	// exact semantics.
	ZonalOwnership() map[string]float64

	// ZonesCount returns the number of zones with at least one instance with
	// tokens. Only when it equals the replication factor does each zone hold
	// exactly one copy of every key, which is what ZonalOwnership assumes.
	ZonesCount() int
}

// ZonesCount returns the number of zones with at least one instance with tokens.
func (r *Ring) ZonesCount() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return len(r.ringZones)
}

// ZonalOwnership returns, for every instance in the ring, the fraction of its
// zone's hash space it owns.
//
// Every instance owns the ranges of its tokens, except READONLY instances:
// writes skip them, and their ranges fall to the next instance in the same
// zone, which is the instance Get extends to. READONLY instances report 0.
// This is where writes go with -distributor.extend-writes=false. With it on,
// writes also skip JOINING, LEAVING and PENDING instances, which this does not
// model.
//
// The values are the fraction of keys each instance receives only with
// zone-aware replication and a replication factor equal to ZonesCount. With
// more replicas than zones, Get puts a second copy in some zones, and with
// fewer it skips some zones, so callers should not use the values then.
//
// The returned map is cached until the ring's tokens change or an instance
// becomes or stops being READONLY, and must not be modified.
func (r *Ring) ZonalOwnership() map[string]float64 {
	r.mtx.RLock()
	cached := r.zonalOwnership
	r.mtx.RUnlock()
	if cached != nil {
		return cached
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()
	if r.zonalOwnership == nil {
		r.zonalOwnership = r.computeZonalOwnership()
	}
	return r.zonalOwnership
}

// invalidateZonalOwnership drops the cached ownership. The caller must hold
// r.mtx for writing.
func (r *Ring) invalidateZonalOwnership() {
	r.zonalOwnership = nil
}

// computeZonalOwnership must be called with r.mtx held.
func (r *Ring) computeZonalOwnership() map[string]float64 {
	ownership := make(map[string]float64, len(r.ringDesc.Ingesters))
	for id := range r.ringDesc.Ingesters {
		ownership[id] = 0
	}

	for _, zoneTokens := range r.ringTokensByZone {
		// Tokens are sorted, so the remaining tokens stay sorted.
		remaining := make([]uint32, 0, len(zoneTokens))
		for _, token := range zoneTokens {
			if r.ownsToken(token) {
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

// ownsToken reports whether the instance holding token owns its range, that
// is, whether it is not READONLY.
func (r *Ring) ownsToken(token uint32) bool {
	info, ok := r.ringInstanceByToken[token]
	if !ok {
		return false
	}
	instance, ok := r.ringDesc.Ingesters[info.InstanceID]
	return ok && instance.State != READONLY
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
