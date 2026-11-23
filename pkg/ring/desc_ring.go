package ring

import (
	"slices"
)

// cloneDesc returns a copy of d which shares nothing with it, so that either can
// be modified without affecting the other.
func cloneDesc(d *Desc) *Desc {
	if d == nil {
		return nil
	}
	c := &Desc{Ingesters: make(map[string]InstanceDesc, len(d.Ingesters))}
	for id, instance := range d.Ingesters {
		instance.Tokens = append([]uint32(nil), instance.Tokens...)
		c.Ingesters[id] = instance
	}
	return c
}

// ownershipMayHaveChanged reports whether next differs from prev in anything
// series ownership depends on: the set of instances, their zones, tokens and
// states. Addresses and heartbeat timestamps are ignored: neither changes a
// replica set, and an instance restarted in Kubernetes usually comes back with a
// new address. A nil prev always differs.
func ownershipMayHaveChanged(prev, next *Desc) bool {
	if prev == nil || next == nil {
		return prev != next
	}
	if len(prev.Ingesters) != len(next.Ingesters) {
		return true
	}
	for id, instance := range next.Ingesters {
		before, ok := prev.Ingesters[id]
		if !ok || before.Zone != instance.Zone || before.State != instance.State || !slices.Equal(before.Tokens, instance.Tokens) {
			return true
		}
	}
	return false
}
