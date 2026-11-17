package clientlimiter

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/cortexproject/cortex/pkg/ring/kv/codec"
	"github.com/cortexproject/cortex/pkg/ring/kv/memberlist"
)

// ProtoClientCounterDescFactory makes new ClientCounterDescs.
func ProtoClientCounterDescFactory() proto.Message {
	return NewClientCounterDesc()
}

// GetClientCounterDescCodec returns the codec used to encode and decode
// ClientCounterDesc values stored in the KV store.
func GetClientCounterDescCodec() codec.Proto {
	return codec.NewProtoCodec("clientCounterDesc", ProtoClientCounterDescFactory)
}

// NewClientCounterDesc returns an empty ClientCounterDesc.
func NewClientCounterDesc() *ClientCounterDesc {
	return &ClientCounterDesc{Clients: map[string]*ClientEntry{}}
}

// Clone returns a deep copy of the descriptor.
func (d *ClientCounterDesc) Clone() any {
	return proto.Clone(d).(*ClientCounterDesc)
}

// Merge merges other into this ClientCounterDesc and returns the sub-set of
// entries that changed, which can be gossiped to other replicas. This is
// modeled on ring.Desc.Merge (pkg/ring/model.go), adapted to merge a
// map-of-maps (client -> replica -> counter) instead of a single-level map:
// for each (client, replica) pair, the entry with the later window wins, or
// -- within the same window -- the entry with the higher count wins. Taking
// the max is idempotent and commutative, which Mergeable requires: replaying
// the same gossip message, or receiving updates in a different order, always
// converges to the same state, unlike a naive "+=" which would double-count
// on replay.
//
// localCAS is unused: unlike ring.Desc, we don't need to detect and tombstone
// entries missing from the incoming value -- staleness is handled separately
// by pruning entries whose LastUpdateUnixNano has aged out (see GossipLimiter's
// prune loop), not by treating "absent from this particular gossip message" as
// "gone".
func (d *ClientCounterDesc) Merge(mergeable memberlist.Mergeable, _ bool) (memberlist.Mergeable, error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*ClientCounterDesc)
	if !ok {
		return nil, fmt.Errorf("expected *clientlimiter.ClientCounterDesc, got %T", mergeable)
	}
	if other == nil {
		return nil, nil
	}

	if d.Clients == nil {
		d.Clients = map[string]*ClientEntry{}
	}

	var updated map[string]*ClientEntry

	for clientID, oEntry := range other.Clients {
		if oEntry == nil {
			continue
		}

		tEntry := d.Clients[clientID]
		if tEntry == nil {
			tEntry = &ClientEntry{Replicas: map[string]*ReplicaCounter{}}
		}

		var changedReplicas map[string]*ReplicaCounter

		for replicaID, oCounter := range oEntry.Replicas {
			if oCounter == nil {
				continue
			}

			tCounter := tEntry.Replicas[replicaID]
			if tCounter == nil || counterIsNewer(oCounter, tCounter) {
				tEntry.Replicas[replicaID] = oCounter
				if changedReplicas == nil {
					changedReplicas = map[string]*ReplicaCounter{}
				}
				changedReplicas[replicaID] = oCounter
			}
		}

		if changedReplicas != nil {
			d.Clients[clientID] = tEntry
			if updated == nil {
				updated = map[string]*ClientEntry{}
			}
			updated[clientID] = &ClientEntry{Replicas: changedReplicas}
		}
	}

	if updated == nil {
		return nil, nil
	}

	return &ClientCounterDesc{Clients: updated}, nil
}

// counterIsNewer reports whether o should replace t: a later window always
// wins, and within the same window the higher (monotonically increasing)
// count wins.
func counterIsNewer(o, t *ReplicaCounter) bool {
	if o.WindowStartUnixNano != t.WindowStartUnixNano {
		return o.WindowStartUnixNano > t.WindowStartUnixNano
	}
	return o.Count > t.Count
}

// MergeContent describes the content of this Mergeable: the set of client
// IDs it carries entries for.
func (d *ClientCounterDesc) MergeContent() []string {
	result := make([]string, 0, len(d.Clients))
	for clientID := range d.Clients {
		result = append(result, clientID)
	}
	return result
}

// RemoveTombstones is a no-op: ClientCounterDesc has no delete-tombstone
// concept. Idle clients/replicas are pruned directly (not soft-deleted) by
// GossipLimiter's own periodic janitor, consistent with this feature's
// existing best-effort, fail-open philosophy -- see allowClientIdentityIngestion's
// doc comment in the original ring/RPC design this replaces.
func (d *ClientCounterDesc) RemoveTombstones(_ time.Time) (total, removed int) {
	return 0, 0
}
