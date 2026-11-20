package ring

import (
	"maps"
	"slices"

	"github.com/go-kit/log"

	"github.com/cortexproject/cortex/pkg/ring/kv"
)

// NewDescFedRing returns a Ring that never watches the KV store: the caller
// keeps it up to date with UpdateDesc. It must not be started as a service.
// kvClient is only used to read the KV store's last update time, which
// instance health checks need.
//
// Its metrics are not registered, because they share names with the metrics
// of a regular Ring in the same process.
func NewDescFedRing(cfg Config, name, key string, kvClient kv.Client, logger log.Logger) (*Ring, error) {
	cfg.DetailedMetricsEnabled = false
	return NewWithStoreClientAndStrategy(cfg, name, key, kvClient, NewDefaultReplicationStrategy(), nil, logger)
}

// UpdateDesc replaces the ring state with a copy of desc. desc is not modified
// or retained, so the caller may keep using it.
func (r *Ring) UpdateDesc(desc *Desc) {
	r.updateRingState(copyDescForRing(desc))
}

// copyDescForRing copies desc deeply enough for updateRingState, which deletes
// instances and sorts their tokens in place. Desc.Clone is not enough: the
// instances it returns share their token slices with desc.
func copyDescForRing(desc *Desc) *Desc {
	out := &Desc{Ingesters: maps.Clone(desc.Ingesters)}
	for id, instance := range out.Ingesters {
		instance.Tokens = slices.Clone(instance.Tokens)
		out.Ingesters[id] = instance
	}
	return out
}
