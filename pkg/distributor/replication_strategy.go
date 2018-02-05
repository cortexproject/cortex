package distributor

import (
	"fmt"

	"github.com/weaveworks/cortex/pkg/ring"
)

func (d *Distributor) replicationStrategy(ingesters []*ring.IngesterDesc) (
	minSuccess, maxFailure int, liveIngesters []*ring.IngesterDesc, err error,
) {
	// We need a response from a quorum of ingesters, which is n/2 + 1.  In the
	// case of a node joining/leaving, the actual replica set might be bigger
	// than the replication factor, so we need to account for this.
	// See comment in ring/ring.go:getInternal.
	replicationFactor := d.cfg.ReplicationFactor
	if len(ingesters) > replicationFactor {
		replicationFactor = len(ingesters)
	}
	minSuccess = (replicationFactor / 2) + 1
	maxFailure = replicationFactor - minSuccess
	if maxFailure < 0 {
		maxFailure = 0
	}

	// Skip those that have not heartbeated in a while. NB these are still
	// included in the calculation of minSuccess, so if too many failed ingesters
	// will cause the whole write to fail.
	liveIngesters = make([]*ring.IngesterDesc, 0, len(ingesters))
	for _, ingester := range ingesters {
		if d.ring.IsHealthy(ingester) {
			liveIngesters = append(liveIngesters, ingester)
		}
	}

	// This is just a shortcut - if there are not minSuccess available ingesters,
	// after filtering out dead ones, don't even bother trying.
	if len(liveIngesters) < minSuccess {
		err = fmt.Errorf("at least %d live ingesters required, could only find %d",
			minSuccess, len(liveIngesters))
		return
	}

	return
}
