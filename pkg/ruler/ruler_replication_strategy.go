package ruler

import (
	"time"

	"github.com/pkg/errors"

	"github.com/cortexproject/cortex/pkg/ring"
)

// RingOp is the operation used for distributing rule groups between rulers.
var RingOp = ring.NewOp([]ring.IngesterState{ring.ACTIVE}, func(s ring.IngesterState) bool {
	// Only ACTIVE rulers get any rule groups. If instance is not ACTIVE, we need to find another ruler.
	return s != ring.ACTIVE
})

type rulerReplicationStrategy struct{}

func (r rulerReplicationStrategy) Filter(instances []ring.IngesterDesc, op ring.Operation, _ int, heartbeatTimeout time.Duration, _ bool) (healthy []ring.IngesterDesc, maxFailures int, err error) {
	now := time.Now()

	// Filter out unhealthy instances.
	for i := 0; i < len(instances); {
		if instances[i].IsHealthy(op, heartbeatTimeout, now) {
			i++
		} else {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}

	if len(instances) == 0 {
		return nil, 0, errors.New("no healthy ruler instance found for the replication set")
	}

	return instances, len(instances) - 1, nil
}
