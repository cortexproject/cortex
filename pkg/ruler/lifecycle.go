package ruler

import (
	"github.com/cortexproject/cortex/pkg/ring"
)

func (r *Ruler) OnRingInstanceRegister(_ *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.IngesterDesc) (ring.IngesterState, ring.Tokens) {
	// When we initialize the store-gateway instance in the ring we want to start from
	// a clean situation, so whatever is the state we set it JOINING, while we keep existing
	// tokens (if any) or the ones loaded from file.
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	_, takenTokens := ringDesc.TokensFor(instanceID)
	newTokens := ring.GenerateTokens(r.cfg.Ring.NumTokens-len(tokens), takenTokens)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.JOINING, tokens
}

func (r *Ruler) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {}
func (r *Ruler) OnRingInstanceStopping(_ *ring.BasicLifecycler)              {}
func (r *Ruler) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.IngesterDesc) {
}
