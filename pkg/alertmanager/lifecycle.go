package alertmanager

import (
	"github.com/cortexproject/cortex/pkg/ring"
)

func (r *MultitenantAlertmanager) OnRingInstanceRegister(lc *ring.BasicLifecycler, ringDesc ring.Desc, instanceExists bool, instanceID string, instanceDesc ring.InstanceDesc) (ring.InstanceState, ring.Tokens) {
	// When we initialize the alertmanager instance in the ring we want to start from
	// a clean situation, so whatever is the state we set it JOINING, while we keep existing
	// tokens (if any).
	var tokens []uint32
	if instanceExists {
		tokens = instanceDesc.GetTokens()
	}

	newTokens := lc.GenerateTokens(&ringDesc, instanceID, instanceDesc.Zone, RingNumTokens-len(tokens), true)

	// Tokens sorting will be enforced by the parent caller.
	tokens = append(tokens, newTokens...)

	return ring.JOINING, tokens
}

func (r *MultitenantAlertmanager) OnRingInstanceTokens(_ *ring.BasicLifecycler, _ ring.Tokens) {}
func (r *MultitenantAlertmanager) OnRingInstanceStopping(_ *ring.BasicLifecycler)              {}
func (r *MultitenantAlertmanager) OnRingInstanceHeartbeat(_ *ring.BasicLifecycler, _ *ring.Desc, _ *ring.InstanceDesc) {
}
