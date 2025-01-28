package compactor

import (
	"context"

	"github.com/cortexproject/cortex/pkg/ring"
)

func (c *Compactor) OnRingInstanceHeartbeat(_ *ring.Lifecycler, ctx context.Context) {
	c.ring.AutoForgetUnhealthy(ctx)
}
