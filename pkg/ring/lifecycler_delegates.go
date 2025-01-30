package ring

import (
	"time"

	"github.com/go-kit/log"
)

// AutoForgetDelegate automatically remove an instance from the ring if the last
// heartbeat is older than a configured period.
type LifecyclerAutoForgetDelegate struct {
	next         LifecyclerDelegate
	logger       log.Logger
	forgetPeriod time.Duration
}

func NewLifecyclerAutoForgetDelegate(forgetPeriod time.Duration, next LifecyclerDelegate, logger log.Logger) *LifecyclerAutoForgetDelegate {
	return &LifecyclerAutoForgetDelegate{
		next:         next,
		logger:       logger,
		forgetPeriod: forgetPeriod,
	}
}

func (d *LifecyclerAutoForgetDelegate) OnRingInstanceHeartbeat(lifecycler *Lifecycler, ringDesc *Desc) {
	AutoForgetFromRing(ringDesc, d.forgetPeriod, d.logger)
	d.next.OnRingInstanceHeartbeat(lifecycler, ringDesc)
}
