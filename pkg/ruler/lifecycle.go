package ruler

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

// TransferOut is a noop for the ruler
func (r *Ruler) TransferOut(ctx context.Context) error {
	return nil
}

// StopIncomingRequests is called during the shutdown process.
// Ensure no new rules are scheduled on this Ruler
// Currently the api is decoupled from the scheduler, no action
// is required.
func (r *Ruler) StopIncomingRequests() {}

// Flush triggers a flush of all the work items currently
// scheduled by the ruler, currently every ruler will
// query a backend rule store for it's rules so no
// flush is required.
func (r *Ruler) Flush() {
	flushTicker := time.NewTicker(r.cfg.FlushCheckPeriod)
	defer flushTicker.Stop()

	for {
		select {
		case <-flushTicker.C:
			level.Debug(util.Logger).Log("msg", "ruler flush timed out")
			return
		default:
			item := r.scheduler.nextWorkItem()
			if item == nil {
				level.Debug(util.Logger).Log("msg", "flushing complete")
				return
			}
			level.Debug(util.Logger).Log("msg", "flushing item", "item", item)
		}
	}
}
