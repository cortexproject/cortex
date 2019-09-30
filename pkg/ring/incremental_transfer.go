package ring

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"go.uber.org/atomic"
)

// transferWorkload holds a set of ingester addresses to the set of
// token ranges that should be sent to or requested from that
// ingester address.
type transferWorkload map[string][]TokenRange

// Do executes functions for the transfer workload in parallel.
func (wl transferWorkload) Do(f func(addr string, ranges []TokenRange)) {
	var wg sync.WaitGroup
	for addr, ranges := range wl {
		wg.Add(1)
		go func(addr string, ranges []TokenRange) {
			defer wg.Done()
			f(addr, ranges)
		}(addr, ranges)
	}
	wg.Wait()
}

// findTransferWorkload finds all ranges that should be sent/requested against
// a target ingester address.
func (i *Lifecycler) findTransferWorkload(d *Desc, n TokenNavigator, token uint32, healthy HealthCheckFunc) (transferWorkload, bool) {
	ret := make(transferWorkload)
	rf := i.cfg.RingConfig.ReplicationFactor

	var lastErr error

	for replica := 0; replica < rf; replica++ {
		// Find the ranges which our token is holding data for as one of the replicas.
		ends, err := n.Predecessors(token, replica, healthy)
		if err != nil {
			lastErr = err
			continue
		}

		for _, end := range ends {
			// Find the starting value for the range.
			start, err := n.Neighbor(end.Token, -1, false, healthy)
			if err != nil {
				lastErr = err
				continue
			}

			// The target token's ingester is the ingester that either has data we
			// want or should have data we'll send. When joining, it's the old
			// end of the replica set. When leaving, it's the new end of the replica set.
			target, err := n.Neighbor(start.Token, rf, false, healthy)
			if err != nil {
				lastErr = err
				continue
			}

			// If our local ingester is already in the replica set, we don't need to
			// transfer. Our current token will never trigger InRange to return
			// true since it's only ever unhealthy at this point.
			checkRange := RangeOptions{
				Range: TokenRange{From: end.Token, To: target.Token},

				ID:          i.ID,
				IncludeFrom: true,
				IncludeTo:   true,
			}
			if ok, _ := n.InRange(checkRange, healthy); ok {
				continue
			}

			addr := d.Ingesters[target.Ingester].Addr
			ret[addr] = append(ret[addr], TokenRange{From: start.Token, To: end.Token})
		}
	}

	if lastErr != nil {
		level.Error(util.Logger).Log("msg", fmt.Sprintf("failed to find complete transfer set for %d", token), "err", lastErr)
	}
	return ret, lastErr == nil
}

// joinIncrementalTransfer will attempt to incrementally obtain chunks from
// neighboring lifecyclers that contain data for token ranges they will
// no longer receive writes for.
func (i *Lifecycler) joinIncrementalTransfer(ctx context.Context) error {
	// Make sure that we set all tokens to ACTIVE, even when we fail.
	defer func() {
		i.setTokens(i.getTransitioningTokens())
		i.changeState(ctx, ACTIVE)
	}()

	r := i.getLastRing()
	n := r.GetNavigator()

	replicationFactor := i.cfg.RingConfig.ReplicationFactor

	// If the replication factor is greater than the number of ingesters, findTransferWorkload
	// will fail to detect target ingesters properly. Instead, just request a full copy from
	// another existing ingester.
	if active := r.FindIngestersByState(ACTIVE); len(active) < replicationFactor {
		if len(active) == 0 {
			return fmt.Errorf("no ingesters to request data from")
		}

		fullRange := []TokenRange{{0, math.MaxUint32}}
		err := i.incTransferer.RequestChunkRanges(ctx, fullRange, active[0].Addr, false)
		if err != nil {
			level.Error(util.Logger).Log("msg", fmt.Sprintf("failed to request copy of data from %s", active[0].Addr))
		}
		return nil
	}

	var completesMtx sync.Mutex
	pendingCompletes := make(map[string][]TokenRange)

	for _, token := range i.transitioningTokens {
		healthy := func(t TokenDesc) bool {
			// When we're joining the ring, any of our tokens < our current token
			// are "healthy" (i.e., they have been added).
			if t.Ingester == i.ID {
				return t.Token < token
			}

			ing := r.Ingesters[t.Ingester]
			return ing.IsHealthy(Read, i.cfg.RingConfig.HeartbeatTimeout)
		}

		// Add the new token into the copy of the ring.
		n.SetIngesterTokens(i.ID, append(i.getTokens(), token))

		workload, _ := i.findTransferWorkload(r, n, token, healthy)

		workload.Do(func(addr string, ranges []TokenRange) {
			err := i.incTransferer.RequestChunkRanges(ctx, ranges, addr, true)
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to request chunks", "target", addr, "ranges", PrintableRanges(ranges), "err", err)
			}

			completesMtx.Lock()
			defer completesMtx.Unlock()
			pendingCompletes[addr] = append(pendingCompletes[addr], ranges...)
		})

		// Add the token into the ring now.
		i.addToken(token)
		n.SetIngesterTokens(i.ID, i.getTokens())

		if err := i.updateConsul(ctx); err != nil {
			level.Error(util.Logger).Log("msg",
				fmt.Sprintf("failed to update consul when changing token %d to ACTIVE", token))
		} else {
			// Update the latest copy of the ring - if the join is slow, we need to
			// keep our copy of the ring updates so ingesters aren't considered
			// unhealthy.
			r = i.getLastRing()
		}
	}

	go func() {
		// Delay unblocking ranges to allow the distributors enough time to notice the
		// new token. If ranges are unblocked too soon, spillover happens.
		if i.cfg.TransferFinishDelay > time.Duration(0) {
			<-time.After(i.cfg.TransferFinishDelay)
		}

		completesMtx.Lock()
		defer completesMtx.Unlock()

		for addr, rgs := range pendingCompletes {
			i.incTransferer.RequestComplete(context.Background(), rgs, addr)
		}
	}()

	return nil
}

// leaveIncrementalTransfer will attempt to incrementally send chunks to
// neighboring lifecyclers that should contain data for token ranges the
// leaving lifecycler will no longer receive writes for.
func (i *Lifecycler) leaveIncrementalTransfer(ctx context.Context) error {
	// Make sure all tokens are set to leaving, even when we
	// fail.
	defer func() {
		i.checkRemainingTokens()

		i.setTokens(nil)
		i.setState(LEAVING)
		i.updateConsul(ctx)
	}()

	r := i.getLastRing()
	navigator := r.GetNavigator()

	replicationFactor := i.cfg.RingConfig.ReplicationFactor
	if active := r.FindIngestersByState(ACTIVE); len(active) <= replicationFactor {
		return fmt.Errorf("not transferring out; number of ingesters less than or equal to replication factor")
	}

	success := atomic.NewBool(true)

	i.setTransitioningTokens(i.getTokens())
	tokens := i.getTransitioningTokens()

	for _, token := range tokens {
		healthy := func(t TokenDesc) bool {
			// When we're leaving the ring, any of our tokens > our current token
			// are "healthy" (i.e., they are still in a "ACTIVE" state).
			if t.Ingester == i.ID {
				return t.Token > token
			}

			ing := r.Ingesters[t.Ingester]
			return ing.IsHealthy(Write, i.cfg.RingConfig.HeartbeatTimeout)
		}

		workload, ok := i.findTransferWorkload(r, navigator, token, healthy)
		if !ok {
			success.Store(false)
		}

		workload.Do(func(addr string, ranges []TokenRange) {
			err := i.incTransferer.SendChunkRanges(ctx, ranges, addr)
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to send chunks", "target_addr", addr, "ranges", PrintableRanges(ranges), "err", err)
				success.Store(false)
			}
		})

		// Remove the token from the ring.
		i.removeToken(token)
		if err := i.updateConsul(ctx); err != nil {
			level.Error(util.Logger).Log("msg",
				fmt.Sprintf("failed to update consul when removing token %d", token))
		} else {
			// Update the latest copy of the ring - if the leave is slow, we need to
			// keep our copy of the ring updates so ingesters aren't considered
			// unhealthy.
			r = i.getLastRing()
		}
	}

	if !success.Load() {
		return fmt.Errorf("incremental transfer out incomplete")
	}
	return nil
}

func (i *Lifecycler) checkRemainingTokens() {
	remainingTokens := i.incTransferer.MemoryStreamTokens()
	if len(remainingTokens) == 0 {
		return
	}

	level.Warn(util.Logger).Log("msg", "not all tokens transferred out", "streams_remaining", len(remainingTokens))

	printTokens := remainingTokens
	if len(printTokens) > 20 {
		printTokens = printTokens[:20]
	}

	level.Debug(util.Logger).Log("msg", "non-transferred tokens", "tokens", printTokens)
}
