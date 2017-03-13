// Responsible for managing the ingester lifecycle.

package ingester

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"golang.org/x/net/context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex"
	"github.com/weaveworks/cortex/ingester/client"
	"github.com/weaveworks/cortex/ring"
	"github.com/weaveworks/cortex/util"
)

const (
	infName                 = "eth0"
	minReadyDuration        = 1 * time.Minute
	pendingSearchIterations = 10
)

var (
	consulHeartbeats = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cortex_ingester_consul_heartbeats_total",
		Help: "The total number of heartbeats sent to consul.",
	})
)

func init() {
	prometheus.MustRegister(consulHeartbeats)
}

// ReadinessHandler is used to indicate to k8s when the ingesters are ready for
// the addition removal of another ingester. Returns 204 when the ingester is
// ready, 500 otherwise.
func (i *Ingester) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	if i.isReady() {
		w.WriteHeader(http.StatusNoContent)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (i *Ingester) isReady() bool {
	i.readyLock.Lock()
	defer i.readyLock.Unlock()

	if i.ready {
		return true
	}

	// Ingester always take at least minReadyDuration to become ready to work
	// around race conditions with ingesters exiting and updating the ring
	if time.Now().Sub(i.startTime) < minReadyDuration {
		return false
	}

	ringDesc, err := i.consul.Get(ring.ConsulKey)
	if err != nil {
		log.Error("Error talking to consul: %v", err)
		return false
	}

	i.ready = i.ready || ringDesc.(*ring.Desc).Ready(i.cfg.ringConfig.HeartbeatTimeout)
	return i.ready
}

// ChangeState of the ingester, for use off of the loop() goroutine.
func (i *Ingester) ChangeState(state ring.IngesterState) error {
	err := make(chan error)
	i.actorChan <- func() {
		err <- i.changeState(state)
	}
	return <-err
}

// ClaimTokensFor takes all the tokens for the supplied ingester and assigns them to this ingester.
func (i *Ingester) ClaimTokensFor(ingesterID string) error {
	err := make(chan error)

	i.actorChan <- func() {
		var tokens []uint32

		claimTokens := func(in interface{}) (out interface{}, retry bool, err error) {
			ringDesc, ok := in.(*ring.Desc)
			if !ok || ringDesc == nil {
				return nil, false, fmt.Errorf("Cannot claim tokens in an empty ring")
			}

			tokens = ringDesc.ClaimTokens(ingesterID, i.id)
			return ringDesc, true, nil
		}

		if err := i.consul.CAS(ring.ConsulKey, claimTokens); err != nil {
			log.Errorf("Failed to write to consul: %v", err)
		}

		i.tokens = tokens
		err <- nil
	}

	return <-err
}

// Shutdown stops the ingester.  It will:
// - send chunks to another ingester, if it can.
// - otherwise, flush chunks to the chunk store.
// - remove config from Consul.
// - block until we've successfully shutdown.
func (i *Ingester) Shutdown() {
	// This will prevent us accepting any more samples
	i.stopLock.Lock()
	i.stopped = true
	i.stopLock.Unlock()

	// closing i.quit triggers loop() to exit, which in turn will trigger
	// the removal of our tokens etc
	close(i.quit)

	i.done.Wait()
}

func (i *Ingester) loop() {
	defer func() {
		log.Infof("Ingester.loop() exited gracefully")
		i.done.Done()
	}()

	// First, see if we exist in the cluster, update our state to match if we do,
	// and add ourselves (without tokens) if we don't.
	if err := i.initRing(); err != nil {
		log.Fatalf("Failed to join consul: %v", err)
	}

	// We do various period tasks
	autoJoinAfter := time.After(i.cfg.JoinAfter)

	heartbeatTicker := time.NewTicker(i.cfg.HeartbeatPeriod)
	defer heartbeatTicker.Stop()

	flushTicker := time.NewTicker(i.cfg.FlushCheckPeriod)
	defer flushTicker.Stop()

	rateUpdateTicker := time.NewTicker(i.cfg.userStatesConfig.RateUpdatePeriod)
	defer rateUpdateTicker.Stop()

loop:
	for {
		select {
		case <-autoJoinAfter:
			// Will only fire once, after auto join timeout.  If we haven't entered "JOINING" state,
			// then pick some tokens and enter ACTIVE state.
			if i.state == ring.PENDING {
				log.Infof("Auto-joining cluster after timout.")
				if err := i.autoJoin(); err != nil {
					log.Fatalf("failed to pick tokens in consul: %v", err)
				}
			}

		case <-heartbeatTicker.C:
			consulHeartbeats.Inc()
			if err := i.updateConsul(); err != nil {
				log.Errorf("Failed to write to consul, sleeping: %v", err)
			}

		case <-flushTicker.C:
			i.sweepUsers(false)

		case <-rateUpdateTicker.C:
			i.userStates.updateRates()

		case f := <-i.actorChan:
			f()

		case <-i.quit:
			break loop
		}
	}

	// Mark ourselved as Leaving so no more samples are send to us.
	i.changeState(ring.LEAVING)

	flushRequired := true
	if i.cfg.ClaimOnRollout {
		if err := i.transferChunks(); err != nil {
			log.Fatalf("Failed to transfer chunks to another ingester: %v", err)
		}
		flushRequired = false
	}
	if flushRequired {
		i.flushAllChunks()
	}

	// Close the flush queues, will wait for chunks to be flushed.
	for _, flushQueue := range i.flushQueues {
		flushQueue.Close()
	}

	if !i.cfg.skipUnregister {
		if err := i.unregister(); err != nil {
			log.Fatalf("Failed to unregister from consul: %v", err)
		}
		log.Infof("Ingester removed from consul")
	}
}

// initRing is the first thing we do when we start. It:
// - add an ingester entry to the ring
// - copies out our state and tokens if they exist
func (i *Ingester) initRing() error {
	return i.consul.CAS(ring.ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *ring.Desc
		if in == nil {
			ringDesc = ring.NewDesc()
		} else {
			ringDesc = in.(*ring.Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.id]
		if !ok {
			// Either we are a new ingester, or consul must have restarted
			log.Infof("Entry not found in ring, adding with no tokens.")
			ringDesc.AddIngester(i.id, i.addr, []uint32{}, i.state)
			return ringDesc, true, nil
		}

		// We exist in the ring, so assume the ring is right and copy out tokens & state out of there.
		i.state = ingesterDesc.State
		i.tokens, _ = ringDesc.TokensFor(i.id)

		log.Infof("Existing entry found in ring with state=%s, tokens=%v.", i.state, i.tokens)
		return ringDesc, true, nil
	})
}

// autoJoin selects random tokens & moves state to ACTIVE
func (i *Ingester) autoJoin() error {
	return i.consul.CAS(ring.ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *ring.Desc
		if in == nil {
			ringDesc = ring.NewDesc()
		} else {
			ringDesc = in.(*ring.Desc)
		}

		// At this point, we should not have any tokens, and we should be in PENDING state.
		myTokens, takenTokens := ringDesc.TokensFor(i.id)
		if len(myTokens) > 0 {
			log.Errorf("%d tokens already exist for this ingester - wasn't expecting any!", len(myTokens))
		}

		newTokens := ring.GenerateTokens(i.cfg.NumTokens-len(myTokens), takenTokens)
		i.state = ring.ACTIVE
		ringDesc.AddIngester(i.id, i.addr, newTokens, i.state)

		tokens := append(myTokens, newTokens...)
		sort.Sort(sortableUint32(tokens))
		i.tokens = tokens

		return ringDesc, true, nil
	})
}

// updateConsul updates our entries in consul, heartbeating and dealing with
// consul restarts.
func (i *Ingester) updateConsul() error {
	return i.consul.CAS(ring.ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		var ringDesc *ring.Desc
		if in == nil {
			ringDesc = ring.NewDesc()
		} else {
			ringDesc = in.(*ring.Desc)
		}

		ingesterDesc, ok := ringDesc.Ingesters[i.id]
		if !ok {
			// consul must have restarted
			log.Infof("Found empty ring, inserting tokens!")
			ringDesc.AddIngester(i.id, i.addr, i.tokens, i.state)
		} else {
			ingesterDesc.Timestamp = time.Now().Unix()
			ingesterDesc.State = i.state
			ingesterDesc.Addr = i.addr
			ringDesc.Ingesters[i.id] = ingesterDesc
		}

		return ringDesc, true, nil
	})
}

// changeState updates consul with state transitions for us.  NB this must be
// called from loop()!  Use ChangeState for calls from outside of loop().
func (i *Ingester) changeState(state ring.IngesterState) error {
	// Only the following state transitions can be triggered externally
	if !((i.state == ring.PENDING && state == ring.JOINING) || // triggered by ClaimStart
		(i.state == ring.PENDING && state == ring.ACTIVE) || // triggered by autoJoin
		(i.state == ring.JOINING && state == ring.ACTIVE) || // triggered by ClaimFinish
		(i.state == ring.ACTIVE && state == ring.LEAVING)) { // triggered by shutdown
		return fmt.Errorf("Changing ingester state from %v -> %v is disallowed", i.state, state)
	}

	log.Infof("Changing ingester state from %v -> %v", i.state, state)
	i.state = state
	return i.updateConsul()
}

// transferChunks finds an ingester in PENDING state and transfers our chunks
// to it.
func (i *Ingester) transferChunks() error {
	targetIngester := i.findTargetIngester()
	if targetIngester == nil {
		return fmt.Errorf("cannot find ingester to transfer chunks to")
	}

	log.Infof("Sending chunks to %v", targetIngester.Addr)
	client, err := client.MakeIngesterClient(targetIngester.Addr, i.cfg.SearchPendingFor)
	if err != nil {
		return err
	}
	defer client.(io.Closer).Close()

	ctx := user.Inject(context.Background(), "-1")
	stream, err := client.TransferChunks(ctx)
	if err != nil {
		return err
	}

	for userID, state := range i.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)

			chunks, err := toWireChunks(pair.series.chunkDescs)
			if err != nil {
				state.fpLocker.Unlock(pair.fp)
				return err
			}

			err = stream.Send(&cortex.TimeSeriesChunk{
				FromIngesterId: i.id,
				UserId:         userID,
				Labels:         util.ToLabelPairs(pair.series.metric),
				Chunks:         chunks,
			})
			state.fpLocker.Unlock(pair.fp)
			if err != nil {
				return err
			}

			sentChunks.Add(float64(len(chunks)))
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return err
	}

	return nil
}

// findTargetIngester finds an ingester in PENDING state.
func (i *Ingester) findTargetIngester() *ring.IngesterDesc {
	for j := 0; j < pendingSearchIterations; j++ {
		ringDesc, err := i.consul.Get(ring.ConsulKey)
		if err != nil {
			log.Errorf("Error talking to consul: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		ingesters := ringDesc.(*ring.Desc).FindIngestersByState(ring.PENDING)
		if len(ingesters) <= 0 {
			log.Warnf("No pending ingesters found...")
			time.Sleep(i.cfg.SearchPendingFor / pendingSearchIterations)
			continue
		}

		return ingesters[0]
	}
	return nil
}

// flushChunks writes all remaining chunks to the chunkStore,
func (i *Ingester) flushAllChunks() {
	i.sweepUsers(true)
}

// unregister removes our entry from consul.
func (i *Ingester) unregister() error {
	return i.consul.CAS(ring.ConsulKey, func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*ring.Desc)
		ringDesc.RemoveIngester(i.id)
		return ringDesc, true, nil
	})
}
