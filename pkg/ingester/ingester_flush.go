package ingester

import (
	"fmt"
	"net/http"
	"time"

	"golang.org/x/net/context"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/util"
)

const (
	// Backoff for retrying 'immediate' flushes. Only counts for queue
	// position, not wallclock time.
	flushBackoff = 1 * time.Second
)

// FlushHandler triggers a flush of all in memory chunks.  Mainly used for
// local testing.
func (i *Ingester) FlushHandler(w http.ResponseWriter, r *http.Request) {
	i.sweepUsers(true)
	w.WriteHeader(http.StatusNoContent)
}

type flushOp struct {
	from      model.Time
	userID    string
	fp        model.Fingerprint
	immediate bool
}

func (o *flushOp) Key() string {
	return fmt.Sprintf("%s-%d-%v", o.userID, o.fp, o.immediate)
}

func (o *flushOp) Priority() int64 {
	return -int64(o.from)
}

// sweepUsers periodically schedules series for flushing and garbage collects users with no series
func (i *Ingester) sweepUsers(immediate bool) {
	if i.chunkStore == nil {
		return
	}

	for id, state := range i.userStates.cp() {
		for pair := range state.fpToSeries.iter() {
			state.fpLocker.Lock(pair.fp)
			i.sweepSeries(id, pair.fp, pair.series, immediate)
			state.fpLocker.Unlock(pair.fp)
		}
	}
}

type flushReason int

const (
	noFlush = iota
	reasonImmediate
	reasonMultipleChunksInSeries
	reasonAged
	reasonIdle
)

// sweepSeries schedules a series for flushing based on a set of criteria
//
// NB we don't close the head chunk here, as the series could wait in the queue
// for some time, and we want to encourage chunks to be as full as possible.
func (i *Ingester) sweepSeries(userID string, fp model.Fingerprint, series *memorySeries, immediate bool) {
	if len(series.chunkDescs) <= 0 {
		return
	}

	firstTime := series.firstTime()
	flush := i.shouldFlushSeries(series, immediate)

	if flush != noFlush {
		flushQueueIndex := int(uint64(fp) % uint64(i.cfg.ConcurrentFlushes))
		if i.flushQueues[flushQueueIndex].Enqueue(&flushOp{firstTime, userID, fp, immediate}) {
			util.Event().Log("msg", "add to flush queue", "userID", userID, "reason", flush, "firstTime", firstTime, "fp", fp, "series", series.metric, "queue", flushQueueIndex)
		}
	}
}

func (i *Ingester) shouldFlushSeries(series *memorySeries, immediate bool) flushReason {
	if immediate {
		return reasonImmediate
	}
	// Series should be scheduled for flushing if they have more than one chunk
	if len(series.chunkDescs) > 1 {
		return reasonMultipleChunksInSeries
	}

	// Or if the only existing chunk need flushing
	if len(series.chunkDescs) > 0 {
		return i.shouldFlushChunk(series.chunkDescs[0])
	}

	return noFlush
}

func (i *Ingester) shouldFlushChunk(c *desc) flushReason {
	// Chunks should be flushed if they span longer than MaxChunkAge
	if c.LastTime.Sub(c.FirstTime) > i.cfg.MaxChunkAge {
		return reasonAged
	}

	// Chunk should be flushed if their last update is older then MaxChunkIdle
	if model.Now().Sub(c.LastUpdate) > i.cfg.MaxChunkIdle {
		return reasonIdle
	}

	return noFlush
}

func (i *Ingester) flushLoop(j int) {
	defer func() {
		level.Debug(util.Logger).Log("msg", "Ingester.flushLoop() exited")
		i.flushQueuesDone.Done()
	}()

	for {
		o := i.flushQueues[j].Dequeue()
		if o == nil {
			return
		}
		op := o.(*flushOp)

		err := i.flushUserSeries(j, op.userID, op.fp, op.immediate)
		if err != nil {
			level.Error(util.WithUserID(op.userID, util.Logger)).Log("msg", "failed to flush user", "err", err)
		}

		// If we're exiting & we failed to flush, put the failed operation
		// back in the queue at a later point.
		if op.immediate && err != nil {
			op.from = op.from.Add(flushBackoff)
			i.flushQueues[j].Enqueue(op)
		}
	}
}

func (i *Ingester) flushUserSeries(flushQueueIndex int, userID string, fp model.Fingerprint, immediate bool) error {
	if i.preFlushUserSeries != nil {
		i.preFlushUserSeries()
	}

	userState, ok := i.userStates.get(userID)
	if !ok {
		return nil
	}

	series, ok := userState.fpToSeries.get(fp)
	if !ok {
		return nil
	}

	userState.fpLocker.Lock(fp)
	reason := i.shouldFlushSeries(series, immediate)
	if reason == noFlush {
		userState.fpLocker.Unlock(fp)
		return nil
	}

	// Assume we're going to flush everything, and maybe don't flush the head chunk if it doesn't need it.
	chunks := series.chunkDescs
	if immediate || (len(chunks) > 0 && i.shouldFlushChunk(series.head()) != noFlush) {
		series.closeHead()
	} else {
		chunks = chunks[:len(chunks)-1]
	}
	userState.fpLocker.Unlock(fp)

	if len(chunks) == 0 {
		return nil
	}

	// flush the chunks without locking the series, as we don't want to hold the series lock for the duration of the dynamo/s3 rpcs.
	ctx := user.InjectOrgID(context.Background(), userID)
	ctx, cancel := context.WithTimeout(ctx, i.cfg.FlushOpTimeout)
	defer cancel() // releases resources if slowOperation completes before timeout elapses

	util.Event().Log("msg", "flush chunks", "userID", userID, "reason", reason, "numChunks", len(chunks), "firstTime", chunks[0].FirstTime, "fp", fp, "series", series.metric, "queue", flushQueueIndex)
	err := i.flushChunks(ctx, fp, series.metric, chunks)
	if err != nil {
		return err
	}

	// now remove the chunks
	userState.fpLocker.Lock(fp)
	series.chunkDescs = series.chunkDescs[len(chunks):]
	i.memoryChunks.Sub(float64(len(chunks)))
	if len(series.chunkDescs) == 0 {
		userState.removeSeries(fp, series.metric)
	}
	userState.fpLocker.Unlock(fp)
	return nil
}

func (i *Ingester) flushChunks(ctx context.Context, fp model.Fingerprint, metric model.Metric, chunkDescs []*desc) error {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return err
	}

	wireChunks := make([]chunk.Chunk, 0, len(chunkDescs))
	for _, chunkDesc := range chunkDescs {
		wireChunks = append(wireChunks, chunk.NewChunk(userID, fp, metric, chunkDesc.C, chunkDesc.FirstTime, chunkDesc.LastTime))
	}

	if err := i.chunkStore.Put(ctx, wireChunks); err != nil {
		return err
	}

	// Record statistsics only when actual put request did not return error.
	for _, chunkDesc := range chunkDescs {
		utilization, length := chunkDesc.C.Utilization(), chunkDesc.C.Len()
		util.Event().Log("msg", "chunk flushed", "userID", userID, "fp", fp, "series", metric, "utilization", utilization, "length", length, "firstTime", chunkDesc.FirstTime, "lastTime", chunkDesc.LastTime)
		i.chunkUtilization.Observe(utilization)
		i.chunkLength.Observe(float64(length))
		i.chunkAge.Observe(model.Now().Sub(chunkDesc.FirstTime).Seconds())
	}

	return nil
}
