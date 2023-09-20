package ring

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/atomic"
	"google.golang.org/grpc/status"

	"github.com/cortexproject/cortex/pkg/util/httpgrpcutil"
)

type batchTracker struct {
	rpcsPending atomic.Int32
	rpcsFailed  atomic.Int32
	done        chan struct{}
	err         chan error
}

type instance struct {
	desc         InstanceDesc
	itemTrackers []*itemTracker
	indexes      []int
}

type itemTracker struct {
	minSuccess  int
	maxFailures int
	succeeded   atomic.Int32
	failed4xx   atomic.Int32
	failed5xx   atomic.Int32
	remaining   atomic.Int32
	err4xx      atomic.Error
	err5xx      atomic.Error
}

func (i *itemTracker) recordError(err error) int32 {

	if status, ok := status.FromError(err); ok && status.Code()/100 == 4 {
		i.err4xx.Store(err)
		return i.failed4xx.Inc()
	}

	i.err5xx.Store(err)
	return i.failed5xx.Inc()
}

func (i *itemTracker) getError() error {
	if i.failed5xx.Load() > i.failed4xx.Load() {
		return i.err5xx.Load()
	}

	return i.err4xx.Load()
}

// DoBatch request against a set of keys in the ring, handling replication and
// failures. For example if we want to write N items where they may all
// hit different instances, and we want them all replicated R ways with
// quorum writes, we track the relationship between batch RPCs and the items
// within them.
//
// Callback is passed the instance to target, and the indexes of the keys
// to send to that instance.
//
// cleanup() is always called, either on an error before starting the batches or after they all finish.
//
// Not implemented as a method on Ring so we can test separately.
func DoBatch(ctx context.Context, op Operation, r ReadRing, keys []uint32, callback func(InstanceDesc, []int) error, cleanup func()) error {
	if r.InstancesCount() <= 0 {
		cleanup()
		return fmt.Errorf("DoBatch: InstancesCount <= 0")
	}

	expectedTrackers := len(keys) * (r.ReplicationFactor() + 1) / r.InstancesCount()
	itemTrackers := make([]itemTracker, len(keys))
	instances := make(map[string]instance, r.InstancesCount())

	var (
		bufDescs [GetBufferSize]InstanceDesc
		bufHosts [GetBufferSize]string
		bufZones = make(map[string]int, GetZoneSize)
	)
	for i, key := range keys {
		replicationSet, err := r.Get(key, op, bufDescs[:0], bufHosts[:0], bufZones)
		if err != nil {
			cleanup()
			return err
		}
		itemTrackers[i].minSuccess = len(replicationSet.Instances) - replicationSet.MaxErrors
		itemTrackers[i].maxFailures = replicationSet.MaxErrors
		itemTrackers[i].remaining.Store(int32(len(replicationSet.Instances)))

		for _, desc := range replicationSet.Instances {
			curr, found := instances[desc.Addr]
			if !found {
				curr.itemTrackers = make([]*itemTracker, 0, expectedTrackers)
				curr.indexes = make([]int, 0, expectedTrackers)
			}
			instances[desc.Addr] = instance{
				desc:         desc,
				itemTrackers: append(curr.itemTrackers, &itemTrackers[i]),
				indexes:      append(curr.indexes, i),
			}
		}
	}

	tracker := batchTracker{
		done: make(chan struct{}, 1),
		err:  make(chan error, 1),
	}
	tracker.rpcsPending.Store(int32(len(itemTrackers)))

	var wg sync.WaitGroup

	wg.Add(len(instances))
	for _, i := range instances {
		go func(i instance) {
			err := callback(i.desc, i.indexes)
			tracker.record(i, err)
			wg.Done()
		}(i)
	}

	// Perform cleanup at the end.
	go func() {
		wg.Wait()

		cleanup()
	}()

	select {
	case err := <-tracker.err:
		return err
	case <-tracker.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *batchTracker) record(instance instance, err error) {
	// If we reach the required number of successful puts on this sample, then decrement the
	// number of pending samples by one.
	//
	// The use of atomic increments here is needed as:
	// * rpcsPending and rpcsPending guarantees only a single sendSamples goroutine will write to either channel
	// * succeeded, failed4xx, failed5xx and remaining guarantees that the "return decision" is made atomically
	// avoiding race condition
	sampleTrackers := instance.itemTrackers
	for i := range sampleTrackers {
		if err != nil {
			// Track the number of errors by error family, and if it exceeds maxFailures
			// shortcut the waiting rpc.
			wrappedErr := httpgrpcutil.WrapHTTPGrpcError(err, "addr=%s state=%s zone=%s", instance.desc.Addr, instance.desc.State, instance.desc.Zone)
			errCount := instance.itemTrackers[i].recordError(wrappedErr)
			// We should return an error if we reach the maxFailure (quorum) on a given error family OR
			// we dont have any remaining ingesters to try
			// Ex: 2xx, 4xx, 5xx -> return 4xx
			// Ex: 2xx, 5xx, 4xx -> return 4xx
			// Ex: 4xx, 4xx, _ -> return 4xx
			// Ex: 5xx, _, 5xx -> return 5xx
			if errCount > int32(sampleTrackers[i].maxFailures) {
				if b.rpcsFailed.Inc() == 1 {
					b.err <- httpgrpcutil.WrapHTTPGrpcError(sampleTrackers[i].getError(), "maxFailure (quorum) on a given error family")
				}
				continue
			}
			if sampleTrackers[i].remaining.Dec() == 0 {
				if b.rpcsFailed.Inc() == 1 {
					b.err <- httpgrpcutil.WrapHTTPGrpcError(sampleTrackers[i].getError(), "not enough remaining instances to try")
				}
				continue
			}
		} else {
			// If we successfully push all samples to min success instances,
			// wake up the waiting rpc so it can return early.
			if sampleTrackers[i].succeeded.Inc() >= int32(sampleTrackers[i].minSuccess) {
				if b.rpcsPending.Dec() == 0 {
					b.done <- struct{}{}
				}
				continue
			}

			// If we succeeded to call this particular ingester but we dont have any remaining ingesters to try
			// and we did not succeeded calling `minSuccess` ingesters we need to return the last error
			// Ex: 4xx, 5xx, 2xx
			if sampleTrackers[i].remaining.Dec() == 0 {
				if b.rpcsFailed.Inc() == 1 {
					b.err <- httpgrpcutil.WrapHTTPGrpcError(sampleTrackers[i].getError(), "not enough remaining instances to try")
				}
			}
		}
	}
}
