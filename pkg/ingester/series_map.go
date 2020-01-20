package ingester

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
)

const seriesMapShards = 128

// seriesMap maps fingerprints to memory series. All its methods are
// goroutine-safe. A seriesMap is effectively is a goroutine-safe version of
// map[model.Fingerprint]*memorySeries.
type seriesMap struct {
	size   int32
	shards []shard
}

type shard struct {
	mtx sync.Mutex
	m   map[model.Fingerprint]*MemorySeries
	// Align this struct.
	_ [cacheLineSize - unsafe.Sizeof(sync.Mutex{}) - unsafe.Sizeof(map[model.Fingerprint]*MemorySeries{})]byte
}

// FingerprintSeriesPair pairs a fingerprint with a memorySeries pointer.
type FingerprintSeriesPair struct {
	Fingerprint model.Fingerprint
	Series      *MemorySeries
}

// newSeriesMap returns a newly allocated empty seriesMap. To create a seriesMap
// based on a prefilled map, use an explicit initializer.
func newSeriesMap() *seriesMap {
	shards := make([]shard, seriesMapShards)
	for i := 0; i < seriesMapShards; i++ {
		shards[i].m = map[model.Fingerprint]*MemorySeries{}
	}
	return &seriesMap{
		shards: shards,
	}
}

// get returns a memorySeries for a fingerprint. Return values have the same
// semantics as the native Go map.
func (sm *seriesMap) get(fp model.Fingerprint) (*MemorySeries, bool) {
	shard := &sm.shards[util.HashFP(fp)%seriesMapShards]
	shard.mtx.Lock()
	ms, ok := shard.m[fp]
	shard.mtx.Unlock()
	return ms, ok
}

// put adds a mapping to the seriesMap.
func (sm *seriesMap) put(fp model.Fingerprint, s *MemorySeries) {
	shard := &sm.shards[util.HashFP(fp)%seriesMapShards]
	shard.mtx.Lock()
	_, ok := shard.m[fp]
	shard.m[fp] = s
	shard.mtx.Unlock()

	if !ok {
		atomic.AddInt32(&sm.size, 1)
	}
}

// del removes a mapping from the series Map.
func (sm *seriesMap) del(fp model.Fingerprint) {
	shard := &sm.shards[util.HashFP(fp)%seriesMapShards]
	shard.mtx.Lock()
	_, ok := shard.m[fp]
	delete(shard.m, fp)
	shard.mtx.Unlock()
	if ok {
		atomic.AddInt32(&sm.size, -1)
	}
}

// Iter returns a channel that produces all mappings in the seriesMap. The
// channel will be closed once all fingerprints have been received. Not
// consuming all fingerprints from the channel will leak a goroutine. The
// semantics of concurrent modification of seriesMap is the similar as the one
// for iterating over a map with a 'range' clause. However, if the next element
// in iteration order is removed after the current element has been received
// from the channel, it will still be produced by the channel.
func (sm *seriesMap) Iter() <-chan FingerprintSeriesPair {
	ch := make(chan FingerprintSeriesPair)
	go func() {
		for i := range sm.shards {
			sm.shards[i].mtx.Lock()
			for fp, ms := range sm.shards[i].m {
				sm.shards[i].mtx.Unlock()
				ch <- FingerprintSeriesPair{fp, ms}
				sm.shards[i].mtx.Lock()
			}
			sm.shards[i].mtx.Unlock()
		}
		close(ch)
	}()
	return ch
}

func (sm *seriesMap) length() int {
	return int(atomic.LoadInt32(&sm.size))
}
