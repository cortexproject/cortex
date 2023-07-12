package tsdb

import (
	"context"
	"sync"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type multiLevelCache struct {
	caches []storecache.IndexCache
}

func (m *multiLevelCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StorePostings(blockID, l, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	misses = keys
	hits = map[labels.Label][]byte{}
	for _, c := range m.caches {
		h, m := c.FetchMultiPostings(ctx, blockID, misses)
		misses = m

		for label, bytes := range h {
			hits[label] = bytes
		}
		if len(misses) == 0 {
			break
		}
	}

	return hits, misses
}

func (m *multiLevelCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreExpandedPostings(blockID, matchers, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher) ([]byte, bool) {
	for _, c := range m.caches {
		if d, h := c.FetchExpandedPostings(ctx, blockID, matchers); h {
			return d, h
		}
	}

	return []byte{}, false
}

func (m *multiLevelCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreSeries(blockID, id, v)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	misses = ids
	hits = map[storage.SeriesRef][]byte{}
	for _, c := range m.caches {
		h, m := c.FetchMultiSeries(ctx, blockID, misses)
		misses = m

		for label, bytes := range h {
			hits[label] = bytes
		}
		if len(misses) == 0 {
			break
		}
	}

	return hits, misses
}

func newMultiLevelCache(c ...storecache.IndexCache) storecache.IndexCache {
	if len(c) == 1 {
		return c[0]
	}
	return &multiLevelCache{
		caches: c,
	}
}
