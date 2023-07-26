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
	backfillMap := map[storecache.IndexCache][]map[labels.Label][]byte{}
	for i, c := range m.caches {
		backfillMap[c] = []map[labels.Label][]byte{}
		h, mi := c.FetchMultiPostings(ctx, blockID, misses)
		misses = mi

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 {
			backfillMap[m.caches[i-1]] = append(backfillMap[m.caches[i-1]], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		for cache, hit := range backfillMap {
			for _, values := range hit {
				for l, b := range values {
					cache.StorePostings(blockID, l, b)
				}
			}
		}
	}()

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
	for i, c := range m.caches {
		if d, h := c.FetchExpandedPostings(ctx, blockID, matchers); h {
			if i > 0 {
				m.caches[i-1].StoreExpandedPostings(blockID, matchers, d)
			}
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
	backfillMap := map[storecache.IndexCache][]map[storage.SeriesRef][]byte{}

	for i, c := range m.caches {
		backfillMap[c] = []map[storage.SeriesRef][]byte{}
		h, miss := c.FetchMultiSeries(ctx, blockID, misses)
		misses = miss

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 && len(h) > 0 {
			backfillMap[m.caches[i-1]] = append(backfillMap[m.caches[i-1]], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		for cache, hit := range backfillMap {
			for _, values := range hit {
				for m, b := range values {
					cache.StoreSeries(blockID, m, b)
				}
			}
		}
	}()

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
