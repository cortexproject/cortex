package tsdb

import (
	"context"
	"errors"
	"sync"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

const (
	cacheTypePostings         string = "Postings"
	cacheTypeExpandedPostings string = "ExpandedPostings"
	cacheTypeSeries           string = "Series"
)

type multiLevelCache struct {
	caches []storecache.IndexCache

	fetchLatency         *prometheus.HistogramVec
	backFillLatency      *prometheus.HistogramVec
	backfillProcessor    *cacheutil.AsyncOperationProcessor
	backfillDroppedItems *prometheus.CounterVec
}

func (m *multiLevelCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StorePostings(blockID, l, v, tenant)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(cacheTypePostings))
	defer timer.ObserveDuration()

	misses = keys
	hits = map[labels.Label][]byte{}
	backfillItems := make([][]map[labels.Label][]byte, len(m.caches)-1)
	for i, c := range m.caches {
		if i < len(m.caches)-1 {
			backfillItems[i] = []map[labels.Label][]byte{}
		}
		if ctx.Err() != nil {
			return
		}
		h, mi := c.FetchMultiPostings(ctx, blockID, misses, tenant)
		misses = mi

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 {
			backfillItems[i-1] = append(backfillItems[i-1], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(cacheTypePostings))
		defer backFillTimer.ObserveDuration()
		for i, hit := range backfillItems {
			for _, values := range hit {
				for lbl, b := range values {
					if err := m.backfillProcessor.EnqueueAsync(func() {
						m.caches[i].StorePostings(blockID, lbl, b, tenant)
					}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
						m.backfillDroppedItems.WithLabelValues(cacheTypePostings).Inc()
					}
				}
			}
		}
	}()

	return hits, misses
}

func (m *multiLevelCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreExpandedPostings(blockID, matchers, v, tenant)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(cacheTypeExpandedPostings))
	defer timer.ObserveDuration()

	for i, c := range m.caches {
		if ctx.Err() != nil {
			return nil, false
		}
		if d, h := c.FetchExpandedPostings(ctx, blockID, matchers, tenant); h {
			if i > 0 {
				backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(cacheTypeExpandedPostings))
				if err := m.backfillProcessor.EnqueueAsync(func() {
					m.caches[i-1].StoreExpandedPostings(blockID, matchers, d, tenant)
				}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
					m.backfillDroppedItems.WithLabelValues(cacheTypeExpandedPostings).Inc()
				}
				backFillTimer.ObserveDuration()
			}
			return d, h
		}
	}

	return []byte{}, false
}

func (m *multiLevelCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	wg := sync.WaitGroup{}
	wg.Add(len(m.caches))
	for _, c := range m.caches {
		cache := c
		go func() {
			defer wg.Done()
			cache.StoreSeries(blockID, id, v, tenant)
		}()
	}
	wg.Wait()
}

func (m *multiLevelCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(cacheTypeSeries))
	defer timer.ObserveDuration()

	misses = ids
	hits = map[storage.SeriesRef][]byte{}
	backfillItems := make([][]map[storage.SeriesRef][]byte, len(m.caches)-1)

	for i, c := range m.caches {
		if i < len(m.caches)-1 {
			backfillItems[i] = []map[storage.SeriesRef][]byte{}
		}
		if ctx.Err() != nil {
			return
		}
		h, miss := c.FetchMultiSeries(ctx, blockID, misses, tenant)
		misses = miss

		for label, bytes := range h {
			hits[label] = bytes
		}

		if i > 0 && len(h) > 0 {
			backfillItems[i-1] = append(backfillItems[i-1], h)
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(cacheTypeSeries))
		defer backFillTimer.ObserveDuration()
		for i, hit := range backfillItems {
			for _, values := range hit {
				for ref, b := range values {
					if err := m.backfillProcessor.EnqueueAsync(func() {
						m.caches[i].StoreSeries(blockID, ref, b, tenant)
					}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
						m.backfillDroppedItems.WithLabelValues(cacheTypeSeries).Inc()
					}
				}
			}
		}
	}()

	return hits, misses
}

func newMultiLevelCache(reg prometheus.Registerer, cfg MultiLevelIndexCacheConfig, c ...storecache.IndexCache) storecache.IndexCache {
	if len(c) == 1 {
		return c[0]
	}

	return &multiLevelCache{
		caches:            c,
		backfillProcessor: cacheutil.NewAsyncOperationProcessor(cfg.MaxAsyncBufferSize, cfg.MaxAsyncConcurrency),
		fetchLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_store_multilevel_index_cache_fetch_duration_seconds",
			Help:    "Histogram to track latency to fetch items from multi level index cache",
			Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 10, 15, 20, 25, 30, 40, 50, 60, 90},
		}, []string{"item_type"}),
		backFillLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_store_multilevel_index_cache_backfill_duration_seconds",
			Help:    "Histogram to track latency to backfill items from multi level index cache",
			Buckets: []float64{0.01, 0.1, 0.3, 0.6, 1, 3, 6, 10, 15, 20, 25, 30, 40, 50, 60, 90},
		}, []string{"item_type"}),
		backfillDroppedItems: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_store_multilevel_index_cache_backfill_dropped_items_total",
			Help: "Total number of items dropped due to async buffer full when backfilling multilevel cache ",
		}, []string{"item_type"}),
	}
}
