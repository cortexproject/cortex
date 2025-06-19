package tsdb

import (
	"context"
	"errors"
	"slices"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
)

type multiLevelCache struct {
	postingsCaches, seriesCaches, expandedPostingCaches []storecache.IndexCache

	fetchLatency         *prometheus.HistogramVec
	backFillLatency      *prometheus.HistogramVec
	backfillProcessor    *cacheutil.AsyncOperationProcessor
	backfillDroppedItems map[string]prometheus.Counter
	storeDroppedItems    map[string]prometheus.Counter

	maxBackfillItems int
}

func (m *multiLevelCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	for _, c := range m.postingsCaches {
		cache := c
		if err := m.backfillProcessor.EnqueueAsync(func() {
			cache.StorePostings(blockID, l, v, tenant)
		}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
			m.storeDroppedItems[storecache.CacheTypePostings].Inc()
		}
	}
}

func (m *multiLevelCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(storecache.CacheTypePostings))
	defer timer.ObserveDuration()

	misses = keys
	hits = map[labels.Label][]byte{}
	backfillItems := make([]map[labels.Label][]byte, len(m.postingsCaches)-1)
	for i, c := range m.postingsCaches {
		if i < len(m.postingsCaches)-1 {
			backfillItems[i] = map[labels.Label][]byte{}
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
			backfillItems[i-1] = h
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(storecache.CacheTypePostings))
		defer backFillTimer.ObserveDuration()
		for i, values := range backfillItems {
			i := i
			values := values
			if len(values) == 0 {
				continue
			}
			if err := m.backfillProcessor.EnqueueAsync(func() {
				cnt := 0
				for lbl, b := range values {
					m.postingsCaches[i].StorePostings(blockID, lbl, b, tenant)
					cnt++
					if cnt == m.maxBackfillItems {
						m.backfillDroppedItems[storecache.CacheTypePostings].Add(float64(len(values) - cnt))
						return
					}
				}
			}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
				m.backfillDroppedItems[storecache.CacheTypePostings].Add(float64(len(values)))
			}
		}
	}()

	return hits, misses
}

func (m *multiLevelCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	for _, c := range m.expandedPostingCaches {
		cache := c
		if err := m.backfillProcessor.EnqueueAsync(func() {
			cache.StoreExpandedPostings(blockID, matchers, v, tenant)
		}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
			m.storeDroppedItems[storecache.CacheTypeExpandedPostings].Inc()
		}
	}
}

func (m *multiLevelCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(storecache.CacheTypeExpandedPostings))
	defer timer.ObserveDuration()

	for i, c := range m.expandedPostingCaches {
		if ctx.Err() != nil {
			return nil, false
		}
		if d, h := c.FetchExpandedPostings(ctx, blockID, matchers, tenant); h {
			if i > 0 {
				backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(storecache.CacheTypeExpandedPostings))
				if err := m.backfillProcessor.EnqueueAsync(func() {
					m.expandedPostingCaches[i-1].StoreExpandedPostings(blockID, matchers, d, tenant)
				}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
					m.backfillDroppedItems[storecache.CacheTypeExpandedPostings].Inc()
				}
				backFillTimer.ObserveDuration()
			}
			return d, h
		}
	}

	return []byte{}, false
}

func (m *multiLevelCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	for _, c := range m.seriesCaches {
		cache := c
		if err := m.backfillProcessor.EnqueueAsync(func() {
			cache.StoreSeries(blockID, id, v, tenant)
		}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
			m.storeDroppedItems[storecache.CacheTypeSeries].Inc()
		}
	}
}

func (m *multiLevelCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(m.fetchLatency.WithLabelValues(storecache.CacheTypeSeries))
	defer timer.ObserveDuration()

	misses = ids
	hits = map[storage.SeriesRef][]byte{}
	backfillItems := make([]map[storage.SeriesRef][]byte, len(m.seriesCaches)-1)

	for i, c := range m.seriesCaches {
		if i < len(m.seriesCaches)-1 {
			backfillItems[i] = map[storage.SeriesRef][]byte{}
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
			backfillItems[i-1] = h
		}

		if len(misses) == 0 {
			break
		}
	}

	defer func() {
		backFillTimer := prometheus.NewTimer(m.backFillLatency.WithLabelValues(storecache.CacheTypeSeries))
		defer backFillTimer.ObserveDuration()
		for i, values := range backfillItems {
			i := i
			values := values
			if len(values) == 0 {
				continue
			}
			if err := m.backfillProcessor.EnqueueAsync(func() {
				cnt := 0
				for ref, b := range values {
					m.seriesCaches[i].StoreSeries(blockID, ref, b, tenant)
					cnt++
					if cnt == m.maxBackfillItems {
						m.backfillDroppedItems[storecache.CacheTypeSeries].Add(float64(len(values) - cnt))
						return
					}
				}
			}); errors.Is(err, cacheutil.ErrAsyncBufferFull) {
				m.backfillDroppedItems[storecache.CacheTypeSeries].Add(float64(len(values)))
			}
		}
	}()

	return hits, misses
}

func filterCachesByItem(enabledItems [][]string, cachedItem string, c ...storecache.IndexCache) []storecache.IndexCache {
	filteredCaches := make([]storecache.IndexCache, 0, len(c))
	for i := range enabledItems {
		if len(enabledItems[i]) == 0 || slices.Contains(enabledItems[i], cachedItem) {
			filteredCaches = append(filteredCaches, c[i])
		}
	}
	return filteredCaches
}

func newMultiLevelCache(reg prometheus.Registerer, cfg MultiLevelIndexCacheConfig, enabledItems [][]string, c ...storecache.IndexCache) storecache.IndexCache {
	if len(c) == 1 {
		if len(enabledItems[0]) == 0 {
			return c[0]
		}
		return storecache.NewFilteredIndexCache(c[0], enabledItems[0])
	}

	backfillDroppedItems := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_store_multilevel_index_cache_backfill_dropped_items_total",
		Help: "Total number of items dropped due to async buffer full when backfilling multilevel cache ",
	}, []string{"item_type"})
	storeDroppedItems := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_store_multilevel_index_cache_store_dropped_items_total",
		Help: "Total number of items dropped due to async buffer full when storing multilevel cache ",
	}, []string{"item_type"})
	return &multiLevelCache{
		postingsCaches:        filterCachesByItem(enabledItems, storecache.CacheTypePostings, c...),
		seriesCaches:          filterCachesByItem(enabledItems, storecache.CacheTypeSeries, c...),
		expandedPostingCaches: filterCachesByItem(enabledItems, storecache.CacheTypeExpandedPostings, c...),
		backfillProcessor:     cacheutil.NewAsyncOperationProcessor(cfg.MaxAsyncBufferSize, cfg.MaxAsyncConcurrency),
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
		backfillDroppedItems: map[string]prometheus.Counter{
			storecache.CacheTypePostings:         backfillDroppedItems.WithLabelValues(storecache.CacheTypePostings),
			storecache.CacheTypeSeries:           backfillDroppedItems.WithLabelValues(storecache.CacheTypeSeries),
			storecache.CacheTypeExpandedPostings: backfillDroppedItems.WithLabelValues(storecache.CacheTypeExpandedPostings),
		},
		storeDroppedItems: map[string]prometheus.Counter{
			storecache.CacheTypePostings:         storeDroppedItems.WithLabelValues(storecache.CacheTypePostings),
			storecache.CacheTypeSeries:           storeDroppedItems.WithLabelValues(storecache.CacheTypeSeries),
			storecache.CacheTypeExpandedPostings: storeDroppedItems.WithLabelValues(storecache.CacheTypeExpandedPostings),
		},
		maxBackfillItems: cfg.MaxBackfillItems,
	}
}
