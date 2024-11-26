package tsdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	storecache "github.com/thanos-io/thanos/pkg/store/cache"
	"github.com/thanos-io/thanos/pkg/tenancy"
	"golang.org/x/sys/unix"

	"github.com/cortexproject/cortex/pkg/util"
)

const (
	defaultDataDir         = "./badger-index-cache"
	defaultGCInterval      = time.Minute * 5
	diskStatUpdateInterval = time.Second * 30
)

type BadgerIndexCache struct {
	logger           log.Logger
	cache            *badger.DB
	maxItemSizeBytes uint64
	indexTTL         time.Duration

	added    *prometheus.CounterVec
	overflow *prometheus.CounterVec

	commonMetrics *storecache.CommonMetrics

	// lastRetentionRun stores the timestamps of the latest retention loop run
	lastRetentionRun prometheus.Gauge
	// lastValueLogCleaned stores the timestamps of the latest ValueLogGC run
	lastValueLogCleaned prometheus.Gauge
	// valueLogSpaceAvailable stores the amount of space left on the value log mount point in bytes
	valueLogSpaceAvailable prometheus.Gauge
	// valueLogSpaceUsed stores the amount of space used by the value log
	valueLogSpaceUsed prometheus.Gauge
	// keyLogSpaceAvailable stores the amount of space left on the key log mount point in bytes
	keyLogSpaceAvailable prometheus.Gauge
	// keyLogSpaceUsed stores the amount of space used by the key log
	keyLogSpaceUsed prometheus.Gauge
}

func newBadgerIndexCache(logger log.Logger, commonMetrics *storecache.CommonMetrics, reg prometheus.Registerer, cfg BadgerIndexCacheConfig, ttl time.Duration) (*BadgerIndexCache, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = defaultDataDir
	}

	keysDataDir := fmt.Sprintf("%s/keys", cfg.DataDir)
	valuesDataDir := fmt.Sprintf("%s/values", cfg.DataDir)

	badgerOptions := badger.
		DefaultOptions("").
		WithLogger(&BadgerLogger{Logger: logger}).
		WithCompression(options.None).
		WithBlockCacheSize(0).       // only disk use
		WithValueDir(valuesDataDir). // value(vlog) dir
		WithDir(keysDataDir)         // key(lsm) dir

	bc, err := badger.Open(badgerOptions)
	if err != nil {
		return nil, err
	}

	if commonMetrics == nil {
		commonMetrics = storecache.NewCommonMetrics(reg)
	}

	maxItemSize := uint64(defaultMaxItemSize)

	c := &BadgerIndexCache{
		logger:           logger,
		cache:            bc,
		maxItemSizeBytes: maxItemSize,
		indexTTL:         ttl,
		commonMetrics:    commonMetrics,
	}
	c.lastRetentionRun = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_last_successful_retention_run_timestamp_seconds",
		Help: "Unix timestamp of the last successful badger index cache retention run.",
	})
	c.lastValueLogCleaned = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_last_value_log_cleaned_timestamp_seconds",
		Help: "Unix timestamp of the last successful badger value log gc run.",
	})
	c.valueLogSpaceAvailable = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_value_log_space_available_bytes",
		Help: "Amount of space left on the value log mount point in bytes.",
	})
	c.valueLogSpaceUsed = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_value_log_space_used_bytes",
		Help: "Amount of space used by the value log",
	})
	c.keyLogSpaceAvailable = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_key_log_space_available_bytes",
		Help: "Amount of space left on the key log mount point in bytes.",
	})
	c.keyLogSpaceUsed = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "badger_key_log_space_used_bytes",
		Help: "Amount of space used by the key log",
	})

	c.added = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_added_total",
		Help: "Total number of items that were added to the index cache.",
	}, []string{"item_type"})
	c.added.WithLabelValues(storecache.CacheTypePostings)
	c.added.WithLabelValues(storecache.CacheTypeSeries)
	c.added.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	c.overflow = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "thanos_store_index_cache_items_overflowed_total",
		Help: "Total number of items that could not be added to the cache due to being too big.",
	}, []string{"item_type"})
	c.overflow.WithLabelValues(storecache.CacheTypePostings)
	c.overflow.WithLabelValues(storecache.CacheTypeSeries)
	c.overflow.WithLabelValues(storecache.CacheTypeExpandedPostings)

	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenancy.DefaultTenant)
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenancy.DefaultTenant)

	level.Info(logger).Log(
		"msg", "created badger index cache",
		"maxItemSizeBytes", maxItemSize,
	)

	go c.retentionLoop(cfg.GCInterval, cfg.GCThreshold)
	go c.diskStatUpdateLoop(valuesDataDir, keysDataDir)

	return c, nil
}

type BadgerLogger struct {
	Logger log.Logger
}

func (l *BadgerLogger) Errorf(f string, v ...interface{}) {
	level.Error(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}
func (l *BadgerLogger) Warningf(f string, v ...interface{}) {
	level.Warn(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}
func (l *BadgerLogger) Infof(f string, v ...interface{}) {
	level.Info(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}
func (l *BadgerLogger) Debugf(f string, v ...interface{}) {
	level.Debug(l.Logger).Log("msg", fmt.Sprintf(f, v...))
}

func (c *BadgerIndexCache) get(typ string, key storecache.CacheKey) ([]byte, bool) {
	var (
		v     []byte
		found bool
	)
	k := yoloBuf(key.String())

	err := c.cache.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			v = val
			found = true
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})

	if err != nil && err != badger.ErrKeyNotFound {
		level.Error(c.logger).Log("msg", "failed to get value from badger cache", "cacheType", typ, "err", err)
	}

	return v, found
}

func (c *BadgerIndexCache) set(typ string, key storecache.CacheKey, val []byte) {
	k := yoloBuf(key.String())

	if _, ok := c.get(typ, key); ok {
		// item exists, no need to set it again.
		return
	}

	size := uint64(len(k) + len(val))
	if size > c.maxItemSizeBytes {
		level.Info(c.logger).Log(
			"msg", "item bigger than maxItemSizeBytes. Ignoring..",
			"maxItemSizeBytes", c.maxItemSizeBytes,
			"cacheType", typ,
		)
		c.overflow.WithLabelValues(typ).Inc()
		return
	}

	err := c.cache.Update(func(txn *badger.Txn) error {
		return txn.SetEntry(badger.NewEntry(k, val).WithTTL(c.indexTTL))
	})
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to cache in badger", "cacheType", typ, "err", err)
	}
	c.added.WithLabelValues(typ).Inc()
}

// StorePostings sets the postings identified by the ulid and label to the value v,
// if the postings already exists in the cache it is not mutated.
func (c *BadgerIndexCache) StorePostings(blockID ulid.ULID, l labels.Label, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypePostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypePostings, storecache.CacheKey{Block: blockID.String(), Key: copyToKey(l)}, v)
}

// FetchMultiPostings fetches multiple postings - each identified by a label -
// and returns a map containing cache hits, along with a list of missing keys.
func (c *BadgerIndexCache) FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label, tenant string) (hits map[labels.Label][]byte, misses []labels.Label) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypePostings, tenant))
	defer timer.ObserveDuration()

	hits = map[labels.Label][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0

	for i, key := range keys {
		if (i+1)%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(requests))
			c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(hit))
			return hits, misses
		}
		requests++
		if b, ok := c.get(storecache.CacheTypePostings, storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeyPostings(key)}); ok {
			hit++
			hits[key] = b
			continue
		}

		misses = append(misses, key)
	}

	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypePostings, tenant).Add(float64(hit))

	return hits, misses
}

// StoreExpandedPostings stores expanded postings for a set of label matchers.
func (c *BadgerIndexCache) StoreExpandedPostings(blockID ulid.ULID, matchers []*labels.Matcher, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeExpandedPostings, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeyExpandedPostings(storecache.LabelMatchersToString(matchers))}, v)
}

// FetchExpandedPostings fetches expanded postings and returns cached data and a boolean value representing whether it is a cache hit or not.
func (c *BadgerIndexCache) FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, matchers []*labels.Matcher, tenant string) ([]byte, bool) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant))
	defer timer.ObserveDuration()

	if ctx.Err() != nil {
		return nil, false
	}
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
	if b, ok := c.get(storecache.CacheTypeExpandedPostings, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeyExpandedPostings(storecache.LabelMatchersToString(matchers))}); ok {
		c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeExpandedPostings, tenant).Inc()
		return b, true
	}
	return nil, false
}

// StoreSeries sets the series identified by the ulid and id to the value v,
// if the series already exists in the cache it is not mutated.
func (c *BadgerIndexCache) StoreSeries(blockID ulid.ULID, id storage.SeriesRef, v []byte, tenant string) {
	c.commonMetrics.DataSizeBytes.WithLabelValues(storecache.CacheTypeSeries, tenant).Observe(float64(len(v)))
	c.set(storecache.CacheTypeSeries, storecache.CacheKey{Block: blockID.String(), Key: storecache.CacheKeySeries(id)}, v)
}

// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
// and returns a map containing cache hits, along with a list of missing IDs.
func (c *BadgerIndexCache) FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef, tenant string) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	timer := prometheus.NewTimer(c.commonMetrics.FetchLatency.WithLabelValues(storecache.CacheTypeSeries, tenant))
	defer timer.ObserveDuration()

	hits = map[storage.SeriesRef][]byte{}

	blockIDKey := blockID.String()
	requests := 0
	hit := 0
	for i, id := range ids {
		if (i+1)%util.CheckContextEveryNIterations == 0 && ctx.Err() != nil {
			c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(requests))
			c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(hit))
			return hits, misses
		}
		requests++
		if b, ok := c.get(storecache.CacheTypeSeries, storecache.CacheKey{Block: blockIDKey, Key: storecache.CacheKeySeries(id)}); ok {
			hit++
			hits[id] = b
			continue
		}

		misses = append(misses, id)
	}
	c.commonMetrics.RequestTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(requests))
	c.commonMetrics.HitsTotal.WithLabelValues(storecache.CacheTypeSeries, tenant).Add(float64(hit))

	return hits, misses
}

func (c *BadgerIndexCache) retentionLoop(interval time.Duration, threshold uint64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		var err error

		lsm, vlog := c.cache.Size()
		if uint64(lsm+vlog) > threshold {
			// err is raised when nothing to clean
			for err == nil {
				err = c.cache.RunValueLogGC(0.5)
			}
			if errors.Is(err, badger.ErrNoRewrite) {
				c.lastValueLogCleaned.SetToCurrentTime()
			} else {
				level.Error(c.logger).Log("msg", "failed to run value log gc", "err", err)
			}
		}
		c.lastRetentionRun.SetToCurrentTime()
	}
}

func (c *BadgerIndexCache) diskStatUpdateLoop(valuesDir, keysDir string) {
	ticker := time.NewTicker(diskStatUpdateInterval)
	defer ticker.Stop()

	var valuesDataDirStatFS unix.Statfs_t
	var keysDataDirStatFS unix.Statfs_t

	for range ticker.C {
		_ = unix.Statfs(valuesDir, &valuesDataDirStatFS)
		_ = unix.Statfs(keysDir, &keysDataDirStatFS)
		lsm, vlog := c.cache.Size()

		// Using Bavail instead of Bfree to get free blocks available for unprivileged users
		c.valueLogSpaceAvailable.Set(float64(valuesDataDirStatFS.Bavail) * float64(valuesDataDirStatFS.Bsize))
		c.keyLogSpaceAvailable.Set(float64(keysDataDirStatFS.Bavail) * float64(keysDataDirStatFS.Bsize))
		c.valueLogSpaceUsed.Set(float64(vlog))
		c.keyLogSpaceUsed.Set(float64(lsm))
	}
}

// Only used in test
func (c *BadgerIndexCache) cleanDir(dataDir string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	go c.cache.Close()
	for range ticker.C {
		if ok := c.cache.IsClosed(); ok {
			os.RemoveAll(dataDir)
			return
		}
	}
}
