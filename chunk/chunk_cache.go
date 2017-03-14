package chunk

import (
	"flag"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"
)

var (
	memcacheRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "memcache_requests_total",
		Help:      "Total count of chunks requested from memcache.",
	})

	memcacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "memcache_hits_total",
		Help:      "Total count of chunks found in memcache.",
	})

	memcacheCorrupt = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "memcache_corrupt_chunks_total",
		Help:      "Total count of number of corrupt chunks found in memcache.",
	})

	memcacheRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "memcache_request_duration_seconds",
		Help:      "Total time spent in seconds doing memcache requests.",
		// Memecache requests are very quick: smallest bucket is 16us, biggest is 1s
		Buckets: prometheus.ExponentialBuckets(0.000016, 4, 8),
	}, []string{"method", "status_code"})
)

func init() {
	prometheus.MustRegister(memcacheRequests)
	prometheus.MustRegister(memcacheHits)
	prometheus.MustRegister(memcacheCorrupt)
	prometheus.MustRegister(memcacheRequestDuration)
}

// Memcache caches things
type Memcache interface {
	GetMulti(keys []string) (map[string]*memcache.Item, error)
	Set(item *memcache.Item) error
}

// CacheConfig is config to make a Cache
type CacheConfig struct {
	Expiration     time.Duration
	memcacheConfig MemcacheConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *CacheConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.Expiration, "memcached.expiration", 0, "How long chunks stay in the memcache.")
	cfg.memcacheConfig.RegisterFlags(f)
}

// Cache type caches chunks
type Cache struct {
	cfg      CacheConfig
	memcache Memcache
}

// NewCache makes a new Cache
func NewCache(cfg CacheConfig) *Cache {
	var memcache Memcache
	if cfg.memcacheConfig.Host != "" {
		memcache = NewMemcacheClient(cfg.memcacheConfig)
	}
	return &Cache{
		cfg:      cfg,
		memcache: memcache,
	}
}

func memcacheStatusCode(err error) string {
	// See https://godoc.org/github.com/bradfitz/gomemcache/memcache#pkg-variables
	switch err {
	case nil:
		return "200"
	case memcache.ErrCacheMiss:
		return "404"
	case memcache.ErrMalformedKey:
		return "400"
	default:
		return "500"
	}
}

// FetchChunkData gets chunks from the chunk cache.
func (c *Cache) FetchChunkData(ctx context.Context, chunks []Chunk) (found []Chunk, missing []Chunk, err error) {
	if c.memcache == nil {
		return nil, chunks, nil
	}

	memcacheRequests.Add(float64(len(chunks)))

	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		keys = append(keys, chunk.externalKey())
	}

	var items map[string]*memcache.Item
	err = instrument.TimeRequestHistogramStatus(ctx, "Memcache.Get", memcacheRequestDuration, memcacheStatusCode, func(_ context.Context) error {
		var err error
		items, err = c.memcache.GetMulti(keys)
		return err
	})
	if err != nil {
		return nil, chunks, err
	}

	for i, externalKey := range keys {
		item, ok := items[externalKey]
		if !ok {
			missing = append(missing, chunks[i])
			continue
		}

		if err := chunks[i].decode(item.Value); err != nil {
			memcacheCorrupt.Inc()
			log.Errorf("Failed to decode chunk from cache: %v", err)
			missing = append(missing, chunks[i])
			continue
		}

		found = append(found, chunks[i])
	}

	memcacheHits.Add(float64(len(found)))
	return found, missing, nil
}

// StoreChunk serializes and stores a chunk in the chunk cache.
func (c *Cache) StoreChunk(ctx context.Context, key string, buf []byte) error {
	if c.memcache == nil {
		return nil
	}

	return instrument.TimeRequestHistogramStatus(ctx, "Memcache.Put", memcacheRequestDuration, memcacheStatusCode, func(_ context.Context) error {
		item := memcache.Item{
			Key:        key,
			Value:      buf,
			Expiration: int32(c.cfg.Expiration.Seconds()),
		}
		return c.memcache.Set(&item)
	})
}

// StoreChunks serializes and stores multiple chunks in the chunk cache.
func (c *Cache) StoreChunks(ctx context.Context, keys []string, bufs [][]byte) error {
	errs := make(chan error)
	for i := range keys {
		go func(i int) {
			errs <- c.StoreChunk(ctx, keys[i], bufs[i])
		}(i)
	}
	var errOut error
	for range keys {
		if err := <-errs; err != nil {
			log.Errorf("Error putting chunk to memcache: %v", err)
			errOut = err
		}
	}
	return errOut
}
