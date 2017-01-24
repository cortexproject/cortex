package chunk

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/weaveworks/scope/common/instrument"
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

func memcacheKey(userID, chunkID string) string {
	return fmt.Sprintf("%s/%s", userID, chunkID)
}

// FetchChunkData gets chunks from the chunk cache.
func (c *Cache) FetchChunkData(ctx context.Context, userID string, chunks []Chunk) (found []Chunk, missing []Chunk, err error) {
	if c.memcache == nil {
		return nil, chunks, nil
	}

	memcacheRequests.Add(float64(len(chunks)))

	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		keys = append(keys, memcacheKey(userID, chunk.ID))
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

	for _, chunk := range chunks {
		item, ok := items[memcacheKey(userID, chunk.ID)]
		if !ok {
			missing = append(missing, chunk)
			continue
		}

		if err := chunk.decode(bytes.NewReader(item.Value)); err != nil {
			log.Errorf("Failed to decode chunk from cache: %v", err)
			missing = append(missing, chunk)
			continue
		}
		found = append(found, chunk)
	}

	memcacheHits.Add(float64(len(found)))
	return found, missing, nil
}

// StoreChunkData serializes and stores a chunk in the chunk cache.
func (c *Cache) StoreChunkData(ctx context.Context, userID string, chunk *Chunk) error {
	if c.memcache == nil {
		return nil
	}

	reader, err := chunk.reader()
	if err != nil {
		return err
	}

	buf, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	return instrument.TimeRequestHistogramStatus(ctx, "Memcache.Put", memcacheRequestDuration, memcacheStatusCode, func(_ context.Context) error {
		item := memcache.Item{
			Key:        memcacheKey(userID, chunk.ID),
			Value:      buf,
			Expiration: int32(c.cfg.Expiration.Seconds()),
		}
		return c.memcache.Set(&item)
	})
}

// StoreChunks serializes and stores multiple chunks in the chunk cache.
func (c *Cache) StoreChunks(ctx context.Context, userID string, chunks []Chunk) error {
	errs := make(chan error)
	for _, chunk := range chunks {
		go func(chunk *Chunk) {
			errs <- c.StoreChunkData(ctx, userID, chunk)
		}(&chunk)
	}
	var errOut error
	for i := 0; i < len(chunks); i++ {
		if err := <-errs; err != nil {
			errOut = err
		}
	}
	return errOut
}
