package chunk

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
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

// Cache type caches chunks
type Cache struct {
	Memcache   Memcache
	Expiration time.Duration
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
	memcacheRequests.Add(float64(len(chunks)))

	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		keys = append(keys, memcacheKey(userID, chunk.ID))
	}

	var items map[string]*memcache.Item
	err = instrument.TimeRequestHistogramStatus(ctx, "Memcache.Get", memcacheRequestDuration, memcacheStatusCode, func(_ context.Context) error {
		var err error
		items, err = c.Memcache.GetMulti(keys)
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
			return nil, nil, err
		}
		found = append(found, chunk)
	}

	memcacheHits.Add(float64(len(found)))
	return found, missing, nil
}

// StoreChunkData serializes and stores a chunk in the chunk cache.
func (c *Cache) StoreChunkData(ctx context.Context, userID string, chunk *Chunk) error {
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
			Expiration: int32(c.Expiration.Seconds()),
		}
		return c.Memcache.Set(&item)
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
