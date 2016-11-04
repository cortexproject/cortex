package chunk

import (
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/scope/common/instrument"
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
func (c *Cache) FetchChunkData(userID string, chunks []Chunk) (found []Chunk, missing []Chunk, err error) {
	memcacheRequests.Add(float64(len(chunks)))

	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		keys = append(keys, memcacheKey(userID, chunk.ID))
	}

	var items map[string]*memcache.Item
	err = instrument.TimeRequestHistogramStatus("Get", memcacheRequestDuration, memcacheStatusCode, func() error {
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
		} else {
			chunk.Data = item.Value
			found = append(found, chunk)
		}
	}

	memcacheHits.Add(float64(len(found)))
	return found, missing, nil
}

// StoreChunkData serializes and stores a chunk in the chunk cache.
func (c *Cache) StoreChunkData(userID string, chunk *Chunk) error {
	return instrument.TimeRequestHistogramStatus("Put", memcacheRequestDuration, memcacheStatusCode, func() error {
		// TODO: Add compression - maybe encapsulated in marshaling/unmarshaling
		// methods of Chunk.
		item := memcache.Item{
			Key:        memcacheKey(userID, chunk.ID),
			Value:      chunk.Data,
			Expiration: int32(c.Expiration.Seconds()),
		}
		return c.Memcache.Set(&item)
	})
}

// StoreChunks serializes and stores multiple chunks in the chunk cache.
func (c *Cache) StoreChunks(userID string, chunks []Chunk) error {
	errs := make(chan error)
	for _, chunk := range chunks {
		go func(chunk *Chunk) {
			errs <- c.StoreChunkData(userID, chunk)
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
