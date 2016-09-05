// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frankenstein

import (
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage/local/wire"
)

var (
	memcacheRequests = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prometheus",
		Name:      "memcache_requests_total",
		Help:      "Total count of chunks requested from memcache.",
	})

	memcacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "prometheus",
		Name:      "memcache_hits_total",
		Help:      "Total count of chunks found in memcache.",
	})

	memcacheRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "prometheus",
		Name:      "memcache_request_duration_seconds",
		Help:      "Total time spent in seconds doing memcache requests.",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05},
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

// ChunkCache caches chunks
type ChunkCache struct {
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
func (c *ChunkCache) FetchChunkData(userID string, chunks []wire.Chunk) (found []wire.Chunk, missing []wire.Chunk, err error) {
	memcacheRequests.Add(float64(len(chunks)))

	keys := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		keys = append(keys, memcacheKey(userID, chunk.ID))
	}

	var items map[string]*memcache.Item
	err = timeRequestMethodStatus("Get", memcacheRequestDuration, memcacheStatusCode, func() error {
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
func (c *ChunkCache) StoreChunkData(userID string, chunk *wire.Chunk) error {
	return timeRequestMethodStatus("Put", memcacheRequestDuration, memcacheStatusCode, func() error {
		// TODO: Add compression - maybe encapsulated in marshaling/unmarshaling
		// methods of wire.Chunk.
		item := memcache.Item{
			Key:        memcacheKey(userID, chunk.ID),
			Value:      chunk.Data,
			Expiration: int32(c.Expiration.Seconds()),
		}
		return c.Memcache.Set(&item)
	})
}

// StoreChunks serializes and stores multiple chunks in the chunk cache.
func (c *ChunkCache) StoreChunks(userID string, chunks []wire.Chunk) error {
	errs := make(chan error)
	for _, chunk := range chunks {
		go func(chunk *wire.Chunk) {
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
