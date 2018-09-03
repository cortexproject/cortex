package storage

import (
	"bytes"
	"context"
	"encoding/gob"
	"strings"
	"time"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/cache"
)

// IndexCache describes the cache for the Index.
type IndexCache interface {
	Store(ctx context.Context, key string, val readBatch)
	Fetch(ctx context.Context, key string) (val readBatch, ok bool, err error)
	Stop() error
}

type indexCache struct {
	cache.Cache
}

func (c *indexCache) Store(ctx context.Context, key string, val readBatch) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(val); err != nil {
		return
	}

	c.Cache.Store(ctx, key, buf.Bytes())
	return
}

func (c *indexCache) Fetch(ctx context.Context, key string) (readBatch, bool, error) {
	found, valBytes, _, err := c.Cache.Fetch(ctx, []string{key})
	if len(found) != 1 || err != nil {
		return nil, false, err
	}

	var q readBatch
	r := bytes.NewReader(valBytes[0])
	if err := gob.NewDecoder(r).Decode(&q); err != nil {
		return nil, false, err
	}

	return q, true, nil
}

type cachingStorageClient struct {
	chunk.StorageClient
	cache    IndexCache
	validity time.Duration
}

func newCachingStorageClient(client chunk.StorageClient, cache cache.Cache) chunk.StorageClient {
	if cache == nil {
		return client
	}

	return &cachingStorageClient{
		StorageClient: client,
		cache:         &indexCache{cache},
	}
}

func (s *cachingStorageClient) QueryPages(ctx context.Context, query chunk.IndexQuery, callback func(result chunk.ReadBatch) (shouldContinue bool)) error {
	value, ok, err := s.cache.Fetch(ctx, queryKey(query))
	if ok {
		filteredBatch, _ := filterBatchByQuery(query, []chunk.ReadBatch{value})
		callback(filteredBatch)

		return nil
	}

	batches := []chunk.ReadBatch{}
	cacheableQuery := chunk.IndexQuery{
		TableName: query.TableName,
		HashValue: query.HashValue,
	} // Just reads the entire row and caches it.

	err = s.StorageClient.QueryPages(ctx, cacheableQuery, copyingCallback(&batches))
	if err != nil {
		return err
	}

	filteredBatch, totalBatches := filterBatchByQuery(query, batches)
	callback(filteredBatch)

	s.cache.Store(ctx, queryKey(query), totalBatches)
	return nil
}

type readBatch []Cell

func (b readBatch) Len() int                { return len(b) }
func (b readBatch) RangeValue(i int) []byte { return b[i].Column }
func (b readBatch) Value(i int) []byte      { return b[i].Value }

// Cell is dummyyyyy.
type Cell struct {
	Column []byte
	Value  []byte
}

func copyingCallback(readBatches *[]chunk.ReadBatch) func(chunk.ReadBatch) bool {
	return func(result chunk.ReadBatch) bool {
		*readBatches = append(*readBatches, result)
		return true
	}
}

func queryKey(q chunk.IndexQuery) string {
	const sep = "\xff"
	return q.TableName + sep + q.HashValue
}

func filterBatchByQuery(query chunk.IndexQuery, batches []chunk.ReadBatch) (filteredBatch readBatch, totalBatch readBatch) {
	filter := func([]byte, []byte) bool { return true }

	if len(query.RangeValuePrefix) != 0 {
		filter = func(rangeValue []byte, value []byte) bool {
			return strings.HasPrefix(string(rangeValue), string(query.RangeValuePrefix))
		}
	}
	if len(query.RangeValueStart) != 0 {
		filter = func(rangeValue []byte, value []byte) bool {
			return string(rangeValue) >= string(query.RangeValueStart)
		}
	}
	if len(query.ValueEqual) != 0 {
		// This is on top of the existing filters.
		existingFilter := filter
		filter = func(rangeValue []byte, value []byte) bool {
			return existingFilter(rangeValue, value) && bytes.Equal(value, query.ValueEqual)
		}
	}

	filteredBatch = make(readBatch, 0, len(batches)) // On the higher side for most queries. On the lower side for column key schema.
	totalBatch = make(readBatch, 0, len(batches))
	for _, batch := range batches {
		for i := 0; i < batch.Len(); i++ {
			totalBatch = append(totalBatch, Cell{Column: batch.RangeValue(i), Value: batch.Value(i)})

			if filter(batch.RangeValue(i), batch.Value(i)) {
				filteredBatch = append(filteredBatch, Cell{Column: batch.RangeValue(i), Value: batch.Value(i)})
			}
		}
	}

	return
}
