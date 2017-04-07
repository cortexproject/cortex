package chunk

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/util"
)

var (
	indexEntriesPerChunk = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "chunk_store_index_entries_per_chunk",
		Help:      "Number of entries written to storage per chunk.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 5),
	})
	rowWrites = util.NewHashBucketHistogram(util.HashBucketHistogramOpts{
		HistogramOpts: prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "chunk_store_row_writes_distribution",
			Help:      "Distribution of writes to individual storage rows",
			Buckets:   prometheus.DefBuckets,
		},
		HashBuckets: 1024,
	})
)

func init() {
	prometheus.MustRegister(indexEntriesPerChunk)
	prometheus.MustRegister(rowWrites)
}

// StoreConfig specifies config for a ChunkStore
type StoreConfig struct {
	SchemaConfig
	CacheConfig

	// For injecting different schemas in tests.
	schemaFactory func(cfg SchemaConfig) Schema
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(f)
	cfg.CacheConfig.RegisterFlags(f)
}

// Store implements Store
type Store struct {
	cfg StoreConfig

	storage StorageClient
	cache   *Cache
	schema  Schema
}

// NewStore makes a new ChunkStore
func NewStore(cfg StoreConfig, storage StorageClient) (*Store, error) {
	var schema Schema
	var err error
	if cfg.schemaFactory == nil {
		schema, err = newCompositeSchema(cfg.SchemaConfig)
	} else {
		schema = cfg.schemaFactory(cfg.SchemaConfig)
	}
	if err != nil {
		return nil, err
	}

	return &Store{
		cfg:     cfg,
		storage: storage,
		schema:  schema,
		cache:   NewCache(cfg.CacheConfig),
	}, nil
}

// Stop any background goroutines (ie in the cache.)
func (c *Store) Stop() {
	c.cache.Stop()
}

// Put implements ChunkStore
func (c *Store) Put(ctx context.Context, chunks []Chunk) error {
	userID, err := user.Extract(ctx)
	if err != nil {
		return err
	}

	// Encode the chunk first - checksum is calculated as a side effect.
	bufs := [][]byte{}
	keys := []string{}
	for i := range chunks {
		encoded, err := chunks[i].encode()
		if err != nil {
			return err
		}
		bufs = append(bufs, encoded)
		keys = append(keys, chunks[i].externalKey())
	}

	err = c.putChunks(ctx, keys, bufs)
	if err != nil {
		return err
	}

	return c.updateIndex(ctx, userID, chunks)
}

// putChunks writes a collection of chunks to S3 in parallel.
func (c *Store) putChunks(ctx context.Context, keys []string, bufs [][]byte) error {
	incomingErrors := make(chan error)
	for i := range bufs {
		go func(i int) {
			incomingErrors <- c.putChunk(ctx, keys[i], bufs[i])
		}(i)
	}

	var lastErr error
	for range keys {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// putChunk puts a chunk into S3.
func (c *Store) putChunk(ctx context.Context, key string, buf []byte) error {
	err := c.storage.PutChunk(ctx, key, buf)
	if err != nil {
		return err
	}

	if err := c.cache.StoreChunk(ctx, key, buf); err != nil {
		log.Warnf("Could not store %v in chunk cache: %v", key, err)
	}
	return nil
}

func (c *Store) updateIndex(ctx context.Context, userID string, chunks []Chunk) error {
	writeReqs, err := c.calculateDynamoWrites(userID, chunks)
	if err != nil {
		return err
	}

	return c.storage.BatchWrite(ctx, writeReqs)
}

// calculateDynamoWrites creates a set of batched WriteRequests to dynamo for all
// the chunks it is given.
func (c *Store) calculateDynamoWrites(userID string, chunks []Chunk) (WriteBatch, error) {
	writeReqs := c.storage.NewWriteBatch()
	for _, chunk := range chunks {
		metricName, err := util.ExtractMetricNameFromMetric(chunk.Metric)
		if err != nil {
			return nil, err
		}

		entries, err := c.schema.GetWriteEntries(chunk.From, chunk.Through, userID, metricName, chunk.Metric, chunk.externalKey())
		if err != nil {
			return nil, err
		}
		indexEntriesPerChunk.Observe(float64(len(entries)))

		for _, entry := range entries {
			rowWrites.Observe(entry.HashValue, 1)
			writeReqs.Add(entry.TableName, entry.HashValue, entry.RangeValue, entry.Value)
		}
	}
	return writeReqs, nil
}

// Get implements ChunkStore
func (c *Store) Get(ctx context.Context, from, through model.Time, allMatchers ...*metric.LabelMatcher) ([]Chunk, error) {
	if through < from {
		return nil, fmt.Errorf("invalid query, through < from (%d < %d)", through, from)
	}

	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)

	// Fetch chunk descriptors (just ID really) from storage
	chunks, err := c.lookupMatchers(ctx, from, through, matchers)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	// Filter out chunks that are not in the selected time range.
	filtered := make([]Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.Through < from || through < chunk.From {
			continue
		}
		filtered = append(filtered, chunk)
	}

	// Now fetch the actual chunk data from Memcache / S3
	fromCache, missing, err := c.cache.FetchChunkData(ctx, filtered)
	if err != nil {
		log.Warnf("Error fetching from cache: %v", err)
	}

	fromS3, err := c.fetchChunkData(ctx, missing)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	if err = c.writeBackCache(ctx, fromS3); err != nil {
		log.Warnf("Could not store chunks in chunk cache: %v", err)
	}

	// TODO instead of doing this sort, propagate an index and assign chunks
	// into the result based on that index.
	allChunks := append(fromCache, fromS3...)
	sort.Sort(ByKey(allChunks))

	// Filter out chunks
	filteredChunks := make([]Chunk, 0, len(allChunks))
outer:
	for _, chunk := range allChunks {
		for _, filter := range filters {
			if !filter.Match(chunk.Metric[filter.Name]) {
				continue outer
			}
		}

		filteredChunks = append(filteredChunks, chunk)
	}

	return filteredChunks, nil
}

func (c *Store) lookupMatchers(ctx context.Context, from, through model.Time, matchers []*metric.LabelMatcher) ([]Chunk, error) {
	metricName, matchers, _ := util.ExtractMetricNameFromMatchers(matchers)

	userID, err := user.Extract(ctx)
	if err != nil {
		return nil, err
	}

	if len(matchers) == 0 {
		queries, err := c.schema.GetReadQueriesForMetric(from, through, userID, metricName)
		if err != nil {
			return nil, err
		}
		return c.lookupEntries(ctx, queries, nil)
	}

	incomingChunkSets := make(chan ByKey)
	incomingErrors := make(chan error)
	for _, matcher := range matchers {
		go func(matcher *metric.LabelMatcher) {
			var queries []IndexQuery
			var err error
			if matcher.Type != metric.Equal {
				queries, err = c.schema.GetReadQueriesForMetricLabel(from, through, userID, metricName, matcher.Name)
			} else {
				queries, err = c.schema.GetReadQueriesForMetricLabelValue(from, through, userID, metricName, matcher.Name, matcher.Value)
			}
			if err != nil {
				incomingErrors <- err
				return
			}
			incoming, err := c.lookupEntries(ctx, queries, matcher)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingChunkSets <- incoming
			}
		}(matcher)
	}

	var chunkSets []ByKey
	var lastErr error
	for i := 0; i < len(matchers); i++ {
		select {
		case incoming := <-incomingChunkSets:
			chunkSets = append(chunkSets, incoming)
		case err := <-incomingErrors:
			lastErr = err
		}
	}

	return nWayIntersect(chunkSets), lastErr
}

func (c *Store) lookupEntries(ctx context.Context, queries []IndexQuery, matcher *metric.LabelMatcher) (ByKey, error) {
	incomingChunkSets := make(chan ByKey)
	incomingErrors := make(chan error)
	for _, query := range queries {
		go func(query IndexQuery) {
			incoming, err := c.lookupEntry(ctx, query, matcher)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingChunkSets <- incoming
			}
		}(query)
	}

	var chunks ByKey
	var lastErr error
	for i := 0; i < len(queries); i++ {
		select {
		case incoming := <-incomingChunkSets:
			chunks = merge(chunks, incoming)
		case err := <-incomingErrors:
			lastErr = err
		}
	}

	return chunks, lastErr
}

func (c *Store) lookupEntry(ctx context.Context, query IndexQuery, matcher *metric.LabelMatcher) (ByKey, error) {
	var chunkSet ByKey
	var processingError error
	if err := c.storage.QueryPages(ctx, query, func(resp ReadBatch, lastPage bool) (shouldContinue bool) {
		processingError = processResponse(ctx, resp, &chunkSet, matcher)
		return processingError == nil && !lastPage
	}); err != nil {
		log.Errorf("Error querying storage: %v", err)
		return nil, err
	} else if processingError != nil {
		log.Errorf("Error processing storage response: %v", processingError)
		return nil, processingError
	}
	sort.Sort(ByKey(chunkSet))
	chunkSet = unique(chunkSet)
	return chunkSet, nil
}

func processResponse(ctx context.Context, resp ReadBatch, chunkSet *ByKey, matcher *metric.LabelMatcher) error {
	userID, err := user.Extract(ctx)
	if err != nil {
		return err
	}

	for i := 0; i < resp.Len(); i++ {
		chunkKey, labelValue, metadataInIndex, err := parseRangeValue(resp.RangeValue(i), resp.Value(i))
		if err != nil {
			return err
		}

		chunk, err := parseExternalKey(userID, chunkKey)
		if err != nil {
			return err
		}

		// This can be removed in Dev 2017, 13 months after the last chunks
		// was written with metadata in the index.
		if metadataInIndex && resp.Value(i) != nil {
			if err := json.Unmarshal(resp.Value(i), &chunk); err != nil {
				return err
			}
			chunk.metadataInIndex = true
		}

		if matcher != nil && !matcher.Match(labelValue) {
			log.Debug("Dropping chunk for non-matching metric ", chunk.Metric)
			continue
		}
		*chunkSet = append(*chunkSet, chunk)
	}
	return nil
}

func (c *Store) fetchChunkData(ctx context.Context, chunkSet []Chunk) ([]Chunk, error) {
	incomingChunks := make(chan Chunk)
	incomingErrors := make(chan error)
	for _, chunk := range chunkSet {
		go func(chunk Chunk) {
			buf, err := c.storage.GetChunk(ctx, chunk.externalKey())
			if err != nil {
				incomingErrors <- err
				return
			}
			if err := chunk.decode(buf); err != nil {
				incomingErrors <- err
				return
			}
			incomingChunks <- chunk
		}(chunk)
	}

	chunks := []Chunk{}
	errors := []error{}
	for i := 0; i < len(chunkSet); i++ {
		select {
		case chunk := <-incomingChunks:
			chunks = append(chunks, chunk)
		case err := <-incomingErrors:
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return nil, errors[0]
	}
	return chunks, nil
}

func (c *Store) writeBackCache(_ context.Context, chunks []Chunk) error {
	for i := range chunks {
		encoded, err := chunks[i].encode()
		if err != nil {
			return err
		}
		c.cache.BackgroundWrite(chunks[i].externalKey(), encoded)
	}
	return nil
}
