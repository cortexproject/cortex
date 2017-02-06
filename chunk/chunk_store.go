package chunk

import (
	"encoding/json"
	"flag"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/weaveworks/common/instrument"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/user"
	"github.com/weaveworks/cortex/util"
)

const (
	hashKey  = "h"
	rangeKey = "r"
	chunkKey = "c"
)

var (
	indexEntriesPerChunk = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "chunk_store_index_entries_per_chunk",
		Help:      "Number of entries written to dynamodb per chunk.",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 5),
	})
	s3RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"})
	rowWrites = util.NewHashBucketHistogram(util.HashBucketHistogramOpts{
		HistogramOpts: prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "chunk_store_row_writes_distribution",
			Help:      "Distribution of writes to individual DynamoDB rows",
			Buckets:   prometheus.DefBuckets,
		},
		HashBuckets: 1024,
	})
)

func init() {
	prometheus.MustRegister(indexEntriesPerChunk)
	prometheus.MustRegister(s3RequestDuration)
	prometheus.MustRegister(rowWrites)
}

// Store type stores and indexes chunks
type Store interface {
	Put(ctx context.Context, chunks []Chunk) error
	Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]Chunk, error)
}

// StoreConfig specifies config for a ChunkStore
type StoreConfig struct {
	SchemaConfig
	CacheConfig
	S3       S3ClientValue
	DynamoDB DynamoDBClientValue

	// For injecting different schemas in tests.
	schemaFactory func(cfg SchemaConfig) Schema
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *StoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.SchemaConfig.RegisterFlags(f)
	cfg.CacheConfig.RegisterFlags(f)

	f.Var(&cfg.S3, "s3.url", "S3 endpoint URL.")
	f.Var(&cfg.DynamoDB, "dynamodb.url", "DynamoDB endpoint URL.")
}

// AWSStore implements ChunkStore for AWS
type AWSStore struct {
	cfg    StoreConfig
	cache  *Cache
	dynamo *dynamoDBBackoffClient
	schema Schema
}

// NewAWSStore makes a new ChunkStore
func NewAWSStore(cfg StoreConfig) (*AWSStore, error) {
	cfg.SchemaConfig.OriginalTableName = cfg.DynamoDB.TableName
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

	return &AWSStore{
		cfg:    cfg,
		schema: schema,
		cache:  NewCache(cfg.CacheConfig),
		dynamo: newDynamoDBBackoffClient(cfg.DynamoDB),
	}, nil
}

func chunkName(userID, chunkID string) string {
	return fmt.Sprintf("%s/%s", userID, chunkID)
}

// Put implements ChunkStore
func (c *AWSStore) Put(ctx context.Context, chunks []Chunk) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	err = c.putChunks(ctx, userID, chunks)
	if err != nil {
		return err
	}

	return c.updateIndex(ctx, userID, chunks)
}

// putChunks writes a collection of chunks to S3 in parallel.
func (c *AWSStore) putChunks(ctx context.Context, userID string, chunks []Chunk) error {
	incomingErrors := make(chan error)
	for _, chunk := range chunks {
		go func(chunk Chunk) {
			incomingErrors <- c.putChunk(ctx, userID, &chunk)
		}(chunk)
	}

	var lastErr error
	for range chunks {
		err := <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// putChunk puts a chunk into S3.
func (c *AWSStore) putChunk(ctx context.Context, userID string, chunk *Chunk) error {
	body, err := chunk.reader()
	if err != nil {
		return err
	}

	err = instrument.TimeRequestHistogram(ctx, "S3.PutObject", s3RequestDuration, func(_ context.Context) error {
		var err error
		_, err = c.cfg.S3.PutObject(&s3.PutObjectInput{
			Body:   body,
			Bucket: aws.String(c.cfg.S3.BucketName),
			Key:    aws.String(chunkName(userID, chunk.ID)),
		})
		return err
	})
	if err != nil {
		return err
	}

	if err = c.cache.StoreChunkData(ctx, userID, chunk); err != nil {
		log.Warnf("Could not store %v in chunk cache: %v", chunk.ID, err)
	}
	return nil
}

func (c *AWSStore) updateIndex(ctx context.Context, userID string, chunks []Chunk) error {
	writeReqs, err := c.calculateDynamoWrites(userID, chunks)
	if err != nil {
		return err
	}

	return c.dynamo.batchWriteDynamo(ctx, writeReqs)
}

// calculateDynamoWrites creates a set of WriteRequests to dynamo for all `the
// chunks it is given.
func (c *AWSStore) calculateDynamoWrites(userID string, chunks []Chunk) (map[string][]*dynamodb.WriteRequest, error) {
	writeReqs := map[string][]*dynamodb.WriteRequest{}
	for _, chunk := range chunks {
		metricName, err := extractMetricNameFromMetric(chunk.Metric)
		if err != nil {
			return nil, err
		}

		entries, err := c.schema.GetWriteEntries(chunk.From, chunk.Through, userID, metricName, chunk.Metric, chunk.ID)
		if err != nil {
			return nil, err
		}
		indexEntriesPerChunk.Observe(float64(len(entries)))

		for _, entry := range entries {
			rowWrites.Observe(entry.HashKey, 1)

			writeReqs[entry.TableName] = append(writeReqs[entry.TableName], &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: map[string]*dynamodb.AttributeValue{
						hashKey:  {S: aws.String(entry.HashKey)},
						rangeKey: {B: entry.RangeKey},
					},
				},
			})
		}
	}
	return writeReqs, nil
}

// Get implements ChunkStore
func (c *AWSStore) Get(ctx context.Context, from, through model.Time, allMatchers ...*metric.LabelMatcher) ([]Chunk, error) {
	userID, err := user.GetID(ctx)
	if err != nil {
		return nil, err
	}

	filters, matchers := util.SplitFiltersAndMatchers(allMatchers)

	// Fetch chunk descriptors (just ID really) from DynamoDB
	chunks, err := c.lookupChunks(ctx, userID, from, through, matchers)
	if err != nil {
		return nil, err
	}

	// Filter out chunks that are not in the selected time range.
	filtered := make([]Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		_, chunkFrom, chunkThrough, err := parseChunkID(chunk.ID)
		if err != nil {
			return nil, err
		}
		if chunkThrough < from || through < chunkFrom {
			continue
		}
		filtered = append(filtered, chunk)
	}

	// Now fetch the actual chunk data from Memcache / S3
	fromCache, missing, err := c.cache.FetchChunkData(ctx, userID, filtered)
	if err != nil {
		log.Warnf("Error fetching from cache: %v", err)
	}

	fromS3, err := c.fetchChunkData(ctx, userID, missing)
	if err != nil {
		return nil, err
	}

	if err = c.cache.StoreChunks(ctx, userID, fromS3); err != nil {
		log.Warnf("Could not store chunks in chunk cache: %v", err)
	}

	// TODO instead of doing this sort, propagate an index and assign chunks
	// into the result based on that index.
	allChunks := append(fromCache, fromS3...)
	sort.Sort(ByID(allChunks))

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

func (c *AWSStore) lookupChunks(ctx context.Context, userID string, from, through model.Time, matchers []*metric.LabelMatcher) ([]Chunk, error) {
	metricName, matchers, err := extractMetricNameFromMatchers(matchers)
	if err != nil {
		return nil, err
	}

	if len(matchers) == 0 {
		entries, err := c.schema.GetReadEntriesForMetric(from, through, userID, metricName)
		if err != nil {
			return nil, err
		}
		return c.lookupEntries(ctx, entries, nil)
	}

	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)
	for _, matcher := range matchers {
		go func(matcher *metric.LabelMatcher) {
			var entries []IndexEntry
			var err error
			if matcher.Type != metric.Equal {
				entries, err = c.schema.GetReadEntriesForMetricLabel(from, through, userID, metricName, matcher.Name)
			} else {
				entries, err = c.schema.GetReadEntriesForMetricLabelValue(from, through, userID, metricName, matcher.Name, matcher.Value)
			}
			if err != nil {
				incomingErrors <- err
				return
			}
			incoming, err := c.lookupEntries(ctx, entries, matcher)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingChunkSets <- incoming
			}
		}(matcher)
	}

	var chunkSets []ByID
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

func (c *AWSStore) lookupEntries(ctx context.Context, entries []IndexEntry, matcher *metric.LabelMatcher) (ByID, error) {
	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)
	for _, entry := range entries {
		go func(entry IndexEntry) {
			incoming, err := c.lookupEntry(ctx, entry, matcher)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingChunkSets <- incoming
			}
		}(entry)
	}

	var chunks ByID
	var lastErr error
	for i := 0; i < len(entries); i++ {
		select {
		case incoming := <-incomingChunkSets:
			chunks = merge(chunks, incoming)
		case err := <-incomingErrors:
			lastErr = err
		}
	}

	return chunks, lastErr
}

func (c *AWSStore) lookupEntry(ctx context.Context, entry IndexEntry, matcher *metric.LabelMatcher) (ByID, error) {
	input := &dynamodb.QueryInput{
		TableName: aws.String(entry.TableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(entry.HashKey)},
				},
				ComparisonOperator: aws.String("EQ"),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	if len(entry.RangeKey) > 0 {
		input.KeyConditions[rangeKey] = &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{
				{B: entry.RangeKey},
			},
			ComparisonOperator: aws.String(dynamodb.ComparisonOperatorBeginsWith),
		}
	}

	var chunkSet ByID
	var processingError error
	if err := c.dynamo.queryPages(ctx, input, func(resp interface{}, lastPage bool) (shouldContinue bool) {
		processingError = processResponse(resp.(*dynamodb.QueryOutput), &chunkSet, matcher)
		return processingError != nil && !lastPage
	}); err != nil {
		log.Errorf("Error querying DynamoDB: %v", err)
		return nil, err
	} else if processingError != nil {
		log.Errorf("Error processing DynamoDB response: %v", processingError)
		return nil, processingError
	}
	sort.Sort(ByID(chunkSet))
	chunkSet = unique(chunkSet)
	return chunkSet, nil
}

func processResponse(resp *dynamodb.QueryOutput, chunkSet *ByID, matcher *metric.LabelMatcher) error {
	for _, item := range resp.Items {
		rangeValue := item[rangeKey].B
		if rangeValue == nil {
			return fmt.Errorf("invalid item: %v", item)
		}
		_, value, chunkID, err := parseRangeValue(rangeValue)
		if err != nil {
			return err
		}

		chunk := Chunk{
			ID: chunkID,
		}

		if chunkValue, ok := item[chunkKey]; ok && chunkValue.B != nil {
			if err := json.Unmarshal(chunkValue.B, &chunk); err != nil {
				return err
			}
			chunk.metadataInIndex = true
		}

		if matcher != nil && !matcher.Match(value) {
			log.Debug("Dropping chunk for non-matching metric ", chunk.Metric)
			continue
		}
		*chunkSet = append(*chunkSet, chunk)
	}
	return nil
}

func (c *AWSStore) fetchChunkData(ctx context.Context, userID string, chunkSet []Chunk) ([]Chunk, error) {
	incomingChunks := make(chan Chunk)
	incomingErrors := make(chan error)
	for _, chunk := range chunkSet {
		go func(chunk Chunk) {
			var resp *s3.GetObjectOutput
			err := instrument.TimeRequestHistogram(ctx, "S3.GetObject", s3RequestDuration, func(_ context.Context) error {
				var err error
				resp, err = c.cfg.S3.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(c.cfg.S3.BucketName),
					Key:    aws.String(chunkName(userID, chunk.ID)),
				})
				return err
			})
			if err != nil {
				incomingErrors <- err
				return
			}
			defer resp.Body.Close()
			if err := chunk.decode(resp.Body); err != nil {
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

func extractMetricNameFromMetric(m model.Metric) (model.LabelValue, error) {
	for name, value := range m {
		if name == model.MetricNameLabel {
			return value, nil
		}
	}
	return "", fmt.Errorf("no MetricNameLabel for chunk")
}

func extractMetricNameFromMatchers(matchers []*metric.LabelMatcher) (model.LabelValue, []*metric.LabelMatcher, error) {
	for i, matcher := range matchers {
		if matcher.Name != model.MetricNameLabel {
			continue
		}
		if matcher.Type != metric.Equal {
			return "", nil, fmt.Errorf("must have equality matcher for MetricNameLabel")
		}
		metricName := matcher.Value
		matchers = matchers[:i+copy(matchers[i:], matchers[i+1:])]
		return metricName, matchers, nil
	}
	return "", nil, fmt.Errorf("no matcher for MetricNameLabel")
}
