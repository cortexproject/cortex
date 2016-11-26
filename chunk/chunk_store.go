package chunk

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/sburnett/lexicographic-tuples"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/user"
)

const (
	hashKey  = "h"
	rangeKey = "r"
	chunkKey = "c"

	// For dynamodb errors
	errorReasonLabel = "error"
	otherError       = "other"

	// Backoff for dynamoDB requests
	minBackoff = 100 * time.Millisecond
	maxBackoff = 1 * time.Second

	// Number of synchronous dynamodb requests
	numDynamoRequests = 25

	// See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
	dynamoMaxBatchSize = 25

	provisionedThroughputExceededException = "ProvisionedThroughputExceededException"
)

var (
	dynamoRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_request_duration_seconds",
		Help:      "Time spent doing DynamoDB requests.",

		// DynamoDB latency seems to range from a few ms to a few sec and is
		// important.  So use 8 buckets from 64us to 8s.
		Buckets: prometheus.ExponentialBuckets(0.000128, 4, 8),
	}, []string{"operation", "status_code"})
	dynamoConsumedCapacity = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_consumed_capacity_total",
		Help:      "The capacity units consumed by operation.",
	}, []string{"operation"})
	dynamoFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_failures_total",
		Help:      "The total number of errors while storing chunks to the chunk store.",
	}, []string{errorReasonLabel})
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

	queryChunks = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_chunks",
		Help:      "Number of chunks loaded per query.",
		Buckets:   prometheus.ExponentialBuckets(1, 4.0, 5),
	})
	queryDynamoLookups = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_dynamo_lookups",
		Help:      "Number of dynamo lookups per query.",
		Buckets:   prometheus.ExponentialBuckets(1, 4.0, 5),
	})
	queryRequestPages = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_dynamo_request_pages",
		Help:      "Number of pages per DynamoDB request",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 5),
	})
	queryDroppedMatches = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "query_dynamo_dropped_matches_total",
		Help:      "The number of chunks IDs fetched from Dynamo but later dropped for not matching (per DynamoDB request).",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 5),
	})
)

func init() {
	prometheus.MustRegister(dynamoRequestDuration)
	prometheus.MustRegister(dynamoConsumedCapacity)
	prometheus.MustRegister(dynamoFailures)
	prometheus.MustRegister(indexEntriesPerChunk)
	prometheus.MustRegister(s3RequestDuration)

	prometheus.MustRegister(queryChunks)
	prometheus.MustRegister(queryDynamoLookups)
	prometheus.MustRegister(queryRequestPages)
	prometheus.MustRegister(queryDroppedMatches)
}

// Store type stores and indexes chunks
type Store interface {
	Put(ctx context.Context, chunks []Chunk) error
	Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]Chunk, error)
	Stop()
}

// StoreConfig specifies config for a ChunkStore
type StoreConfig struct {
	S3URL       string
	DynamoDBURL string
	ChunkCache  *Cache

	// Not exported as only used by tests to inject mocks
	dynamodb dynamodbClient
	s3       s3Client
}

type dynamodbClient interface {
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	ListTables(*dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	QueryRequest(*dynamodb.QueryInput) (req dynamoRequest, output *dynamodb.QueryOutput)
}

type dynamoRequest interface {
	NextPage() dynamoRequest
	HasNextPage() bool
	Data() interface{}
	OperationName() string
	Send() error
	Error() error
}

type dynamoClientAdapter struct {
	*dynamodb.DynamoDB
}

func (d dynamoClientAdapter) QueryRequest(in *dynamodb.QueryInput) (dynamoRequest, *dynamodb.QueryOutput) {
	req, out := d.DynamoDB.QueryRequest(in)
	return dynamoRequestAdapter{req}, out
}

type dynamoRequestAdapter struct {
	*request.Request
}

func (d dynamoRequestAdapter) Data() interface{} {
	return d.Request.Data
}

func (d dynamoRequestAdapter) OperationName() string {
	return d.Operation.Name
}

func (d dynamoRequestAdapter) NextPage() dynamoRequest {
	return dynamoRequestAdapter{d.Request.NextPage()}
}

func (d dynamoRequestAdapter) Error() error {
	return d.Request.Error
}

type s3Client interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

// AWSStore implements ChunkStore for AWS
type AWSStore struct {
	dynamodb   dynamodbClient
	s3         s3Client
	chunkCache *Cache
	tableName  string
	bucketName string

	dynamoRequests     chan dynamoOp
	dynamoRequestsDone sync.WaitGroup
}

// NewAWSStore makes a new ChunkStore
func NewAWSStore(cfg StoreConfig) (*AWSStore, error) {
	s3Client, bucketName := cfg.s3, ""
	if s3Client == nil {
		s3URL, err := url.Parse(cfg.S3URL)
		if err != nil {
			return nil, err
		}

		s3Config, err := awsConfigFromURL(s3URL)
		if err != nil {
			return nil, err
		}

		s3Client = s3.New(session.New(s3Config))
		bucketName = strings.TrimPrefix(s3URL.Path, "/")
	}

	dynamodbClient, tableName := cfg.dynamodb, ""
	if dynamodbClient == nil {
		dynamodbURL, err := url.Parse(cfg.DynamoDBURL)
		if err != nil {
			return nil, err
		}

		dynamoDBConfig, err := awsConfigFromURL(dynamodbURL)
		if err != nil {
			return nil, err
		}

		dynamodbClient = dynamoClientAdapter{dynamodb.New(session.New(dynamoDBConfig))}
		tableName = strings.TrimPrefix(dynamodbURL.Path, "/")
	}

	store := &AWSStore{
		dynamodb:   dynamodbClient,
		s3:         s3Client,
		chunkCache: cfg.ChunkCache,
		tableName:  tableName,
		bucketName: bucketName,

		dynamoRequests: make(chan dynamoOp),
	}

	store.dynamoRequestsDone.Add(numDynamoRequests)
	for i := 0; i < numDynamoRequests; i++ {
		go store.dynamoRequestLoop()
	}

	return store, nil
}

func awsConfigFromURL(url *url.URL) (*aws.Config, error) {
	if url.User == nil {
		return nil, fmt.Errorf("must specify username & password in URL")
	}
	password, _ := url.User.Password()
	creds := credentials.NewStaticCredentials(url.User.Username(), password, "")
	config := aws.NewConfig().
		WithCredentials(creds).
		WithMaxRetries(0) // We do our own retries, so we can monitor them
	if strings.Contains(url.Host, ".") {
		config = config.WithEndpoint(fmt.Sprintf("http://%s", url.Host)).WithRegion("dummy")
	} else {
		config = config.WithRegion(url.Host)
	}
	return config, nil
}

// Stop background goroutines.
func (c *AWSStore) Stop() {
	close(c.dynamoRequests)
	c.dynamoRequestsDone.Wait()
}

// CreateTables creates the required tables in DynamoDB.
func (c *AWSStore) CreateTables() error {
	// See if tableName exists.
	resp, err := c.dynamodb.ListTables(&dynamodb.ListTablesInput{
		Limit: aws.Int64(10),
	})
	if err != nil {
		return err
	}
	for _, s := range resp.TableNames {
		if *s == c.tableName {
			return nil
		}
	}

	params := &dynamodb.CreateTableInput{
		TableName: aws.String(c.tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(rangeKey),
				AttributeType: aws.String("B"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(rangeKey),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
	}
	log.Infof("Creating table %s", c.tableName)
	_, err = c.dynamodb.CreateTable(params)
	return err
}

func bigBuckets(from, through model.Time) []int64 {
	var (
		secondsInHour = int64(time.Hour / time.Second)
		fromHour      = from.Unix() / secondsInHour
		throughHour   = through.Unix() / secondsInHour
		result        []int64
	)
	for i := fromHour; i <= throughHour; i++ {
		result = append(result, i)
	}
	return result
}

func chunkName(userID, chunkID string) string {
	return fmt.Sprintf("%s/%s", userID, chunkID)
}

func hashValue(userID string, hour int64, metricName model.LabelValue) string {
	return fmt.Sprintf("%s:%d:%s", userID, hour, metricName)
}

func rangeValue(label model.LabelName, value model.LabelValue, chunkID string) []byte {
	return lex.EncodeOrDie(string(label), string(value), chunkID)
}

func parseRangeValue(v []byte) (label model.LabelName, value model.LabelValue, chunkID string, err error) {
	var labelStr, valueStr string
	_, err = lex.Decode(v, &labelStr, &valueStr, &chunkID)
	label, value = model.LabelName(labelStr), model.LabelValue(valueStr)
	return
}

func recordDynamoError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		dynamoFailures.WithLabelValues(awsErr.Code()).Add(float64(1))
	} else {
		dynamoFailures.WithLabelValues(otherError).Add(float64(1))
	}
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
		_, err = c.s3.PutObject(&s3.PutObjectInput{
			Body:   body,
			Bucket: aws.String(c.bucketName),
			Key:    aws.String(chunkName(userID, chunk.ID)),
		})
		return err
	})
	if err != nil {
		return err
	}

	if c.chunkCache != nil {
		if err = c.chunkCache.StoreChunkData(ctx, userID, chunk); err != nil {
			log.Warnf("Could not store %v in chunk cache: %v", chunk.ID, err)
		}
	}
	return nil
}

func (c *AWSStore) updateIndex(ctx context.Context, userID string, chunks []Chunk) error {
	writeReqs, err := c.calculateDynamoWrites(userID, chunks)
	if err != nil {
		return err
	}

	return c.batchWriteDynamo(ctx, c.tableName, writeReqs)
}

// calculateDynamoWrites creates a set of batched WriteRequests to dynamo for all
// the chunks it is given.
//
// Creates one WriteRequest per bucket per metric per chunk.
func (c *AWSStore) calculateDynamoWrites(userID string, chunks []Chunk) ([]*dynamodb.WriteRequest, error) {
	writeReqs := []*dynamodb.WriteRequest{}
	for _, chunk := range chunks {
		metricName, ok := chunk.Metric[model.MetricNameLabel]
		if !ok {
			return nil, fmt.Errorf("no MetricNameLabel for chunk")
		}

		entries := 0
		for _, hour := range bigBuckets(chunk.From, chunk.Through) {
			hashValue := hashValue(userID, hour, metricName)
			for label, value := range chunk.Metric {
				if label == model.MetricNameLabel {
					continue
				}

				entries++
				rangeValue := rangeValue(label, value, chunk.ID)
				writeReqs = append(writeReqs, &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							hashKey:  {S: aws.String(hashValue)},
							rangeKey: {B: rangeValue},
						},
					},
				})
			}
		}
		indexEntriesPerChunk.Observe(float64(entries))
	}
	return writeReqs, nil
}

// Get implements ChunkStore
func (c *AWSStore) Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]Chunk, error) {
	userID, err := user.GetID(ctx)
	if err != nil {
		return nil, err
	}

	chunks, err := c.lookupChunks(ctx, userID, from, through, matchers)
	if err != nil {
		return nil, err
	}

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

	queryChunks.Observe(float64(len(filtered)))

	var fromCache []Chunk
	var missing = filtered
	if c.chunkCache != nil {
		fromCache, missing, err = c.chunkCache.FetchChunkData(ctx, userID, missing)
		if err != nil {
			log.Warnf("Error fetching from cache: %v", err)
		}
	}

	fromS3, err := c.fetchChunkData(ctx, userID, missing)
	if err != nil {
		return nil, err
	}

	if c.chunkCache != nil {
		if err = c.chunkCache.StoreChunks(ctx, userID, fromS3); err != nil {
			log.Warnf("Could not store chunks in chunk cache: %v", err)
		}
	}

	// TODO instead of doing this sort, propagate an index and assign chunks
	// into the result based on that index.
	allChunks := append(fromCache, fromS3...)
	sort.Sort(ByID(allChunks))
	return allChunks, nil
}

func extractMetricName(matchers []*metric.LabelMatcher) (model.LabelValue, []*metric.LabelMatcher, error) {
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

func (c *AWSStore) lookupChunks(ctx context.Context, userID string, from, through model.Time, matchers []*metric.LabelMatcher) ([]Chunk, error) {
	metricName, matchers, err := extractMetricName(matchers)
	if err != nil {
		return nil, err
	}

	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)
	buckets := bigBuckets(from, through)
	totalLookups := int32(0)
	for _, hour := range buckets {
		go func(hour int64) {
			incoming, lookups, err := c.lookupChunksFor(ctx, userID, hour, metricName, matchers)
			atomic.AddInt32(&totalLookups, lookups)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingChunkSets <- incoming
			}
		}(hour)
	}

	var chunks ByID
	var lastErr error
	for i := 0; i < len(buckets); i++ {
		select {
		case incoming := <-incomingChunkSets:
			chunks = merge(chunks, incoming)
		case err := <-incomingErrors:
			lastErr = err
		}
	}

	queryDynamoLookups.Observe(float64(atomic.LoadInt32(&totalLookups)))
	return chunks, lastErr
}

func next(s string) string {
	// TODO deal with overflows
	l := len(s)
	result := s[:l-1] + string(s[l-1]+1)
	return result
}

func (c *AWSStore) lookupChunksFor(ctx context.Context, userID string, hour int64, metricName model.LabelValue, matchers []*metric.LabelMatcher) (ByID, int32, error) {
	if len(matchers) == 0 {
		return c.lookupChunksForMetricName(ctx, userID, hour, metricName)
	}

	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)

	for _, matcher := range matchers {
		go func(matcher *metric.LabelMatcher) {
			incoming, err := c.lookupChunksForMatcher(ctx, userID, hour, metricName, matcher)
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
	return nWayIntersect(chunkSets), int32(len(matchers)), lastErr
}

func (c *AWSStore) lookupChunksForMetricName(ctx context.Context, userID string, hour int64, metricName model.LabelValue) (ByID, int32, error) {
	hashValue := hashValue(userID, hour, metricName)
	input := &dynamodb.QueryInput{
		TableName: aws.String(c.tableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(hashValue)},
				},
				ComparisonOperator: aws.String("EQ"),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	chunkSet := ByID{}
	var processingError error
	var pages, totalDropped int
	defer func() {
		queryRequestPages.Observe(float64(pages))
		queryDroppedMatches.Observe(float64(totalDropped))
	}()

	if err := c.queryPages(ctx, input, func(resp interface{}, lastPage bool) (shouldContinue bool) {
		var dropped int
		dropped, processingError = processResponse(resp.(*dynamodb.QueryOutput), &chunkSet, nil)
		totalDropped += dropped
		pages++
		return processingError != nil && !lastPage
	}); err != nil {
		log.Errorf("Error querying DynamoDB: %v", err)
		return nil, 1, err
	} else if processingError != nil {
		log.Errorf("Error processing DynamoDB response: %v", processingError)
		return nil, 1, processingError
	}
	sort.Sort(ByID(chunkSet))
	chunkSet = unique(chunkSet)
	return chunkSet, 1, nil
}

func (c *AWSStore) lookupChunksForMatcher(ctx context.Context, userID string, hour int64, metricName model.LabelValue, matcher *metric.LabelMatcher) (ByID, error) {
	hashValue := hashValue(userID, hour, metricName)
	var rangeMinValue, rangeMaxValue []byte
	if matcher.Type == metric.Equal {
		nextValue := model.LabelValue(next(string(matcher.Value)))
		rangeMinValue = rangeValue(matcher.Name, matcher.Value, "")
		rangeMaxValue = rangeValue(matcher.Name, nextValue, "")
	} else {
		nextLabel := model.LabelName(next(string(matcher.Name)))
		rangeMinValue = rangeValue(matcher.Name, "", "")
		rangeMaxValue = rangeValue(nextLabel, "", "")
	}

	input := &dynamodb.QueryInput{
		TableName: aws.String(c.tableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(hashValue)},
				},
				ComparisonOperator: aws.String("EQ"),
			},
			rangeKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{B: rangeMinValue},
					{B: rangeMaxValue},
				},
				ComparisonOperator: aws.String("BETWEEN"),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	chunkSet := ByID{}
	var processingError error
	var pages, totalDropped int
	defer func() {
		queryRequestPages.Observe(float64(pages))
		queryDroppedMatches.Observe(float64(totalDropped))
	}()
	if err := c.queryPages(ctx, input, func(resp interface{}, lastPage bool) (shouldContinue bool) {
		var dropped int
		dropped, processingError = processResponse(resp.(*dynamodb.QueryOutput), &chunkSet, matcher)
		totalDropped += dropped
		pages++
		return processingError != nil && !lastPage
	}); err != nil {
		log.Errorf("Error querying DynamoDB: %v", err)
		return nil, err
	} else if processingError != nil {
		log.Errorf("Error processing DynamoDB response: %v", processingError)
		return nil, processingError
	}

	sort.Sort(ByID(chunkSet))
	return chunkSet, nil
}

func processResponse(resp *dynamodb.QueryOutput, chunkSet *ByID, matcher *metric.LabelMatcher) (int, error) {
	if resp.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues("Query").
			Add(float64(*resp.ConsumedCapacity.CapacityUnits))
	}
	dropped := 0
	for _, item := range resp.Items {
		rangeValue := item[rangeKey].B
		if rangeValue == nil {
			return dropped, fmt.Errorf("invalid item: %v", item)
		}
		label, value, chunkID, err := parseRangeValue(rangeValue)
		if err != nil {
			return dropped, err
		}

		chunk := Chunk{
			ID: chunkID,
		}

		if chunkValue, ok := item[chunkKey]; ok && chunkValue.B != nil {
			if err := json.Unmarshal(chunkValue.B, &chunk); err != nil {
				return dropped, err
			}
			chunk.metadataInIndex = true
		}

		if matcher != nil && (label != matcher.Name || !matcher.Match(value)) {
			log.Debugf("Dropping unexpected", chunk.Metric)
			dropped++
			continue
		}
		*chunkSet = append(*chunkSet, chunk)
	}
	return dropped, nil
}

func (c *AWSStore) fetchChunkData(ctx context.Context, userID string, chunkSet []Chunk) ([]Chunk, error) {
	incomingChunks := make(chan Chunk)
	incomingErrors := make(chan error)
	for _, chunk := range chunkSet {
		go func(chunk Chunk) {
			var resp *s3.GetObjectOutput
			err := instrument.TimeRequestHistogram(ctx, "S3.GetObject", s3RequestDuration, func(_ context.Context) error {
				var err error
				resp, err = c.s3.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(c.bucketName),
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

// batchWriteDynamo writes many requests to dynamo in a single batch.
func (c *AWSStore) batchWriteDynamo(ctx context.Context, tableName string, reqs []*dynamodb.WriteRequest) error {
	req := &dynamoBatchWriteItemsOp{
		ctx:       ctx,
		tableName: tableName,
		reqs:      reqs,
		dynamodb:  c.dynamodb,
		done:      make(chan error),
	}
	c.dynamoRequests <- req
	return <-req.done
}

func (c *AWSStore) queryPages(ctx context.Context, input *dynamodb.QueryInput, callback func(resp interface{}, lastPage bool) (shouldContinue bool)) error {
	page, _ := c.dynamodb.QueryRequest(input)
	req := &dynamoQueryPagesOp{
		ctx:      ctx,
		request:  page,
		callback: callback,
		done:     make(chan error),
	}
	c.dynamoRequests <- req
	return <-req.done
}

func (c *AWSStore) dynamoRequestLoop() {
	defer c.dynamoRequestsDone.Done()
	for {
		select {
		case request, ok := <-c.dynamoRequests:
			if !ok {
				return
			}
			request.do()
		}
	}
}

type dynamoOp interface {
	do()
}

type dynamoQueryPagesOp struct {
	ctx      context.Context
	request  dynamoRequest
	callback func(resp interface{}, lastPage bool) (shouldContinue bool)
	done     chan error
}

type dynamoBatchWriteItemsOp struct {
	ctx       context.Context
	tableName string
	reqs      []*dynamodb.WriteRequest
	dynamodb  dynamodbClient
	done      chan error
}

func nextBackoff(lastBackoff time.Duration) time.Duration {
	// Based on the "Decorrelated Jitter" approach from https://www.awsarchitectureblog.com/2015/03/backoff.html
	// sleep = min(cap, random_between(base, sleep * 3))
	backoff := minBackoff + time.Duration(rand.Int63n(int64((lastBackoff*3)-minBackoff)))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	return backoff
}

func (r *dynamoQueryPagesOp) do() {
	backoff := minBackoff

	for page := r.request; page != nil; page = page.NextPage() {
		err := instrument.TimeRequestHistogram(r.ctx, "DynamoDB.QueryPages", dynamoRequestDuration, func(_ context.Context) error {
			return page.Send()
		})

		if cc := page.Data().(*dynamodb.QueryOutput).ConsumedCapacity; cc != nil {
			dynamoConsumedCapacity.WithLabelValues("DynamoDB.QueryPages").
				Add(float64(*cc.CapacityUnits))
		}

		if err != nil {
			recordDynamoError(err)

			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
				time.Sleep(backoff)
				backoff = nextBackoff(backoff)
				continue
			}

			r.done <- page.Error()
			return
		}

		if getNextPage := r.callback(page.Data(), !page.HasNextPage()); !getNextPage {
			r.done <- page.Error()
			return
		}

		backoff = minBackoff
	}

	r.done <- nil
}

func (r *dynamoBatchWriteItemsOp) do() {
	outstanding, unprocessed := r.reqs, []*dynamodb.WriteRequest{}
	backoff := minBackoff
	min := func(i, j int) int {
		if i < j {
			return i
		}
		return j
	}
	fillReq := func(in *[]*dynamodb.WriteRequest, out *[]*dynamodb.WriteRequest) {
		outLen, inLen := len(*out), len(*in)
		toFill := min(inLen, dynamoMaxBatchSize-inLen-outLen)
		*out = append(*out, (*in)[:toFill]...)
		*in = (*in)[toFill:]
	}

	for len(outstanding)+len(unprocessed) > 0 {
		var reqs []*dynamodb.WriteRequest
		fillReq(&unprocessed, &reqs)
		fillReq(&outstanding, &reqs)

		var resp *dynamodb.BatchWriteItemOutput
		err := instrument.TimeRequestHistogram(r.ctx, "DynamoDB.BatchWriteItem", dynamoRequestDuration, func(_ context.Context) error {
			var err error
			resp, err = r.dynamodb.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems:           map[string][]*dynamodb.WriteRequest{r.tableName: reqs},
				ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
			})
			if err != nil {
				recordDynamoError(err)
			}
			return err
		})
		for _, cc := range resp.ConsumedCapacity {
			dynamoConsumedCapacity.WithLabelValues("DynamoDB.BatchWriteItem").
				Add(float64(*cc.CapacityUnits))
		}

		// If there are unprocessed items, backoff and retry those items.
		if resp.UnprocessedItems != nil && len(resp.UnprocessedItems[r.tableName]) > 0 {
			unprocessed = append(unprocessed, resp.UnprocessedItems[r.tableName]...)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		// If we get provisionedThroughputExceededException, then no items were processed,
		// so back off and retry all.
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
			unprocessed = append(unprocessed, reqs...)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		// All other errors are fatal.
		if err != nil {
			r.done <- err
			return
		}

		backoff = minBackoff
	}

	r.done <- nil
}
