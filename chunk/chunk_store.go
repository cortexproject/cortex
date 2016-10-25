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

package chunk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
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

	// See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
	dynamoMaxBatchSize = 25
	maxConcurrentPuts  = 100
)

var (
	dynamoRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_request_duration_seconds",
		Help:      "Time spent doing DynamoDB requests.",
		Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
	}, []string{"operation", "status_code"})
	dynamoRequestPages = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "dynamo_request_pages",
		Help:      "Number of pages by DynamoDB request",
		Buckets:   prometheus.ExponentialBuckets(1, 2.0, 5),
	})
	dynamoConsumedCapacity = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_consumed_capacity_total",
		Help:      "The capacity units consumed by operation.",
	}, []string{"operation"})
	droppedMatches = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dropped_matches_total",
		Help:      "The number of chunks fetched but later dropped for not matching.",
	})
	s3RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex",
		Name:      "s3_request_duration_seconds",
		Help:      "Time spent doing S3 requests.",
		Buckets:   []float64{.025, .05, .1, .25, .5, 1, 2},
	}, []string{"operation", "status_code"})
)

func init() {
	prometheus.MustRegister(dynamoRequestDuration)
	prometheus.MustRegister(dynamoConsumedCapacity)
	prometheus.MustRegister(dynamoRequestPages)
	prometheus.MustRegister(droppedMatches)
	prometheus.MustRegister(s3RequestDuration)
}

// Store type stores and indexes chunks
type Store interface {
	Put(ctx context.Context, chunks []Chunk) error
	Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]Chunk, error)
}

// StoreConfig specifies config for a ChunkStore
type StoreConfig struct {
	S3URL       string
	DynamoDBURL string
	ChunkCache  *Cache
}

// NewAWSStore makes a new ChunkStore
func NewAWSStore(cfg StoreConfig) (*AWSStore, error) {
	s3URL, err := url.Parse(cfg.S3URL)
	if err != nil {
		return nil, err
	}

	s3Config, err := awsConfigFromURL(s3URL)
	if err != nil {
		return nil, err
	}

	dynamodbURL, err := url.Parse(cfg.DynamoDBURL)
	if err != nil {
		return nil, err
	}

	dynamoDBConfig, err := awsConfigFromURL(dynamodbURL)
	if err != nil {
		return nil, err
	}

	tableName := strings.TrimPrefix(dynamodbURL.Path, "/")
	bucketName := strings.TrimPrefix(s3URL.Path, "/")

	return &AWSStore{
		dynamodb:   dynamodb.New(session.New(dynamoDBConfig)),
		s3:         s3.New(session.New(s3Config)),
		chunkCache: cfg.ChunkCache,
		tableName:  tableName,
		bucketName: bucketName,
		putLimiter: NewSemaphore(maxConcurrentPuts),
	}, nil
}

func awsConfigFromURL(url *url.URL) (*aws.Config, error) {
	if url.User == nil {
		return nil, fmt.Errorf("must specify username & password in URL")
	}
	password, _ := url.User.Password()
	creds := credentials.NewStaticCredentials(url.User.Username(), password, "")
	config := aws.NewConfig().WithCredentials(creds)
	if strings.Contains(url.Host, ".") {
		config = config.WithEndpoint(fmt.Sprintf("http://%s", url.Host)).WithRegion("dummy")
	} else {
		config = config.WithRegion(url.Host)
	}
	return config, nil
}

// AWSStore implements ChunkStore for AWS
type AWSStore struct {
	dynamodb   dynamodbClient
	s3         s3Client
	chunkCache *Cache
	tableName  string
	bucketName string
	putLimiter Semaphore
}

type dynamodbClient interface {
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	ListTables(*dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error)
	BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	QueryPages(*dynamodb.QueryInput, func(p *dynamodb.QueryOutput, lastPage bool) bool) error
}

type s3Client interface {
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
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

// Put implements ChunkStore
func (c *AWSStore) Put(ctx context.Context, chunks []Chunk) error {
	userID, err := user.GetID(ctx)
	if err != nil {
		return err
	}

	err = c.putChunks(userID, chunks)
	if err != nil {
		return err
	}

	return c.updateIndex(userID, chunks)
}

// putChunks writes a collection of chunks to S3 in parallel.
func (c *AWSStore) putChunks(userID string, chunks []Chunk) error {
	incomingErrors := make(chan error)
	for _, chunk := range chunks {
		c.putLimiter.Acquire()
		go func(chunk Chunk) {
			incomingErrors <- c.putChunk(userID, &chunk)
			c.putLimiter.Release()
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
func (c *AWSStore) putChunk(userID string, chunk *Chunk) error {
	err := instrument.TimeRequestHistogram("Put", s3RequestDuration, func() error {
		var err error
		_, err = c.s3.PutObject(&s3.PutObjectInput{
			Body:   bytes.NewReader(chunk.Data),
			Bucket: aws.String(c.bucketName),
			Key:    aws.String(chunkName(userID, chunk.ID)),
		})
		return err
	})
	if err != nil {
		return err
	}

	if c.chunkCache != nil {
		if err = c.chunkCache.StoreChunkData(userID, chunk); err != nil {
			log.Warnf("Could not store %v in chunk cache: %v", chunk.ID, err)
		}
	}
	return nil
}

func (c *AWSStore) updateIndex(userID string, chunks []Chunk) error {
	writeReqs, err := c.calculateDynamoWrites(userID, chunks)
	if err != nil {
		return err
	}

	batches := c.batchRequests(writeReqs)

	// Request all the batches in parallel.
	incomingErrors := make(chan error)
	for _, batch := range batches {
		c.putLimiter.Acquire()
		go func(batch []*dynamodb.WriteRequest) {
			incomingErrors <- c.batchWriteDynamo(batch)
			c.putLimiter.Release()
		}(batch)
	}
	var lastErr error
	for range batches {
		err = <-incomingErrors
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
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

		// TODO compression
		chunkValue, err := json.Marshal(chunk)
		if err != nil {
			return nil, err
		}

		for _, hour := range bigBuckets(chunk.From, chunk.Through) {
			hashValue := hashValue(userID, hour, metricName)
			for label, value := range chunk.Metric {
				if label == model.MetricNameLabel {
					continue
				}

				rangeValue := rangeValue(label, value, chunk.ID)
				writeReqs = append(writeReqs, &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							hashKey:  {S: aws.String(hashValue)},
							rangeKey: {B: rangeValue},
							chunkKey: {B: chunkValue},
						},
					},
				})
			}
		}
	}
	return writeReqs, nil
}

// batchRequests takes a bunch of WriteRequests and groups them into batches
// for later writing.
func (c *AWSStore) batchRequests(writeReqs []*dynamodb.WriteRequest) [][]*dynamodb.WriteRequest {
	batches := [][]*dynamodb.WriteRequest{}
	for i := 0; i < len(writeReqs); i += dynamoMaxBatchSize {
		var reqs []*dynamodb.WriteRequest
		if i+dynamoMaxBatchSize > len(writeReqs) {
			reqs = writeReqs[i:]
		} else {
			reqs = writeReqs[i : i+dynamoMaxBatchSize]
		}
		batches = append(batches, reqs)
	}
	return batches
}

// batchWriteDynamo writes many requests to dynamo in a single batch.
func (c *AWSStore) batchWriteDynamo(reqs []*dynamodb.WriteRequest) error {
	var resp *dynamodb.BatchWriteItemOutput
	err := instrument.TimeRequestHistogram("BatchWriteItem", dynamoRequestDuration, func() error {
		var err error
		resp, err = c.dynamodb.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems:           map[string][]*dynamodb.WriteRequest{c.tableName: reqs},
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		})
		return err
	})
	for _, cc := range resp.ConsumedCapacity {
		dynamoConsumedCapacity.WithLabelValues("BatchWriteItem").
			Add(float64(*cc.CapacityUnits))
	}
	return err
}

// Get implements ChunkStore
func (c *AWSStore) Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]Chunk, error) {
	userID, err := user.GetID(ctx)
	if err != nil {
		return nil, err
	}

	// TODO push ctx all the way through, so we can do cancellation (eventually!)
	missing, err := c.lookupChunks(userID, from, through, matchers)
	if err != nil {
		return nil, err
	}

	var fromCache []Chunk
	if c.chunkCache != nil {
		fromCache, missing, err = c.chunkCache.FetchChunkData(userID, missing)
		if err != nil {
			log.Warnf("Error fetching from cache: %v", err)
		}
	}

	fromS3, err := c.fetchChunkData(userID, missing)
	if err != nil {
		return nil, err
	}

	if c.chunkCache != nil {
		if err = c.chunkCache.StoreChunks(userID, fromS3); err != nil {
			log.Warnf("Could not store chunks in chunk cache: %v", err)
		}
	}

	// TODO instead of doing this sort, propagate an index and assign chunks
	// into the result based on that index.
	chunks := append(fromCache, fromS3...)
	sort.Sort(ByID(chunks))
	return chunks, nil
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

func (c *AWSStore) lookupChunks(userID string, from, through model.Time, matchers []*metric.LabelMatcher) ([]Chunk, error) {
	metricName, matchers, err := extractMetricName(matchers)
	if err != nil {
		return nil, err
	}

	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)
	buckets := bigBuckets(from, through)
	for _, hour := range buckets {
		go func(hour int64) {
			incoming, err := c.lookupChunksFor(userID, hour, metricName, matchers)
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
	return chunks, lastErr
}

func next(s string) string {
	// TODO deal with overflows
	l := len(s)
	result := s[:l-1] + string(s[l-1]+1)
	return result
}

func (c *AWSStore) lookupChunksFor(userID string, hour int64, metricName model.LabelValue, matchers []*metric.LabelMatcher) (ByID, error) {
	if len(matchers) == 0 {
		return c.lookupChunksForMetricName(userID, hour, metricName)
	}

	incomingChunkSets := make(chan ByID)
	incomingErrors := make(chan error)
	for _, matcher := range matchers {
		go func(matcher *metric.LabelMatcher) {
			incoming, err := c.lookupChunksForMatcher(userID, hour, metricName, matcher)
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

func (c *AWSStore) lookupChunksForMetricName(userID string, hour int64, metricName model.LabelValue) (ByID, error) {
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
	var pages int
	defer func() { dynamoRequestPages.Observe(float64(pages)) }()
	if err := instrument.TimeRequestHistogram("QueryPages", dynamoRequestDuration, func() error {
		pages++
		return c.dynamodb.QueryPages(input, func(resp *dynamodb.QueryOutput, lastPage bool) (shouldContinue bool) {
			processingError = processResponse(resp, &chunkSet, nil)
			return processingError != nil && !lastPage
		})
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

func (c *AWSStore) lookupChunksForMatcher(userID string, hour int64, metricName model.LabelValue, matcher *metric.LabelMatcher) (ByID, error) {
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
	var pages int
	defer func() { dynamoRequestPages.Observe(float64(pages)) }()
	if err := instrument.TimeRequestHistogram("QueryPages", dynamoRequestDuration, func() error {
		return c.dynamodb.QueryPages(input, func(resp *dynamodb.QueryOutput, lastPage bool) (shouldContinue bool) {
			pages++
			processingError = processResponse(resp, &chunkSet, matcher)
			return processingError != nil && !lastPage
		})
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

func processResponse(resp *dynamodb.QueryOutput, chunkSet *ByID, matcher *metric.LabelMatcher) error {
	if resp.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues("Query").
			Add(float64(*resp.ConsumedCapacity.CapacityUnits))
	}

	for _, item := range resp.Items {
		rangeValue := item[rangeKey].B
		if rangeValue == nil {
			return fmt.Errorf("invalid item: %v", item)
		}
		label, value, chunkID, err := parseRangeValue(rangeValue)
		if err != nil {
			return err
		}
		chunkValue := item[chunkKey].B
		if chunkValue == nil {
			return fmt.Errorf("invalid item: %v", item)
		}
		chunk := Chunk{
			ID: chunkID,
		}
		if err := json.Unmarshal(chunkValue, &chunk); err != nil {
			return err
		}
		if matcher != nil && (label != matcher.Name || !matcher.Match(value)) {
			log.Debugf("Dropping unexpected", chunk.Metric)
			droppedMatches.Add(1)
			continue
		}
		*chunkSet = append(*chunkSet, chunk)
	}
	return nil
}

func (c *AWSStore) fetchChunkData(userID string, chunkSet []Chunk) ([]Chunk, error) {
	incomingChunks := make(chan Chunk)
	incomingErrors := make(chan error)
	for _, chunk := range chunkSet {
		go func(chunk Chunk) {
			var resp *s3.GetObjectOutput
			err := instrument.TimeRequestHistogram("Get", s3RequestDuration, func() error {
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
			buf, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				incomingErrors <- err
				return
			}
			chunk.Data = buf
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

// unique will remove duplicates from the input
func unique(cs ByID) ByID {
	if len(cs) == 0 {
		return nil
	}

	result := make(ByID, 1, len(cs))
	result[0] = cs[0]
	i, j := 0, 1
	for j < len(cs) {
		if result[i].ID == cs[j].ID {
			j++
			continue
		}
		result = append(result, cs[j])
		i++
		j++
	}
	return result
}

// merge will merge & dedupe two lists of chunks.
// list musts be sorted and not contain dupes.
func merge(a, b ByID) ByID {
	result := make(ByID, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].ID < b[j].ID {
			result = append(result, a[i])
			i++
		} else if a[i].ID > b[j].ID {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		result = append(result, a[i])
	}
	for ; j < len(b); j++ {
		result = append(result, b[j])
	}
	return result
}

// nWayIntersect will interesct n sorted lists of chunks.
func nWayIntersect(sets []ByID) ByID {
	l := len(sets)
	switch l {
	case 0:
		return ByID{}
	case 1:
		return sets[0]
	case 2:
		var (
			left, right = sets[0], sets[1]
			i, j        = 0, 0
			result      = []Chunk{}
		)
		for i < len(left) && j < len(right) {
			if left[i].ID == right[j].ID {
				result = append(result, left[i])
			}

			if left[i].ID < right[j].ID {
				i++
			} else {
				j++
			}
		}
		return result
	default:
		var (
			split = l / 2
			left  = nWayIntersect(sets[:split])
			right = nWayIntersect(sets[split:])
		)
		return nWayIntersect([]ByID{left, right})
	}
}
