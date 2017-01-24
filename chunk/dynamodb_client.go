package chunk

import (
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/scope/common/instrument"
	"golang.org/x/net/context"
)

const (
	// For dynamodb errors
	errorReasonLabel = "error"
	otherError       = "other"

	// Backoff for dynamoDB requests, to match AWS lib - see:
	// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/customizations.go
	minBackoff = 50 * time.Millisecond
	maxBackoff = 50 * time.Second

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
	dynamoUnprocessedItems = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "dynamo_unprocessed_items_total",
		Help:      "Unprocessed items",
	})
)

func init() {
	prometheus.MustRegister(dynamoRequestDuration)
	prometheus.MustRegister(dynamoConsumedCapacity)
	prometheus.MustRegister(dynamoFailures)
	prometheus.MustRegister(dynamoUnprocessedItems)
}

func recordDynamoError(err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		dynamoFailures.WithLabelValues(awsErr.Code()).Add(float64(1))
	} else {
		dynamoFailures.WithLabelValues(otherError).Add(float64(1))
	}
}

// DynamoDBClient is a client for DynamoDB
type DynamoDBClient interface {
	ListTablesPages(*dynamodb.ListTablesInput, func(p *dynamodb.ListTablesOutput, lastPage bool) (shouldContinue bool)) error
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(*dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error)

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

// NewDynamoDBClient makes a new DynamoDBClient
func NewDynamoDBClient(dynamoDBURL string) (DynamoDBClient, string, error) {
	url, err := url.Parse(dynamoDBURL)
	if err != nil {
		return nil, "", err
	}

	dynamoDBConfig, err := awsConfigFromURL(url)
	if err != nil {
		return nil, "", err
	}

	dynamoDBClient := dynamoClientAdapter{dynamodb.New(session.New(dynamoDBConfig))}
	tableName := strings.TrimPrefix(url.Path, "/")
	return dynamoDBClient, tableName, nil
}

// DynamoDBClientValue is a flag.Value that parses a URL and produces a DynamoDBClient
type DynamoDBClientValue struct {
	url, TableName string
	DynamoDBClient
}

// String implements flag.Value
func (c *DynamoDBClientValue) String() string {
	return c.url
}

// Set implements flag.Value
func (c *DynamoDBClientValue) Set(v string) error {
	var err error
	c.DynamoDBClient, c.TableName, err = NewDynamoDBClient(v)
	return err
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
	if r := d.Request.NextPage(); r != nil {
		return dynamoRequestAdapter{r}
	}
	return nil
}

func (d dynamoRequestAdapter) Error() error {
	return d.Request.Error
}

type dynamoDBBackoffClient struct {
	client DynamoDBClient
}

func newDynamoDBBackoffClient(client DynamoDBClient) *dynamoDBBackoffClient {
	return &dynamoDBBackoffClient{
		client: client,
	}
}

// batchWriteDynamo writes many requests to dynamo in a single batch.
func (c *dynamoDBBackoffClient) batchWriteDynamo(ctx context.Context, reqs map[string][]*dynamodb.WriteRequest) error {
	min := func(i, j int) int {
		if i < j {
			return i
		}
		return j
	}

	dictLen := func(in map[string][]*dynamodb.WriteRequest) int {
		result := 0
		for _, reqs := range in {
			result += len(reqs)
		}
		return result
	}

	// Fill 'out' with WriteRequests from 'in' until it 'out' has at most dynamoMaxBatchSize requests. Remove those requests from 'in'.
	fillReq := func(in map[string][]*dynamodb.WriteRequest, out map[string][]*dynamodb.WriteRequest) {
		outLen, inLen := dictLen(out), dictLen(in)
		toFill := min(inLen, dynamoMaxBatchSize-outLen)
		for toFill > 0 {
			for tableName := range in {
				reqs := in[tableName]
				taken := min(len(reqs), toFill)
				if taken > 0 {
					out[tableName] = append(out[tableName], reqs[:taken]...)
					in[tableName] = reqs[taken:]
					toFill -= taken
				}
			}
		}
	}

	copyUnprocessed := func(in map[string][]*dynamodb.WriteRequest, out map[string][]*dynamodb.WriteRequest) {
		for tableName, unprocessReqs := range in {
			out[tableName] = append(out[tableName], unprocessReqs...)
			dynamoUnprocessedItems.Add(float64(len(unprocessReqs)))
		}
	}

	outstanding, unprocessed := reqs, map[string][]*dynamodb.WriteRequest{}
	backoff := minBackoff
	for dictLen(outstanding)+dictLen(unprocessed) > 0 {
		reqs := map[string][]*dynamodb.WriteRequest{}
		fillReq(unprocessed, reqs)
		fillReq(outstanding, reqs)

		var resp *dynamodb.BatchWriteItemOutput
		err := instrument.TimeRequestHistogram(ctx, "DynamoDB.BatchWriteItem", dynamoRequestDuration, func(_ context.Context) error {
			var err error
			resp, err = c.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems:           reqs,
				ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
			})
			return err
		})
		for _, cc := range resp.ConsumedCapacity {
			dynamoConsumedCapacity.WithLabelValues("DynamoDB.BatchWriteItem").
				Add(float64(*cc.CapacityUnits))
		}

		if err != nil {
			recordDynamoError(err)
		}

		// If there are unprocessed items, backoff and retry those items.
		if resp.UnprocessedItems != nil && dictLen(resp.UnprocessedItems) > 0 {
			copyUnprocessed(resp.UnprocessedItems, unprocessed)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		// If we get provisionedThroughputExceededException, then no items were processed,
		// so back off and retry all.
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
			copyUnprocessed(reqs, unprocessed)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		// All other errors are fatal.
		if err != nil {
			return err
		}

		backoff = minBackoff
	}

	return nil
}

func (c *dynamoDBBackoffClient) queryPages(ctx context.Context, input *dynamodb.QueryInput, callback func(resp interface{}, lastPage bool) (shouldContinue bool)) error {
	request, _ := c.client.QueryRequest(input)
	backoff := minBackoff

	for page := request; page != nil; page = page.NextPage() {
		err := instrument.TimeRequestHistogram(ctx, "DynamoDB.QueryPages", dynamoRequestDuration, func(_ context.Context) error {
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

			return page.Error()
		}

		if getNextPage := callback(page.Data(), !page.HasNextPage()); !getNextPage {
			return page.Error()
		}

		backoff = minBackoff
	}

	return nil
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
