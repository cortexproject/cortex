package chunk

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/net/context"

	"github.com/weaveworks/common/instrument"
	"github.com/weaveworks/cortex/util"
)

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"

	// For dynamodb errors
	tableNameLabel   = "table"
	errorReasonLabel = "error"
	otherError       = "other"

	provisionedThroughputExceededException = "ProvisionedThroughputExceededException"

	// Backoff for dynamoDB requests, to match AWS lib - see:
	// https://github.com/aws/aws-sdk-go/blob/master/service/dynamodb/customizations.go
	minBackoff = 50 * time.Millisecond
	maxBackoff = 50 * time.Second
	maxRetries = 20

	// See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
	dynamoMaxBatchSize = 25
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
	}, []string{tableNameLabel, errorReasonLabel})
)

func init() {
	prometheus.MustRegister(dynamoRequestDuration)
	prometheus.MustRegister(dynamoConsumedCapacity)
	prometheus.MustRegister(dynamoFailures)
}

// DynamoDBClientConfig specifies config for a DynamoDB client.
type DynamoDBClientConfig struct {
	URL util.URLValue
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *DynamoDBClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.URL, "dynamodb.url", "DynamoDB endpoint URL with escaped Key and Secret encoded. "+
		"If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<table-name> to use a mock in-memory implementation.")
}

type dynamoClientAdapter struct {
	DynamoDB dynamodbiface.DynamoDBAPI
}

// NewDynamoDBClient makes a new DynamoDBClient
func NewDynamoDBClient(cfg DynamoDBClientConfig) (StorageClient, string, error) {
	dynamoDBURL := cfg.URL.URL
	tableName := strings.TrimPrefix(dynamoDBURL.Path, "/")

	if dynamoDBURL.Scheme == "inmemory" {
		return NewMockStorage(), tableName, nil
	}

	dynamoDBConfig, err := awsConfigFromURL(dynamoDBURL)
	if err != nil {
		return nil, "", err
	}

	dynamoDBClient := dynamoClientAdapter{dynamodb.New(session.New(dynamoDBConfig))}
	return dynamoDBClient, tableName, nil
}

func (d dynamoClientAdapter) NewWriteBatch() WriteBatch {
	return dynamoDBWriteBatch(map[string][]*dynamodb.WriteRequest{})
}

// batchWrite writes requests to the underlying storage, handling retires and backoff.
func (d dynamoClientAdapter) BatchWrite(ctx context.Context, input WriteBatch) error {
	outstanding := input.(dynamoDBWriteBatch)
	unprocessed := map[string][]*dynamodb.WriteRequest{}
	backoff, numRetries := minBackoff, 0
	for dictLen(outstanding)+dictLen(unprocessed) > 0 && numRetries < maxRetries {
		reqs := map[string][]*dynamodb.WriteRequest{}
		takeReqs(unprocessed, reqs, dynamoMaxBatchSize)
		takeReqs(outstanding, reqs, dynamoMaxBatchSize)
		var resp *dynamodb.BatchWriteItemOutput

		err := instrument.TimeRequestHistogram(ctx, "DynamoDB.BatchWriteItem", dynamoRequestDuration, func(_ context.Context) error {
			var err error
			resp, err = d.DynamoDB.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
			for tableName := range reqs {
				recordDynamoError(tableName, err)
			}
		}

		// If there are unprocessed items, backoff and retry those items.
		if unprocessedItems := resp.UnprocessedItems; unprocessedItems != nil && dictLen(unprocessedItems) > 0 {
			takeReqs(unprocessedItems, unprocessed, -1)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			continue
		}

		// If we get provisionedThroughputExceededException, then no items were processed,
		// so back off and retry all.
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
			takeReqs(reqs, unprocessed, -1)
			time.Sleep(backoff)
			backoff = nextBackoff(backoff)
			numRetries++
			continue
		}

		// All other errors are fatal.
		if err != nil {
			return err
		}

		backoff = minBackoff
		numRetries = 0
	}

	if valuesLeft := dictLen(outstanding) + dictLen(unprocessed); valuesLeft > 0 {
		return fmt.Errorf("failed to write chunk after %d retries, %d values remaining", numRetries, valuesLeft)
	}
	return nil
}

func (d dynamoClientAdapter) QueryPages(ctx context.Context, entry IndexEntry, callback func(result ReadBatch, lastPage bool) (shouldContinue bool)) error {
	input := &dynamodb.QueryInput{
		TableName: aws.String(entry.TableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(entry.HashValue)},
				},
				ComparisonOperator: aws.String(dynamodb.ComparisonOperatorEq),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	if entry.RangeValuePrefix != nil {
		input.KeyConditions[rangeKey] = &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{
				{B: entry.RangeValuePrefix},
			},
			ComparisonOperator: aws.String(dynamodb.ComparisonOperatorBeginsWith),
		}
	} else if entry.RangeValueStart != nil {
		input.KeyConditions[rangeKey] = &dynamodb.Condition{
			AttributeValueList: []*dynamodb.AttributeValue{
				{B: entry.RangeValueStart},
			},
			ComparisonOperator: aws.String(dynamodb.ComparisonOperatorGe),
		}
	}

	request, _ := d.DynamoDB.QueryRequest(input)
	backoff := minBackoff
	for page := request; page != nil; page = page.NextPage() {
		err := instrument.TimeRequestHistogram(ctx, "DynamoDB.QueryPages", dynamoRequestDuration, func(_ context.Context) error {
			return page.Send()
		})

		if cc := page.Data.(*dynamodb.QueryOutput).ConsumedCapacity; cc != nil {
			dynamoConsumedCapacity.WithLabelValues("DynamoDB.QueryPages").
				Add(float64(*cc.CapacityUnits))
		}

		if err != nil {
			recordDynamoError(*input.TableName, err)

			if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == provisionedThroughputExceededException {
				time.Sleep(backoff)
				backoff = nextBackoff(backoff)
				continue
			}

			return page.Error
		}

		queryOutput := page.Data.(*dynamodb.QueryOutput)
		if getNextPage := callback(dynamoDBReadBatch(queryOutput.Items), !page.HasNextPage()); !getNextPage {
			return page.Error
		}

		backoff = minBackoff
	}

	return nil
}

func (d dynamoClientAdapter) ListTables() ([]string, error) {
	table := []string{}
	if err := d.DynamoDB.ListTablesPages(&dynamodb.ListTablesInput{}, func(resp *dynamodb.ListTablesOutput, _ bool) bool {
		for _, s := range resp.TableNames {
			table = append(table, *s)
		}
		return true
	}); err != nil {
		return nil, err
	}
	return table, nil
}

func (d dynamoClientAdapter) CreateTable(name string, readCapacity, writeCapacity int64) error {
	input := &dynamodb.CreateTableInput{
		TableName: aws.String(name),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String(rangeKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeB),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String(rangeKey),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
	}
	_, err := d.DynamoDB.CreateTable(input)
	return err
}

func (d dynamoClientAdapter) DescribeTable(name string) (readCapacity, writeCapacity int64, status string, err error) {
	out, err := d.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(name),
	})
	if err != nil {
		return 0, 0, "", err
	}

	return *out.Table.ProvisionedThroughput.ReadCapacityUnits, *out.Table.ProvisionedThroughput.WriteCapacityUnits, *out.Table.TableStatus, nil
}

func (d dynamoClientAdapter) UpdateTable(name string, readCapacity, writeCapacity int64) error {
	_, err := d.DynamoDB.UpdateTable(&dynamodb.UpdateTableInput{
		TableName: aws.String(name),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(readCapacity),
			WriteCapacityUnits: aws.Int64(writeCapacity),
		},
	})
	return err
}

type dynamoDBWriteBatch map[string][]*dynamodb.WriteRequest

func (b dynamoDBWriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	item := map[string]*dynamodb.AttributeValue{
		hashKey:  {S: aws.String(hashValue)},
		rangeKey: {B: rangeValue},
	}

	if value != nil {
		item[valueKey] = &dynamodb.AttributeValue{B: value}
	}

	b[tableName] = append(b[tableName], &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: item,
		},
	})
}

type dynamoDBReadBatch []map[string]*dynamodb.AttributeValue

func (b dynamoDBReadBatch) Len() int {
	return len(b)
}

func (b dynamoDBReadBatch) RangeValue(i int) []byte {
	return b[i][rangeKey].B
}

func (b dynamoDBReadBatch) Value(i int) []byte {
	chunkValue, ok := b[i][valueKey]
	if !ok {
		return nil
	}
	return chunkValue.B
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

func recordDynamoError(tableName string, err error) {
	if awsErr, ok := err.(awserr.Error); ok {
		dynamoFailures.WithLabelValues(tableName, awsErr.Code()).Add(float64(1))
	} else {
		dynamoFailures.WithLabelValues(tableName, otherError).Add(float64(1))
	}
}

func dictLen(b map[string][]*dynamodb.WriteRequest) int {
	result := 0
	for _, reqs := range b {
		result += len(reqs)
	}
	return result
}

// Fill 'to' with WriteRequests from 'from' until 'to' has at most max requests. Remove those requests from 'from'.
func takeReqs(from, to map[string][]*dynamodb.WriteRequest, max int) {
	outLen, inLen := dictLen(to), dictLen(from)
	toFill := inLen
	if max > 0 {
		toFill = util.Min(inLen, max-outLen)
	}
	for toFill > 0 {
		for tableName, fromReqs := range from {
			taken := util.Min(len(fromReqs), toFill)
			if taken > 0 {
				to[tableName] = append(to[tableName], fromReqs[:taken]...)
				from[tableName] = fromReqs[taken:]
				toFill -= taken
			}
		}
	}
}
