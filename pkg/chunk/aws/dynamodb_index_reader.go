package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	gklog "github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/cortexproject/cortex/pkg/chunk"
)

type dynamodbIndexReader struct {
	dynamoDBStorageClient

	log        gklog.Logger
	maxRetries int

	rowsRead prometheus.Counter
}

// NewDynamoDBIndexReader returns an object that can scan an entire index table
func NewDynamoDBIndexReader(cfg DynamoDBConfig, schemaCfg chunk.SchemaConfig, reg prometheus.Registerer, l gklog.Logger, rowsRead prometheus.Counter) (*dynamodbIndexReader, error) {
	client, err := newDynamoDBStorageClient(cfg, schemaCfg, reg)
	if err != nil {
		return nil, err
	}

	return &dynamodbIndexReader{
		dynamoDBStorageClient: *client,
		maxRetries:            cfg.BackoffConfig.MaxRetries,
		log:                   l,

		rowsRead: rowsRead,
	}, nil
}

func (r *dynamodbIndexReader) IndexTableNames(ctx context.Context) ([]string, error) {
	// fake up a table client - if we call NewDynamoDBTableClient() it will double-register metrics
	tableClient := dynamoTableClient{
		DynamoDB: r.DynamoDB,
		metrics:  r.metrics,
	}
	return tableClient.ListTables(ctx)
}

type seriesMap struct {
	mutex           sync.Mutex          // protect concurrent access to map
	seriesProcessed map[string]struct{} // set of all series processes (NOTE: does not scale indefinitely)
}

// ReadIndexEntries reads the whole of a table on multiple goroutines in parallel.
// Entries for the same HashValue and RangeValue should be passed to the same processor.
func (r *dynamodbIndexReader) ReadIndexEntries(ctx context.Context, tableName string, processors []chunk.IndexEntryProcessor) error {
	var outerErr error
	projection := hashKey + "," + rangeKey

	sm := &seriesMap{ // new map per table
		seriesProcessed: make(map[string]struct{}),
	}

	var readerGroup sync.WaitGroup
	readerGroup.Add(len(processors))
	// Start a goroutine for each processor
	for i, processor := range processors {
		go func(segment int, processor chunk.IndexEntryProcessor) {
			input := &dynamodb.ScanInput{
				TableName:              aws.String(tableName),
				ProjectionExpression:   aws.String(projection),
				Segment:                aws.Int64(int64(segment)),
				TotalSegments:          aws.Int64(int64(len(processors))),
				ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
			}
			withRetrys := func(req *request.Request) {
				req.Retryer = client.DefaultRetryer{NumMaxRetries: r.maxRetries}
			}
			err := r.DynamoDB.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
				if cc := page.ConsumedCapacity; cc != nil {
					r.metrics.dynamoConsumedCapacity.WithLabelValues("DynamoDB.ScanTable", *cc.TableName).
						Add(float64(*cc.CapacityUnits))
				}
				r.processPage(ctx, sm, processor, tableName, page)
				return true
			}, withRetrys)
			if err != nil {
				outerErr = err
				// TODO: abort all segments
			}
			processor.Flush()
			level.Info(r.log).Log("msg", "Segment finished", "segment", segment)
			readerGroup.Done()
		}(i, processor)
	}
	// Wait until all reader segments have finished
	readerGroup.Wait()
	if outerErr != nil {
		return outerErr
	}
	return nil
}

func (r *dynamodbIndexReader) processPage(ctx context.Context, sm *seriesMap, processor chunk.IndexEntryProcessor, tableName string, page *dynamodb.ScanOutput) {
	for _, item := range page.Items {
		r.rowsRead.Inc()
		rangeValue := item[rangeKey].B
		if !isSeriesIndexEntry(rangeValue) {
			continue
		}
		hashValue := aws.StringValue(item[hashKey].S)
		orgStr, day, seriesID, err := decodeHashValue(hashValue)
		if err != nil {
			level.Error(r.log).Log("msg", "Failed to decode hash value", "err", err)
			continue
		}
		queryHashKey := orgStr + ":" + day + ":" + seriesID // from v9Entries.GetChunkWriteEntries()

		// Check whether we have already processed this series
		sm.mutex.Lock()
		if _, exists := sm.seriesProcessed[queryHashKey]; exists {
			sm.mutex.Unlock()
			continue
		}
		sm.seriesProcessed[queryHashKey] = struct{}{}
		sm.mutex.Unlock()

		err = r.queryChunkEntriesForSeries(ctx, processor, tableName, queryHashKey)
		if err != nil {
			level.Error(r.log).Log("msg", "error while reading series", "err", err)
			return
		}
	}
}

func (r *dynamodbIndexReader) queryChunkEntriesForSeries(ctx context.Context, processor chunk.IndexEntryProcessor, tableName, queryHashKey string) error {
	// DynamoDB query which just says "all rows with hashKey X"
	// This is hard-coded for schema v9
	input := &dynamodb.QueryInput{
		TableName: aws.String(tableName),
		KeyConditions: map[string]*dynamodb.Condition{
			hashKey: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{S: aws.String(queryHashKey)},
				},
				ComparisonOperator: aws.String(dynamodb.ComparisonOperatorEq),
			},
		},
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	var result error
	err := r.DynamoDB.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		if cc := output.ConsumedCapacity; cc != nil {
			r.metrics.dynamoConsumedCapacity.WithLabelValues("DynamoDB.QueryPages", *cc.TableName).
				Add(float64(*cc.CapacityUnits))
		}

		for _, item := range output.Items {
			err := processor.ProcessIndexEntry(chunk.IndexEntry{
				TableName:  tableName,
				HashValue:  aws.StringValue(item[hashKey].S),
				RangeValue: item[rangeKey].B})
			if err != nil {
				result = errors.Wrap(err, "processor error")
				return false
			}
		}
		return true
	})
	if err != nil {
		return errors.Wrap(err, "DynamoDB error")
	}
	return result
}

func isSeriesIndexEntry(rangeValue []byte) bool {
	const chunkTimeRangeKeyV3 = '3' // copied from pkg/chunk/schema.go
	return len(rangeValue) > 2 && rangeValue[len(rangeValue)-2] == chunkTimeRangeKeyV3
}

func decodeHashValue(hashValue string) (orgStr, day, seriesID string, err error) {
	hashParts := strings.SplitN(hashValue, ":", 3)
	if len(hashParts) != 3 {
		err = fmt.Errorf("unrecognized hash value: %q", hashValue)
		return
	}
	orgStr = hashParts[0]
	day = hashParts[1]
	seriesID = hashParts[2]
	return
}
