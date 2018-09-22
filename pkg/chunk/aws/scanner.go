package aws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// ScanTable reads the whole of a table on multiple goroutines in
// parallel, calling back with batches of results on one of the
// callbacks for each goroutine.
func (a storageClient) ScanTable(ctx context.Context, tableName string, withValue bool, callbacks []func(result chunk.ReadBatch)) error {
	var outerErr error
	projection := hashKey + "," + rangeKey
	if withValue {
		projection += "," + valueKey
	}
	var readerGroup sync.WaitGroup
	readerGroup.Add(len(callbacks))
	for segment, callback := range callbacks {
		go func(segment int, callback func(result chunk.ReadBatch)) {
			input := &dynamodb.ScanInput{
				TableName:              aws.String(tableName),
				ProjectionExpression:   aws.String(projection),
				Segment:                aws.Int64(int64(segment)),
				TotalSegments:          aws.Int64(int64(len(callbacks))),
				ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
			}
			err := a.DynamoDB.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
				if cc := page.ConsumedCapacity; cc != nil {
					dynamoConsumedCapacity.WithLabelValues("DynamoDB.ScanTable", *cc.TableName).
						Add(float64(*cc.CapacityUnits))
				}

				callback(&dynamoDBReadResponse{items: page.Items})
				return true
			})
			if err != nil {
				outerErr = err
				// TODO: abort all segments
			}
			readerGroup.Done()
		}(segment, callback)
	}
	// Wait until all reader segments have finished
	readerGroup.Wait()
	return outerErr
}
