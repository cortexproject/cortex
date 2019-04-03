package aws

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/common/model"

	"github.com/cortexproject/cortex/pkg/chunk"
)

// ScanTable reads the whole of a table on multiple goroutines in
// parallel, calling back with batches of results on one of the
// callbacks for each goroutine.
func (a dynamoDBStorageClient) Scan(ctx context.Context, from, through model.Time, withValue bool, callbacks []func(result chunk.ReadBatch)) error {
	tableName, err := a.schemaCfg.ChunkTableFor(from) // FIXME ignoring 'through'
	if err != nil {
		return err
	}
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
			withRetrys := func(req *request.Request) {
				req.Retryer = client.DefaultRetryer{NumMaxRetries: a.cfg.backoffConfig.MaxRetries}
			}
			err := a.DynamoDB.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
				if cc := page.ConsumedCapacity; cc != nil {
					dynamoConsumedCapacity.WithLabelValues("DynamoDB.ScanTable", *cc.TableName).
						Add(float64(*cc.CapacityUnits))
				}

				callback(&dynamoDBReadResponse{items: page.Items})
				return true
			}, withRetrys)
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

func (a s3ObjectClient) Scan(ctx context.Context, from, through model.Time, withValue bool, callbacks []func(result chunk.ReadBatch)) error {
	return errors.New("not implemented")
}
