package aws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/cortexproject/cortex/pkg/chunk"
)

func (a storageClient) ScanTable(ctx context.Context, tableName string, callbacks []func(result chunk.ReadBatch)) error {
	var outerErr error
	var readerGroup sync.WaitGroup
	readerGroup.Add(len(callbacks))
	for segment, callback := range callbacks {
		go func(segment int, callback func(result chunk.ReadBatch)) {
			err := a.segmentScan(segment, len(callbacks), tableName, callback)
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

func (a storageClient) segmentScan(segment, totalSegments int, tableName string, callback func(result chunk.ReadBatch)) error {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(tableName),
		ProjectionExpression: aws.String(hashKey + "," + rangeKey),
		Segment:              aws.Int64(int64(segment)),
		TotalSegments:        aws.Int64(int64(totalSegments)),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	err := a.DynamoDB.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		callback(&dynamoDBReadResponse{items: page.Items})
		return true
	})
	if err != nil {
		return err
	}
	return nil
}
