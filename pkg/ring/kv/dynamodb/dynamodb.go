package dynamodb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/go-kit/log"
)

const (
	// DdbBatchSizeLimit Current limit of 25 actions per batch
	// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
	DdbBatchSizeLimit = 25
)

type dynamodbKey struct {
	primaryKey string
	sortKey    string
}

type dynamoDbClient interface {
	List(ctx context.Context, key dynamodbKey) ([]string, float64, error)
	Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, float64, error)
	Delete(ctx context.Context, key dynamodbKey) error
	Put(ctx context.Context, key dynamodbKey, data []byte) error
	Batch(ctx context.Context, put map[dynamodbKey][]byte, delete []dynamodbKey) error
}

type dynamodbKV struct {
	ddbClient dynamodbiface.DynamoDBAPI
	logger    log.Logger
	tableName *string
	ttlValue  time.Duration
}

var (
	primaryKey  = "RingKey"
	sortKey     = "InstanceKey"
	contentData = "Data"
	timeToLive  = "ttl"
)

func newDynamodbKV(cfg Config, logger log.Logger) (dynamodbKV, error) {
	if err := validateConfigInput(cfg); err != nil {
		return dynamodbKV{}, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return dynamodbKV{}, err
	}

	if len(cfg.Region) > 0 {
		sess.Config = &aws.Config{
			Region: aws.String(cfg.Region),
		}
	}

	dynamoDB := dynamodb.New(sess)

	ddbKV := &dynamodbKV{
		ddbClient: dynamoDB,
		logger:    logger,
		tableName: aws.String(cfg.TableName),
		ttlValue:  cfg.TTL,
	}

	return *ddbKV, nil
}

func validateConfigInput(cfg Config) error {
	if len(cfg.TableName) < 3 {
		return fmt.Errorf("invalid dynamodb table name: %s", cfg.TableName)
	}

	return nil
}

// for testing
func (kv dynamodbKV) getTTL() time.Duration {
	return kv.ttlValue
}

func (kv dynamodbKV) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	var keys []string
	var totalCapacity float64
	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		KeyConditions: map[string]*dynamodb.Condition{
			primaryKey: {
				ComparisonOperator: aws.String("EQ"),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(key.primaryKey),
					},
				},
			},
		},
		AttributesToGet: []*string{aws.String(sortKey)},
	}

	err := kv.ddbClient.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		totalCapacity += getCapacityUnits(output.ConsumedCapacity)
		for _, item := range output.Items {
			keys = append(keys, item[sortKey].String())
		}
		return true
	})
	if err != nil {
		return nil, totalCapacity, err
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string][]byte, float64, error) {
	keys := make(map[string][]byte)
	var totalCapacity float64
	co := dynamodb.ComparisonOperatorEq
	if isPrefix {
		co = dynamodb.ComparisonOperatorBeginsWith
	}
	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		KeyConditions: map[string]*dynamodb.Condition{
			primaryKey: {
				ComparisonOperator: aws.String(co),
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(key.primaryKey),
					},
				},
			},
		},
	}

	err := kv.ddbClient.QueryPagesWithContext(ctx, input, func(output *dynamodb.QueryOutput, _ bool) bool {
		totalCapacity += getCapacityUnits(output.ConsumedCapacity)
		for _, item := range output.Items {
			keys[*item[sortKey].S] = item[contentData].B
		}
		return true
	})
	if err != nil {
		return nil, totalCapacity, err
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Delete(ctx context.Context, key dynamodbKey) (float64, error) {
	input := &dynamodb.DeleteItemInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Key:                    generateItemKey(key),
	}
	totalCapacity := float64(0)
	output, err := kv.ddbClient.DeleteItemWithContext(ctx, input)
	if err != nil {
		totalCapacity = getCapacityUnits(output.ConsumedCapacity)
	}
	return totalCapacity, err
}

func (kv dynamodbKV) Put(ctx context.Context, key dynamodbKey, data []byte) (float64, error) {
	input := &dynamodb.PutItemInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
		Item:                   kv.generatePutItemRequest(key, data),
	}
	totalCapacity := float64(0)
	output, err := kv.ddbClient.PutItemWithContext(ctx, input)
	if err != nil {
		totalCapacity = getCapacityUnits(output.ConsumedCapacity)
	}
	return totalCapacity, err
}

func (kv dynamodbKV) Batch(ctx context.Context, put map[dynamodbKey][]byte, delete []dynamodbKey) (float64, error) {
	totalCapacity := float64(0)
	writeRequestSize := len(put) + len(delete)
	if writeRequestSize == 0 {
		return totalCapacity, nil
	}

	writeRequestsSlices := make([][]*dynamodb.WriteRequest, int(math.Ceil(float64(writeRequestSize)/float64(DdbBatchSizeLimit))))
	for i := 0; i < len(writeRequestsSlices); i++ {
		writeRequestsSlices[i] = make([]*dynamodb.WriteRequest, 0, DdbBatchSizeLimit)
	}

	currIdx := 0
	for key, data := range put {
		item := kv.generatePutItemRequest(key, data)
		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, key := range delete {
		item := generateItemKey(key)
		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: item,
			},
		})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, slice := range writeRequestsSlices {
		input := &dynamodb.BatchWriteItemInput{
			ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
			RequestItems: map[string][]*dynamodb.WriteRequest{
				*kv.tableName: slice,
			},
		}

		resp, err := kv.ddbClient.BatchWriteItemWithContext(ctx, input)
		if err != nil {
			return totalCapacity, err
		}
		for _, consumedCapacity := range resp.ConsumedCapacity {
			totalCapacity += getCapacityUnits(consumedCapacity)
		}

		if resp.UnprocessedItems != nil && len(resp.UnprocessedItems) > 0 {
			return totalCapacity, fmt.Errorf("error processing batch request for %s requests", resp.UnprocessedItems)
		}
	}

	return totalCapacity, nil
}

func (kv dynamodbKV) generatePutItemRequest(key dynamodbKey, data []byte) map[string]*dynamodb.AttributeValue {
	item := generateItemKey(key)
	item[contentData] = &dynamodb.AttributeValue{
		B: data,
	}
	if kv.getTTL() > 0 {
		item[timeToLive] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(time.Now().UTC().Add(kv.getTTL()).Unix(), 10)),
		}
	}

	return item
}

func generateItemKey(key dynamodbKey) map[string]*dynamodb.AttributeValue {
	resp := map[string]*dynamodb.AttributeValue{
		primaryKey: {
			S: aws.String(key.primaryKey),
		},
	}
	if len(key.sortKey) > 0 {
		resp[sortKey] = &dynamodb.AttributeValue{
			S: aws.String(key.sortKey),
		}
	}

	return resp
}

func getCapacityUnits(cap *dynamodb.ConsumedCapacity) float64 {
	if cap != nil && cap.CapacityUnits != nil {
		return *cap.CapacityUnits
	}
	return 0
}
