package dynamodb

import (
	"context"
	"errors"
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
	Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string]dynamodbItem, float64, error)
	Delete(ctx context.Context, key dynamodbKey) error
	Put(ctx context.Context, key dynamodbKey, data []byte) error
	Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (bool, error)
}

type dynamodbKV struct {
	ddbClient dynamodbiface.DynamoDBAPI
	logger    log.Logger
	tableName *string
	ttlValue  time.Duration
}

type dynamodbItem struct {
	data    []byte
	version int64
}

var (
	primaryKey  = "RingKey"
	sortKey     = "InstanceKey"
	contentData = "Data"
	timeToLive  = "ttl"
	version     = "version"
)

func newDynamodbKV(cfg Config, logger log.Logger) (dynamodbKV, error) {
	if err := validateConfigInput(cfg); err != nil {
		return dynamodbKV{}, err
	}

	sess, err := session.NewSession()
	if err != nil {
		return dynamodbKV{}, err
	}

	awsCfg := aws.NewConfig()
	if len(cfg.Region) > 0 {
		awsCfg = awsCfg.WithRegion(cfg.Region)
	}

	dynamoDB := dynamodb.New(sess, awsCfg)

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

func (kv dynamodbKV) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string]dynamodbItem, float64, error) {
	keys := make(map[string]dynamodbItem)
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
			itemVersion := int64(0)
			if item[version] != nil {
				parsedVersion, err := strconv.ParseInt(*item[version].N, 10, 0)
				if err != nil {
					kv.logger.Log("msg", "failed to parse item version", "version", *item[version].N, "err", err)
				} else {
					itemVersion = parsedVersion
				}
			}

			keys[*item[sortKey].S] = dynamodbItem{
				data:    item[contentData].B,
				version: itemVersion,
			}

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
		Item:                   kv.generatePutItemRequest(key, dynamodbItem{data: data}),
	}
	totalCapacity := float64(0)
	output, err := kv.ddbClient.PutItemWithContext(ctx, input)
	if err != nil {
		totalCapacity = getCapacityUnits(output.ConsumedCapacity)
	}
	return totalCapacity, err
}

func (kv dynamodbKV) Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (float64, bool, error) {
	totalCapacity := float64(0)
	writeRequestSize := len(put) + len(delete)
	if writeRequestSize == 0 {
		return totalCapacity, false, nil
	}

	writeRequestsSlices := make([][]*dynamodb.TransactWriteItem, int(math.Ceil(float64(writeRequestSize)/float64(DdbBatchSizeLimit))))
	for i := range writeRequestsSlices {
		writeRequestsSlices[i] = make([]*dynamodb.TransactWriteItem, 0, DdbBatchSizeLimit)
	}
	currIdx := 0
	for key, ddbItem := range put {
		item := kv.generatePutItemRequest(key, ddbItem)
		ddbPut := &dynamodb.Put{
			TableName: kv.tableName,
			Item:      item,
		}
		// condition for optimistic locking; DynamoDB will only succeed the request if either the version attribute does not exist
		// (for backwards compatibility) or the object version has not changed since it was last read
		ddbPut.ConditionExpression = aws.String("attribute_not_exists(version) OR version = :v")
		ddbPut.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":v": {N: aws.String(strconv.FormatInt(ddbItem.version, 10))},
		}

		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], &dynamodb.TransactWriteItem{Put: ddbPut})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, key := range delete {
		item := generateItemKey(key)
		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], &dynamodb.TransactWriteItem{
			Delete: &dynamodb.Delete{
				TableName: kv.tableName,
				Key:       item,
			},
		})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, slice := range writeRequestsSlices {
		transactItems := &dynamodb.TransactWriteItemsInput{
			TransactItems: slice,
		}
		resp, err := kv.ddbClient.TransactWriteItemsWithContext(ctx, transactItems)
		if err != nil {
			var checkFailed *dynamodb.ConditionalCheckFailedException
			isCheckFailedException := errors.As(err, &checkFailed)
			if isCheckFailedException {
				kv.logger.Log("msg", "conditional check failed on DynamoDB Batch", "item", fmt.Sprintf("%v", checkFailed.Item), "err", err)
			}
			return totalCapacity, isCheckFailedException, err
		}
		for _, consumedCapacity := range resp.ConsumedCapacity {
			totalCapacity += getCapacityUnits(consumedCapacity)
		}
	}

	return totalCapacity, false, nil
}

func (kv dynamodbKV) generatePutItemRequest(key dynamodbKey, ddbItem dynamodbItem) map[string]*dynamodb.AttributeValue {
	item := generateItemKey(key)
	item[contentData] = &dynamodb.AttributeValue{
		B: ddbItem.data,
	}
	item[version] = &dynamodb.AttributeValue{
		N: aws.String(strconv.FormatInt(ddbItem.version+1, 10)),
	}
	if kv.getTTL() > 0 {
		item[timeToLive] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(time.Now().UTC().Add(kv.getTTL()).Unix(), 10)),
		}
	}

	return item
}

type dynamodbKVWithTimeout struct {
	ddbClient dynamoDbClient
	timeout   time.Duration
}

func newDynamodbKVWithTimeout(client dynamoDbClient, timeout time.Duration) *dynamodbKVWithTimeout {
	return &dynamodbKVWithTimeout{ddbClient: client, timeout: timeout}
}

func (d *dynamodbKVWithTimeout) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.ddbClient.List(ctx, key)
}

func (d *dynamodbKVWithTimeout) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string]dynamodbItem, float64, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.ddbClient.Query(ctx, key, isPrefix)
}

func (d *dynamodbKVWithTimeout) Delete(ctx context.Context, key dynamodbKey) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.ddbClient.Delete(ctx, key)
}

func (d *dynamodbKVWithTimeout) Put(ctx context.Context, key dynamodbKey, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.ddbClient.Put(ctx, key, data)
}

func (d *dynamodbKVWithTimeout) Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.ddbClient.Batch(ctx, put, delete)
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
