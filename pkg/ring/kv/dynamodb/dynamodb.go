package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

type dynamoDBAPI interface {
	dynamodb.QueryAPIClient
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type dynamodbKV struct {
	ddbClient dynamoDBAPI
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

	awsConfig := []func(*config.LoadOptions) error{}

	if len(cfg.Region) > 0 {
		awsConfig = append(awsConfig, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(
		context.Background(),
		awsConfig...,
	)
	if err != nil {
		return dynamodbKV{}, err
	}

	dynamoDB := dynamodb.NewFromConfig(awsCfg)

	return dynamodbKV{
		ddbClient: dynamoDB,
		logger:    logger,
		tableName: aws.String(cfg.TableName),
		ttlValue:  cfg.TTL,
	}, nil
}

func validateConfigInput(cfg Config) error {
	if len(cfg.TableName) < 3 {
		return fmt.Errorf("invalid dynamodb table name: %s", cfg.TableName)
	}
	return nil
}

func (kv dynamodbKV) getTTL() time.Duration {
	return kv.ttlValue
}

func (kv dynamodbKV) List(ctx context.Context, key dynamodbKey) ([]string, float64, error) {
	var keys []string
	var totalCapacity float64

	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		KeyConditions: map[string]types.Condition{
			primaryKey: {
				ComparisonOperator: types.ComparisonOperatorEq,
				AttributeValueList: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: key.primaryKey},
				},
			},
		},
		AttributesToGet: []string{sortKey},
	}

	paginator := dynamodb.NewQueryPaginator(kv.ddbClient, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, totalCapacity, err
		}
		totalCapacity += getCapacityUnits(page.ConsumedCapacity)
		for _, item := range page.Items {
			if v, ok := item[sortKey].(*types.AttributeValueMemberS); ok {
				keys = append(keys, v.Value)
			}
		}
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Query(ctx context.Context, key dynamodbKey, isPrefix bool) (map[string]dynamodbItem, float64, error) {
	keys := make(map[string]dynamodbItem)
	var totalCapacity float64

	co := types.ComparisonOperatorEq
	if isPrefix {
		co = types.ComparisonOperatorBeginsWith
	}

	input := &dynamodb.QueryInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		KeyConditions: map[string]types.Condition{
			primaryKey: {
				ComparisonOperator: co,
				AttributeValueList: []types.AttributeValue{
					&types.AttributeValueMemberS{Value: key.primaryKey},
				},
			},
		},
	}

	paginator := dynamodb.NewQueryPaginator(kv.ddbClient, input)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, totalCapacity, err
		}
		totalCapacity += getCapacityUnits(page.ConsumedCapacity)

		for _, item := range page.Items {
			itemVersion := int64(0)
			if v, ok := item[version].(*types.AttributeValueMemberN); ok {
				parsedVersion, err := strconv.ParseInt(v.Value, 10, 0)
				if err != nil {
					kv.logger.Log("msg", "failed to parse item version", "version", v.Value, "err", err)
				} else {
					itemVersion = parsedVersion
				}
			}

			if d, ok := item[contentData].(*types.AttributeValueMemberB); ok {
				if s, ok := item[sortKey].(*types.AttributeValueMemberS); ok {
					keys[s.Value] = dynamodbItem{
						data:    d.Value,
						version: itemVersion,
					}
				}
			}
		}
	}

	return keys, totalCapacity, nil
}

func (kv dynamodbKV) Delete(ctx context.Context, key dynamodbKey) (float64, error) {
	input := &dynamodb.DeleteItemInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Key:                    generateItemKey(key),
	}
	output, err := kv.ddbClient.DeleteItem(ctx, input)
	totalCapacity := getCapacityUnits(output.ConsumedCapacity)
	return totalCapacity, err
}

func (kv dynamodbKV) Put(ctx context.Context, key dynamodbKey, data []byte) (float64, error) {
	input := &dynamodb.PutItemInput{
		TableName:              kv.tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		Item:                   kv.generatePutItemRequest(key, dynamodbItem{data: data}),
	}
	output, err := kv.ddbClient.PutItem(ctx, input)
	totalCapacity := getCapacityUnits(output.ConsumedCapacity)
	return totalCapacity, err
}

func (kv dynamodbKV) Batch(ctx context.Context, put map[dynamodbKey]dynamodbItem, delete []dynamodbKey) (float64, bool, error) {
	totalCapacity := float64(0)
	writeRequestSize := len(put) + len(delete)
	if writeRequestSize == 0 {
		return totalCapacity, false, nil
	}

	writeRequestsSlices := make([][]types.TransactWriteItem, int(math.Ceil(float64(writeRequestSize)/float64(DdbBatchSizeLimit))))
	for i := range writeRequestsSlices {
		writeRequestsSlices[i] = make([]types.TransactWriteItem, 0, DdbBatchSizeLimit)
	}

	currIdx := 0
	for key, ddbItem := range put {
		item := kv.generatePutItemRequest(key, ddbItem)
		ddbPut := &types.Put{
			TableName:           kv.tableName,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(version) OR version = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberN{Value: strconv.FormatInt(ddbItem.version, 10)},
			},
		}

		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], types.TransactWriteItem{Put: ddbPut})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, key := range delete {
		item := generateItemKey(key)
		ddbDelete := &types.Delete{
			TableName: kv.tableName,
			Key:       item,
		}
		writeRequestsSlices[currIdx] = append(writeRequestsSlices[currIdx], types.TransactWriteItem{Delete: ddbDelete})
		if len(writeRequestsSlices[currIdx]) == DdbBatchSizeLimit {
			currIdx++
		}
	}

	for _, slice := range writeRequestsSlices {
		if len(slice) == 0 {
			continue
		}
		resp, err := kv.ddbClient.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: slice,
		})
		if err != nil {
			var checkFailed *types.ConditionalCheckFailedException
			isCheckFailed := errors.As(err, &checkFailed)
			if isCheckFailed {
				kv.logger.Log("msg", "conditional check failed on DynamoDB Batch", "err", err)
			}
			return totalCapacity, isCheckFailed, err
		}
		for _, consumedCapacity := range resp.ConsumedCapacity {
			totalCapacity += getCapacityUnits(&consumedCapacity)
		}
	}

	return totalCapacity, false, nil
}

func (kv dynamodbKV) generatePutItemRequest(key dynamodbKey, ddbItem dynamodbItem) map[string]types.AttributeValue {
	item := generateItemKey(key)
	item[contentData] = &types.AttributeValueMemberB{Value: ddbItem.data}
	item[version] = &types.AttributeValueMemberN{Value: strconv.FormatInt(ddbItem.version+1, 10)}

	if kv.getTTL() > 0 {
		item[timeToLive] = &types.AttributeValueMemberN{
			Value: strconv.FormatInt(time.Now().UTC().Add(kv.getTTL()).Unix(), 10),
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

func generateItemKey(key dynamodbKey) map[string]types.AttributeValue {
	resp := map[string]types.AttributeValue{
		primaryKey: &types.AttributeValueMemberS{Value: key.primaryKey},
	}
	if len(key.sortKey) > 0 {
		resp[sortKey] = &types.AttributeValueMemberS{Value: key.sortKey}
	}
	return resp
}

func getCapacityUnits(cap *types.ConsumedCapacity) float64 {
	if cap != nil && cap.CapacityUnits != nil {
		return *cap.CapacityUnits
	}
	return 0
}
