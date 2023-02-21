package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/stretchr/testify/require"
)

func Test_TTLDisabled(t *testing.T) {
	ddbClientMock := &mockDynamodb{
		putItem: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			require.Nil(t, input.Item["ttl"])
			return &dynamodb.PutItemOutput{}
		},
	}

	ddb := newDynamodbClientMock("TEST", ddbClientMock, 0)
	err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)

}

func Test_TTL(t *testing.T) {
	ddbClientMock := &mockDynamodb{
		putItem: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			require.NotNil(t, input.Item["ttl"].N)
			parsedTime, err := strconv.ParseInt(*input.Item["ttl"].N, 10, 64)
			require.NoError(t, err)
			require.Greater(t, time.Unix(parsedTime, 0), time.Now().UTC().Add(4*time.Hour), 10)
			require.LessOrEqual(t, time.Unix(parsedTime, 0), time.Now().UTC().Add(6*time.Hour), 10)
			return &dynamodb.PutItemOutput{}
		},
	}

	ddb := newDynamodbClientMock("TEST", ddbClientMock, 5*time.Hour)
	err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)
}

func Test_Batch(t *testing.T) {
	tableName := "TEST"
	ddbKeyUpdate := dynamodbKey{
		primaryKey: "PKUpdate",
		sortKey:    "SKUpdate",
	}
	ddbKeyDelete := dynamodbKey{
		primaryKey: "PKDelete",
		sortKey:    "SKDelete",
	}
	update := map[dynamodbKey][]byte{
		ddbKeyUpdate: {},
	}
	delete := []dynamodbKey{ddbKeyDelete}

	ddbClientMock := &mockDynamodb{
		batchWriteItem: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			require.NotNil(t, input.RequestItems[tableName])
			require.EqualValues(t, 2, len(input.RequestItems[tableName]))
			require.True(t,
				(checkPutRequestForItem(input.RequestItems[tableName][0], ddbKeyUpdate) || checkPutRequestForItem(input.RequestItems[tableName][1], ddbKeyUpdate)) &&
					(checkDeleteRequestForItem(input.RequestItems[tableName][0], ddbKeyDelete) || checkDeleteRequestForItem(input.RequestItems[tableName][1], ddbKeyDelete)))
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	err := ddb.Batch(context.TODO(), update, delete)
	require.NoError(t, err)
}

func Test_BatchSlices(t *testing.T) {
	tableName := "TEST"
	ddbKeyDelete := dynamodbKey{
		primaryKey: "PKDelete",
		sortKey:    "SKDelete",
	}
	numOfCalls := 0
	ddbClientMock := &mockDynamodb{
		batchWriteItem: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			numOfCalls++
			return &dynamodb.BatchWriteItemOutput{}, nil
		},
	}
	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)

	for _, tc := range []struct {
		name            string
		numOfExecutions int
		expectedCalls   int
	}{
		// These tests follow each other (end state of KV in state is starting point in the next state).
		{
			name:            "Test slice on lower bound",
			numOfExecutions: 24,
			expectedCalls:   1,
		},
		{
			name:            "Test slice on exact size",
			numOfExecutions: 25,
			expectedCalls:   1,
		},
		{
			name:            "Test slice on upper bound",
			numOfExecutions: 26,
			expectedCalls:   2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			numOfCalls = 0
			delete := make([]dynamodbKey, 0, tc.numOfExecutions)
			for i := 0; i < tc.numOfExecutions; i++ {
				delete = append(delete, ddbKeyDelete)
			}

			err := ddb.Batch(context.TODO(), nil, delete)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectedCalls, numOfCalls)

		})
	}

}

func Test_EmptyBatch(t *testing.T) {
	tableName := "TEST"
	ddbClientMock := &mockDynamodb{}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	err := ddb.Batch(context.TODO(), nil, nil)
	require.NoError(t, err)
}

func Test_Batch_UnprocessedItems(t *testing.T) {
	tableName := "TEST"
	ddbKeyDelete := dynamodbKey{
		primaryKey: "PKDelete",
		sortKey:    "SKDelete",
	}
	delete := []dynamodbKey{ddbKeyDelete}

	ddbClientMock := &mockDynamodb{
		batchWriteItem: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					tableName: {&dynamodb.WriteRequest{
						PutRequest: &dynamodb.PutRequest{Item: generateItemKey(ddbKeyDelete)}},
					},
				},
			}, nil
		},
	}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	err := ddb.Batch(context.TODO(), nil, delete)
	require.Errorf(t, err, "error processing batch dynamodb")
}

func Test_Batch_Error(t *testing.T) {
	tableName := "TEST"
	ddbKeyDelete := dynamodbKey{
		primaryKey: "PKDelete",
		sortKey:    "SKDelete",
	}
	delete := []dynamodbKey{ddbKeyDelete}

	ddbClientMock := &mockDynamodb{
		batchWriteItem: func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			return &dynamodb.BatchWriteItemOutput{}, fmt.Errorf("mocked error")
		},
	}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	err := ddb.Batch(context.TODO(), nil, delete)
	require.Errorf(t, err, "mocked error")
}

func checkPutRequestForItem(request *dynamodb.WriteRequest, key dynamodbKey) bool {
	return request.PutRequest != nil &&
		request.PutRequest.Item[primaryKey] != nil &&
		request.PutRequest.Item[sortKey] != nil &&
		*request.PutRequest.Item[primaryKey].S == key.primaryKey &&
		*request.PutRequest.Item[sortKey].S == key.sortKey
}

func checkDeleteRequestForItem(request *dynamodb.WriteRequest, key dynamodbKey) bool {
	return request.DeleteRequest != nil &&
		request.DeleteRequest.Key[primaryKey] != nil &&
		request.DeleteRequest.Key[sortKey] != nil &&
		*request.DeleteRequest.Key[primaryKey].S == key.primaryKey &&
		*request.DeleteRequest.Key[sortKey].S == key.sortKey
}

type mockDynamodb struct {
	putItem        func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput
	batchWriteItem func(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)

	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamodb) PutItemWithContext(_ context.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	return m.putItem(input), nil
}

func (m *mockDynamodb) BatchWriteItemWithContext(ctx context.Context, input *dynamodb.BatchWriteItemInput, opts ...request.Option) (*dynamodb.BatchWriteItemOutput, error) {
	return m.batchWriteItem(input)
}

func newDynamodbClientMock(tableName string, mock *mockDynamodb, ttl time.Duration) *dynamodbKV {
	ddbKV := &dynamodbKV{
		ddbClient: mock,
		logger:    TestLogger{},
		tableName: aws.String(tableName),
		ttlValue:  ttl,
	}

	return ddbKV
}
