package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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
	_, err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)

}

func Test_newDynamodbKV(t *testing.T) {
	_, err := newDynamodbKV(Config{Region: "us-west-2", TableName: "TEST"}, TestLogger{})

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
	_, err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
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
	update := map[dynamodbKey]dynamodbItem{
		ddbKeyUpdate: {
			data:    []byte{},
			version: 0,
		},
	}
	delete := []dynamodbKey{ddbKeyDelete}

	ddbClientMock := &mockDynamodb{
		transactWriteItem: func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			require.NotNil(t, input.TransactItems)
			require.EqualValues(t, 2, len(input.TransactItems))
			require.True(t,
				(checkPutForItem(input.TransactItems[0].Put, ddbKeyUpdate)) &&
					(checkPutForConditionalExpression(input.TransactItems[0].Put, ddbKeyUpdate)) &&
					(checkDeleteForItem(input.TransactItems[1].Delete, ddbKeyDelete)))
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	_, _, err := ddb.Batch(context.TODO(), update, delete)
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
		transactWriteItem: func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			numOfCalls++
			return &dynamodb.TransactWriteItemsOutput{}, nil
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

			_, _, err := ddb.Batch(context.TODO(), nil, delete)
			require.NoError(t, err)
			require.EqualValues(t, tc.expectedCalls, numOfCalls)

		})
	}

}

func Test_EmptyBatch(t *testing.T) {
	tableName := "TEST"
	ddbClientMock := &mockDynamodb{}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	_, _, err := ddb.Batch(context.TODO(), nil, nil)
	require.NoError(t, err)
}

func Test_Batch_Error(t *testing.T) {
	tableName := "TEST"

	testCases := []struct {
		name          string
		mockError     error
		expectedRetry bool
	}{
		{
			name:          "generic_error_no_retry",
			mockError:     fmt.Errorf("mocked error"),
			expectedRetry: false,
		},
		{
			name:          "conditional_check_failed_should_retry",
			mockError:     &dynamodb.ConditionalCheckFailedException{},
			expectedRetry: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ddbClientMock := &mockDynamodb{
				transactWriteItem: func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
					return nil, tc.mockError
				},
			}

			ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)

			delete := []dynamodbKey{{primaryKey: "PKDelete", sortKey: "SKDelete"}}
			_, retry, err := ddb.Batch(context.TODO(), nil, delete)

			require.Error(t, err)
			require.Equal(t, tc.expectedRetry, retry)
		})
	}
}

func checkPutForItem(request *dynamodb.Put, key dynamodbKey) bool {
	return request != nil &&
		request.Item != nil &&
		request.Item[primaryKey] != nil &&
		request.Item[sortKey] != nil &&
		*request.Item[primaryKey].S == key.primaryKey &&
		*request.Item[sortKey].S == key.sortKey
}

func checkDeleteForItem(request *dynamodb.Delete, key dynamodbKey) bool {
	return request != nil &&
		request.Key[primaryKey] != nil &&
		request.Key[sortKey] != nil &&
		*request.Key[primaryKey].S == key.primaryKey &&
		*request.Key[sortKey].S == key.sortKey
}

func checkPutForConditionalExpression(request *dynamodb.Put, key dynamodbKey) bool {
	return request != nil &&
		request.ConditionExpression != nil &&
		strings.Contains(*request.ConditionExpression, "version = :v")
}

type mockDynamodb struct {
	putItem           func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput
	transactWriteItem func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error)

	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamodb) PutItemWithContext(_ aws.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	return m.putItem(input), nil
}

func (m *mockDynamodb) TransactWriteItemsWithContext(_ aws.Context, input *dynamodb.TransactWriteItemsInput, _ ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return m.transactWriteItem(input)
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
