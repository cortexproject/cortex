package dynamodb

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func Test_TTLDisabled(t *testing.T) {
	ddbClientMock := &mockDynamodb{
		putItem: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			// ttl must be absent
			_, hasTTL := input.Item["ttl"]
			require.False(t, hasTTL)
			return &dynamodb.PutItemOutput{}
		},
	}

	ddb := newDynamodbClientMock("TEST", ddbClientMock, 0)
	_, err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)
}

func Test_newDynamodbKV(t *testing.T) {
	// Just ensures construction doesn’t error (no API call happens here)
	_, err := newDynamodbKV(Config{Region: "us-west-2", TableName: "TEST"}, TestLogger{})
	require.NoError(t, err)
}

func Test_TTL(t *testing.T) {
	ddbClientMock := &mockDynamodb{
		putItem: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			av, ok := input.Item["ttl"]
			require.True(t, ok, "ttl attribute missing")
			num, ok := av.(*types.AttributeValueMemberN)
			require.True(t, ok, "ttl should be a number")

			parsedTime, err := strconv.ParseInt(num.Value, 10, 64)
			require.NoError(t, err)

			ts := time.Unix(parsedTime, 0)
			require.True(t, ts.After(time.Now().UTC().Add(4*time.Hour)))
			require.True(t, !ts.After(time.Now().UTC().Add(6*time.Hour)))
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
	toDelete := []dynamodbKey{ddbKeyDelete}

	ddbClientMock := &mockDynamodb{
		transactWriteItem: func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			require.NotNil(t, input.TransactItems)
			require.EqualValues(t, 2, len(input.TransactItems))
			require.True(t,
				(checkPutForItem(input.TransactItems[0].Put, ddbKeyUpdate)) &&
					(checkPutForConditionalExpression(input.TransactItems[0].Put)) &&
					(checkDeleteForItem(input.TransactItems[1].Delete, ddbKeyDelete)))
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	ddb := newDynamodbClientMock(tableName, ddbClientMock, 5*time.Hour)
	_, _, err := ddb.Batch(context.TODO(), update, toDelete)
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
			toDelete := make([]dynamodbKey, 0, tc.numOfExecutions)
			for i := 0; i < tc.numOfExecutions; i++ {
				toDelete = append(toDelete, ddbKeyDelete)
			}

			_, _, err := ddb.Batch(context.TODO(), nil, toDelete)
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
			mockError:     &types.ConditionalCheckFailedException{},
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

			toDelete := []dynamodbKey{{primaryKey: "PKDelete", sortKey: "SKDelete"}}
			_, retry, err := ddb.Batch(context.TODO(), nil, toDelete)

			require.Error(t, err)
			require.Equal(t, tc.expectedRetry, retry)
		})
	}
}

func checkPutForItem(request *types.Put, key dynamodbKey) bool {
	if request == nil || request.Item == nil {
		return false
	}
	pk, ok := request.Item[primaryKey].(*types.AttributeValueMemberS)
	if !ok {
		return false
	}
	sk, ok := request.Item[sortKey].(*types.AttributeValueMemberS)
	if !ok {
		return false
	}
	return pk.Value == key.primaryKey && sk.Value == key.sortKey
}

func checkDeleteForItem(request *types.Delete, key dynamodbKey) bool {
	if request == nil || request.Key == nil {
		return false
	}
	pk, ok := request.Key[primaryKey].(*types.AttributeValueMemberS)
	if !ok {
		return false
	}
	sk, ok := request.Key[sortKey].(*types.AttributeValueMemberS)
	if !ok {
		return false
	}
	return pk.Value == key.primaryKey && sk.Value == key.sortKey
}

func checkPutForConditionalExpression(request *types.Put) bool {
	return request != nil &&
		request.ConditionExpression != nil &&
		strings.Contains(*request.ConditionExpression, "version = :v")
}

// ---- v2 mock ----

type mockDynamodb struct {
	putItem           func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput
	transactWriteItem func(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error)
	query             func(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
}

// Implement the minimal methods our code calls (v2 signatures).
func (m *mockDynamodb) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if m.putItem == nil {
		return &dynamodb.PutItemOutput{}, nil
	}
	return m.putItem(input), nil
}

func (m *mockDynamodb) DeleteItem(_ context.Context, _ *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	// Not used in these tests; return empty success.
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *mockDynamodb) TransactWriteItems(_ context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if m.transactWriteItem == nil {
		return &dynamodb.TransactWriteItemsOutput{}, nil
	}
	return m.transactWriteItem(input)
}

func (m *mockDynamodb) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if m.query == nil {
		return &dynamodb.QueryOutput{}, nil
	}
	return m.query(input)
}

func newDynamodbClientMock(tableName string, mock *mockDynamodb, ttl time.Duration) *dynamodbKV {
	return &dynamodbKV{
		ddbClient: mock, // satisfies our dynamodbAPI interface
		logger:    TestLogger{},
		tableName: aws.String(tableName),
		ttlValue:  ttl,
	}
}
