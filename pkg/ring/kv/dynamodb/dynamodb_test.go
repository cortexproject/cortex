package dynamodb

import (
	"context"
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
		f: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			require.Nil(t, input.Item["ttl"])
			return &dynamodb.PutItemOutput{}
		},
	}

	ddb := newDynamodbClientMock(ddbClientMock, 0)
	err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)

}

func Test_TTL(t *testing.T) {
	ddbClientMock := &mockDynamodb{
		f: func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput {
			require.NotNil(t, input.Item["ttl"].N)
			parsedTime, err := strconv.ParseInt(*input.Item["ttl"].N, 10, 64)
			require.NoError(t, err)
			require.Greater(t, time.Unix(parsedTime, 0), time.Now().UTC().Add(4*time.Hour), 10)
			require.LessOrEqual(t, time.Unix(parsedTime, 0), time.Now().UTC().Add(6*time.Hour), 10)
			return &dynamodb.PutItemOutput{}
		},
	}

	ddb := newDynamodbClientMock(ddbClientMock, 5*time.Hour)
	err := ddb.Put(context.TODO(), dynamodbKey{primaryKey: "test", sortKey: "test1"}, []byte("TEST"))
	require.NoError(t, err)
}

type mockDynamodb struct {
	f func(input *dynamodb.PutItemInput) *dynamodb.PutItemOutput
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamodb) PutItemWithContext(_ context.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	return m.f(input), nil
}

func newDynamodbClientMock(mock *mockDynamodb, ttl time.Duration) *dynamodbKV {
	ddbKV := &dynamodbKV{
		ddbClient: mock,
		logger:    TestLogger{},
		tableName: aws.String("TEST"),
		ttlValue:  ttl,
	}

	return ddbKV
}
