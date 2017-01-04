package chunk

import (
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type dynamodbClient interface {
	ListTablesPages(*dynamodb.ListTablesInput, func(p *dynamodb.ListTablesOutput, lastPage bool) (shouldContinue bool)) error
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	UpdateTable(*dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error)

	BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error)
	QueryRequest(*dynamodb.QueryInput) (req dynamoRequest, output *dynamodb.QueryOutput)
}

type dynamoRequest interface {
	NextPage() dynamoRequest
	HasNextPage() bool
	Data() interface{}
	OperationName() string
	Send() error
	Error() error
}

type dynamoClientAdapter struct {
	*dynamodb.DynamoDB
}

func (d dynamoClientAdapter) QueryRequest(in *dynamodb.QueryInput) (dynamoRequest, *dynamodb.QueryOutput) {
	req, out := d.DynamoDB.QueryRequest(in)
	return dynamoRequestAdapter{req}, out
}

type dynamoRequestAdapter struct {
	*request.Request
}

func (d dynamoRequestAdapter) Data() interface{} {
	return d.Request.Data
}

func (d dynamoRequestAdapter) OperationName() string {
	return d.Operation.Name
}

func (d dynamoRequestAdapter) NextPage() dynamoRequest {
	return dynamoRequestAdapter{d.Request.NextPage()}
}

func (d dynamoRequestAdapter) Error() error {
	return d.Request.Error
}
