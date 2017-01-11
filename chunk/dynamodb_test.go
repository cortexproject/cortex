package chunk

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type MockDynamoDB struct {
	mtx            sync.RWMutex
	unprocessed    int
	provisionedErr int
	tables         map[string]*mockDynamoDBTable
}

type mockDynamoDBTable struct {
	hashKey     string
	rangeKey    string
	items       map[string][]mockDynamoDBItem
	write, read int64
}

type mockDynamoDBItem map[string]*dynamodb.AttributeValue

func NewMockDynamoDB(unprocessed int, provisionedErr int) *MockDynamoDB {
	return &MockDynamoDB{
		tables:         map[string]*mockDynamoDBTable{},
		unprocessed:    unprocessed,
		provisionedErr: provisionedErr,
	}
}

func (m *MockDynamoDB) ListTablesPages(_ *dynamodb.ListTablesInput, fn func(p *dynamodb.ListTablesOutput, lastPage bool) (shouldContinue bool)) error {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var tableNames []*string
	for tableName := range m.tables {
		func(tableName string) {
			tableNames = append(tableNames, &tableName)
		}(tableName)
	}
	fn(&dynamodb.ListTablesOutput{
		TableNames: tableNames,
	}, true)
	return nil
}

func (m *MockDynamoDB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.tables[*input.TableName]; ok {
		return nil, fmt.Errorf("table already exists")
	}

	var hashKey, rangeKey string
	for _, schemaElement := range input.KeySchema {
		if *schemaElement.KeyType == "HASH" {
			hashKey = *schemaElement.AttributeName
		} else if *schemaElement.KeyType == "RANGE" {
			rangeKey = *schemaElement.AttributeName
		}
	}

	m.tables[*input.TableName] = &mockDynamoDBTable{
		hashKey:  hashKey,
		rangeKey: rangeKey,
		items:    map[string][]mockDynamoDBItem{},
		write:    *input.ProvisionedThroughput.WriteCapacityUnits,
		read:     *input.ProvisionedThroughput.ReadCapacityUnits,
	}

	return &dynamodb.CreateTableOutput{}, nil
}

func (m *MockDynamoDB) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	table, ok := m.tables[*input.TableName]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	return &dynamodb.DescribeTableOutput{
		Table: &dynamodb.TableDescription{
			ItemCount: aws.Int64(int64(len(table.items))),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
				ReadCapacityUnits:  aws.Int64(table.read),
				WriteCapacityUnits: aws.Int64(table.write),
			},
			TableStatus: aws.String(dynamodb.TableStatusActive),
		},
	}, nil
}

func (m *MockDynamoDB) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	table, ok := m.tables[*input.TableName]
	if !ok {
		return nil, fmt.Errorf("not found")
	}

	table.read = *input.ProvisionedThroughput.ReadCapacityUnits
	table.write = *input.ProvisionedThroughput.WriteCapacityUnits

	return &dynamodb.UpdateTableOutput{}, nil
}

func (m *MockDynamoDB) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	resp := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
	}

	if m.provisionedErr > 0 {
		m.provisionedErr--
		return resp, awserr.New(provisionedThroughputExceededException, "", nil)
	}

	for tableName, writeRequests := range input.RequestItems {
		table, ok := m.tables[tableName]
		if !ok {
			return &dynamodb.BatchWriteItemOutput{}, fmt.Errorf("table not found")
		}

		for _, writeRequest := range writeRequests {
			if m.unprocessed > 0 {
				m.unprocessed--
				resp.UnprocessedItems[tableName] = append(resp.UnprocessedItems[tableName], writeRequest)
				continue
			}

			hashValue := *writeRequest.PutRequest.Item[table.hashKey].S
			rangeValue := writeRequest.PutRequest.Item[table.rangeKey].B
			log.Printf("Write %s/%x", hashValue, rangeValue)

			items := table.items[hashValue]

			// insert in order
			i := sort.Search(len(items), func(i int) bool {
				return bytes.Compare(items[i][table.rangeKey].B, rangeValue) >= 0
			})
			if i >= len(items) || !bytes.Equal(items[i][table.rangeKey].B, rangeValue) {
				items = append(items, nil)
				copy(items[i+1:], items[i:])
			}
			items[i] = writeRequest.PutRequest.Item

			table.items[hashValue] = items
		}
	}
	return resp, nil
}

func (m *MockDynamoDB) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	table, ok := m.tables[*input.TableName]
	if !ok {
		return nil, fmt.Errorf("table not found")
	}

	hashValueCondition, ok := input.KeyConditions[table.hashKey]
	if !ok {
		return &dynamodb.QueryOutput{}, fmt.Errorf("must specify hash value condition")
	}

	hashValue := *hashValueCondition.AttributeValueList[0].S
	items, ok := table.items[hashValue]
	if !ok {
		return &dynamodb.QueryOutput{}, nil
	}

	var found []mockDynamoDBItem
	rangeKeyCondition, ok := input.KeyConditions[table.rangeKey]
	if !ok {
		log.Printf("Lookup %s/* -> *", hashValue)
		found = items
	} else if *rangeKeyCondition.ComparisonOperator == dynamodb.ComparisonOperatorBetween {
		rangeValueStart := rangeKeyCondition.AttributeValueList[0].B
		rangeValueEnd := rangeKeyCondition.AttributeValueList[1].B

		log.Printf("Lookup %s/%x -> %x (%d)", hashValue, rangeValueStart, rangeValueEnd, len(items))

		i := sort.Search(len(items), func(i int) bool {
			return bytes.Compare(items[i][table.rangeKey].B, rangeValueStart) >= 0
		})

		j := sort.Search(len(items), func(i int) bool {
			return bytes.Compare(items[i][table.rangeKey].B, rangeValueEnd) > 0
		})

		log.Printf("  found range [%d:%d]", i, j)
		if i > len(items) || i == j {
			return &dynamodb.QueryOutput{}, nil
		}
		found = items[i:j]
	} else if *rangeKeyCondition.ComparisonOperator == dynamodb.ComparisonOperatorBeginsWith {
		prefix := rangeKeyCondition.AttributeValueList[0].B

		log.Printf("Lookup prefix %s/%x (%d)", hashValue, prefix, len(items))

		// the smallest index i in [0, n) at which f(i) is true
		i := sort.Search(len(items), func(i int) bool {
			if bytes.Compare(items[i][table.rangeKey].B, prefix) > 0 {
				return true
			}
			return bytes.HasPrefix(items[i][table.rangeKey].B, prefix)
		})
		j := sort.Search(len(items)-i, func(j int) bool {
			if bytes.Compare(items[i+j][table.rangeKey].B, prefix) < 0 {
				return false
			}
			return !bytes.HasPrefix(items[i+j][table.rangeKey].B, prefix)
		})

		log.Printf("  found range [%d:%d)", i, i+j)
		if i > len(items) || j == 0 {
			return &dynamodb.QueryOutput{}, nil
		}
		found = items[i : i+j]
	} else {
		panic(fmt.Sprintf("%s not supported", *rangeKeyCondition.ComparisonOperator))
	}

	result := make([]map[string]*dynamodb.AttributeValue, 0, len(found))
	for _, item := range found {
		result = append(result, item)
	}

	return &dynamodb.QueryOutput{
		Items: result,
	}, nil
}

func (m *MockDynamoDB) QueryPages(input *dynamodb.QueryInput, f func(p *dynamodb.QueryOutput, lastPage bool) bool) error {
	output, err := m.Query(input)
	if err != nil {
		return err
	}

	f(output, true)
	return nil
}

func (m *MockDynamoDB) QueryRequest(in *dynamodb.QueryInput) (req dynamoRequest, output *dynamodb.QueryOutput) {
	output, err := m.Query(in)
	return &mockDynamoRequest{output, err}, nil
}

type mockDynamoRequest struct {
	data *dynamodb.QueryOutput
	err  error
}

func (m *mockDynamoRequest) NextPage() dynamoRequest { return nil }
func (m *mockDynamoRequest) HasNextPage() bool       { return false }
func (m *mockDynamoRequest) Data() interface{}       { return m.data }
func (m *mockDynamoRequest) OperationName() string   { return "Query" }
func (m *mockDynamoRequest) Send() error             { return m.err }
func (m *mockDynamoRequest) Error() error            { return nil }
