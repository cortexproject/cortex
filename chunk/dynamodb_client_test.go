package chunk

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"golang.org/x/net/context"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI

	mtx            sync.RWMutex
	unprocessed    int
	provisionedErr int
	tables         map[string]*mockDynamoDBTable
}

type mockDynamoDBTable struct {
	items map[string][]mockDynamoDBItem
}

type mockDynamoDBItem map[string]*dynamodb.AttributeValue

func newMockDynamoDB(unprocessed int, provisionedErr int) *mockDynamoDBClient {
	return &mockDynamoDBClient{
		tables:         map[string]*mockDynamoDBTable{},
		unprocessed:    unprocessed,
		provisionedErr: provisionedErr,
	}
}

func (m *mockDynamoDBClient) createTable(name string) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.tables[name] = &mockDynamoDBTable{
		items: map[string][]mockDynamoDBItem{},
	}
}

func (m *mockDynamoDBClient) BatchWriteItem(input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
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

			hashValue := *writeRequest.PutRequest.Item[hashKey].S
			rangeValue := writeRequest.PutRequest.Item[rangeKey].B
			log.Printf("Write %s/%x", hashValue, rangeValue)

			items := table.items[hashValue]

			// insert in order
			i := sort.Search(len(items), func(i int) bool {
				return bytes.Compare(items[i][rangeKey].B, rangeValue) >= 0
			})
			if i >= len(items) || !bytes.Equal(items[i][rangeKey].B, rangeValue) {
				items = append(items, nil)
				copy(items[i+1:], items[i:])
			} else {
				return &dynamodb.BatchWriteItemOutput{}, fmt.Errorf("Duplicate entry")
			}
			items[i] = writeRequest.PutRequest.Item

			table.items[hashValue] = items
		}
	}
	return resp, nil
}

func TestDynamoDBClient(t *testing.T) {
	dynamoDB := newMockDynamoDB(0, 0)
	client := dynamoClientAdapter{
		DynamoDB: dynamoDB,
	}
	batch := client.NewWriteBatch()
	for i := 0; i < 30; i++ {
		batch.Add("table", fmt.Sprintf("hash%d", i), []byte(fmt.Sprintf("range%d", i)))
	}
	dynamoDB.createTable("table")

	if err := client.BatchWrite(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
}
