package chunk

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/prometheus/common/log"
	"golang.org/x/net/context"
)

type MockStorage struct {
	mtx    sync.RWMutex
	tables map[string]*mockTable
}

type mockTable struct {
	items       map[string][]mockItem
	write, read int64
}

type mockItem struct {
	rangeValue []byte
	value      []byte
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		tables: map[string]*mockTable{},
	}
}

func (m *MockStorage) ListTables() ([]string, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var tableNames []string
	for tableName := range m.tables {
		func(tableName string) {
			tableNames = append(tableNames, tableName)
		}(tableName)
	}
	return tableNames, nil
}

func (m *MockStorage) CreateTable(name string, read, write int64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.tables[name]; ok {
		return fmt.Errorf("table already exists")
	}

	m.tables[name] = &mockTable{
		items: map[string][]mockItem{},
		write: write,
		read:  read,
	}

	return nil
}

func (m *MockStorage) DescribeTable(name string) (readCapacity, writeCapacity int64, status string, err error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	table, ok := m.tables[name]
	if !ok {
		return 0, 0, "", fmt.Errorf("not found")
	}

	return table.read, table.write, dynamodb.TableStatusActive, nil
}

func (m *MockStorage) UpdateTable(name string, readCapacity, writeCapacity int64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	table, ok := m.tables[name]
	if !ok {
		return fmt.Errorf("not found")
	}

	table.read = readCapacity
	table.write = writeCapacity

	return nil
}

func (m *MockStorage) NewWriteBatch() WriteBatch {
	return &mockWriteBatch{}
}

func (m *MockStorage) BatchWrite(ctx context.Context, batch WriteBatch) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, req := range *batch.(*mockWriteBatch) {
		table, ok := m.tables[req.tableName]
		if !ok {
			return fmt.Errorf("table not found")
		}

		log.Debugf("Write %s/%x", req.hashValue, req.rangeValue)

		items := table.items[req.hashValue]

		// insert in order
		i := sort.Search(len(items), func(i int) bool {
			return bytes.Compare(items[i].rangeValue, req.rangeValue) >= 0
		})
		if i >= len(items) || !bytes.Equal(items[i].rangeValue, req.rangeValue) {
			items = append(items, mockItem{})
			copy(items[i+1:], items[i:])
		} else {
			return fmt.Errorf("Dupe write")
		}
		items[i] = mockItem{
			rangeValue: req.rangeValue,
			value:      req.value,
		}

		table.items[req.hashValue] = items
	}
	return nil
}

func (m *MockStorage) QueryPages(ctx context.Context, entry IndexEntry, callback func(result ReadBatch, lastPage bool) (shouldContinue bool)) error {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	table, ok := m.tables[entry.TableName]
	if !ok {
		return fmt.Errorf("table not found")
	}

	items, ok := table.items[entry.HashValue]
	if !ok {
		return nil
	}

	if entry.RangeValuePrefix != nil {
		log.Debugf("Lookup prefix %s/%x (%d)", entry.HashValue, entry.RangeValuePrefix, len(items))

		// the smallest index i in [0, n) at which f(i) is true
		i := sort.Search(len(items), func(i int) bool {
			if bytes.Compare(items[i].rangeValue, entry.RangeValuePrefix) > 0 {
				return true
			}
			return bytes.HasPrefix(items[i].rangeValue, entry.RangeValuePrefix)
		})
		j := sort.Search(len(items)-i, func(j int) bool {
			if bytes.Compare(items[i+j].rangeValue, entry.RangeValuePrefix) < 0 {
				return false
			}
			return !bytes.HasPrefix(items[i+j].rangeValue, entry.RangeValuePrefix)
		})

		log.Debugf("  found range [%d:%d)", i, i+j)
		if i > len(items) || j == 0 {
			return nil
		}
		items = items[i : i+j]

	} else if entry.RangeValueStart != nil {
		log.Debugf("Lookup range %s/%x -> ... (%d)", entry.HashValue, entry.RangeValueStart, len(items))

		// the smallest index i in [0, n) at which f(i) is true
		i := sort.Search(len(items), func(i int) bool {
			return bytes.Compare(items[i].rangeValue, entry.RangeValueStart) >= 0
		})

		log.Debugf("  found range [%d)", i)
		if i > len(items) {
			return nil
		}
		items = items[i:]

	} else {
		log.Debugf("Lookup %s/* (%d)", entry.HashValue, len(items))
	}

	result := mockReadBatch{}
	for _, item := range items {
		result = append(result, item)
	}

	callback(result, true)
	return nil
}

type mockWriteBatch []struct {
	tableName, hashValue string
	rangeValue           []byte
	value                []byte
}

func (b *mockWriteBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	*b = append(*b, struct {
		tableName, hashValue string
		rangeValue           []byte
		value                []byte
	}{tableName, hashValue, rangeValue, value})
}

type mockReadBatch []mockItem

func (b mockReadBatch) Len() int {
	return len(b)
}

func (b mockReadBatch) RangeValue(i int) []byte {
	return b[i].rangeValue
}

func (b mockReadBatch) Value(i int) []byte {
	return b[i].value
}
